(ns ^:no-doc onyx.peer.task-lifecycle
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [onyx.schema :as os]
            [schema.core :as s]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :as entry]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.static.planning :refer [find-task]]
            [onyx.static.uuid :as uuid]
            [onyx.peer.coordinator :as coordinator]
            [onyx.peer.task-compile :as c]
            [onyx.windowing.window-compile :as wc]
            [onyx.lifecycles.lifecycle-invoke :as lc]
            [onyx.peer.function :as function]
            [onyx.peer.operation :as operation]
            [onyx.compression.nippy :refer [messaging-decompress]]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.messenger-state :as ms]
            [onyx.log.replica]
            [onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count! map->EventState ->EventState]]
            ;[onyx.peer.event-state :as event-state]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :refer [apply-fn]]
            [onyx.plugin.onyx-input :as oi]
            [onyx.plugin.onyx-output :as oo]
            [onyx.plugin.onyx-plugin :as op]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.static.logging :as logger]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.messaging.aeron :as messaging]
            [onyx.messaging.common :as mc]))

; (defn resolve-log [{:keys [peer-opts] :as pipeline}]
;   (let [log-impl (arg-or-default :onyx.peer/state-log-impl peer-opts)] 
;     (assoc pipeline :state-log (if (:windowed-task? pipeline) 
;                                  (state-extensions/initialize-log log-impl pipeline)))))

; (defn resolve-filter-state [{:keys [peer-opts] :as pipeline}]
;   (let [filter-impl (arg-or-default :onyx.peer/state-filter-impl peer-opts)] 
;     (assoc pipeline 
;            :filter-state 
;            (if (and (:windowed-task? pipeline)
;                     (:uniquesness-task? pipeline))
;              (atom (state-extensions/initialize-filter filter-impl pipeline))))))

(s/defn start-lifecycle? [event]
  (let [rets (lc/invoke-start-task event)]
    (when-not (:start-lifecycle? rets)
      (info (:log-prefix event) "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defrecord SegmentRetries [segments retries])

(defn add-from-leaf 
  [event result root leaves accum {:keys [message] :as leaf}]
  (let [routes (r/route-data event result message)
        message* (r/flow-conditions-transform message routes event)
        leaf* (if (= message message*)
                leaf
                (assoc leaf :message message*))]
    (if (= :retry (:action routes))
      (assoc accum :retries (conj! (:retries accum) root))
      (update accum :segments (fn [s] (conj! s (assoc leaf* :flow (:flow routes))))))))

(s/defn add-from-leaves
  "Flattens root/leaves into an xor'd ack-val, and accumulates new segments and retries"
  [segments retries event :- os/Event result]
  (let [root (:root result)
        leaves (:leaves result)]
    (reduce (fn [accum leaf]
              (lc/invoke-flow-conditions add-from-leaf event result root leaves accum leaf))
            (->SegmentRetries segments retries)
            leaves)))

(defn persistent-results! [results]
  (->Results (:tree results)
             (persistent! (:segments results))
             (persistent! (:retries results))))

(defn build-new-segments
  [state]
  (update state 
          :event
          (fn [{:keys [results monitoring] :as event}]
            (emit-latency 
              :peer-batch-latency 
              monitoring
              #(let [results (reduce (fn [accumulated result]
                                       (let [root (:root result)
                                             segments (:segments accumulated)
                                             retries (:retries accumulated)
                                             ret (add-from-leaves segments retries event result)]
                                         (->Results (:tree results) (:segments ret) (:retries ret))))
                                     results
                                     (:tree results))]
                 (assoc event :results (persistent-results! results)))))))

; (s/defn flow-retry-segments :- Event
;   [{:keys [task-state state messenger monitoring results] :as event} 
;   (doseq [root (:retries results)]
;     (when-let [site (peer-site task-state (:completion-id root))]
;       (emit-latency :peer-retry-segment
;                     monitoring
;                     #(extensions/internal-retry-segment messenger (:id root) site))))
;   event)

(s/defn start-processing
  [state]
  {:post [(empty? (:batch (:event %)))
          (empty? (:segments (:results (:event %))))]}
  (assert (:init-event state))
  (assoc state :event (assoc (:init-event state) :lifecycle-id (uuid/random-uuid))))

;; TODO, good place to implement another protocol and use type dispatch
(def input-readers
  {:input #'function/read-input-batch
   :function #'function/read-function-batch
   :output #'function/read-function-batch})

(defn read-batch [state]
  (assert (:event state))
  (let [task-type (:task-type (:event state))
        _ (assert task-type)
        _ (assert (:apply-fn (:event state)))
        f (get input-readers task-type)]
    (f state))

  ; (lc/invoke-read-batch 
  ;   (fn read-batch-invoke
  ;     [{:keys [task-type] :as event}]
  ;     (let [f (get input-readers task-type)
  ;           rets (f state)]
  ;       ;; FIXME INVOKE AFTER READ BATCH
  ;       #_(lc/invoke-after-read-batch rets)))
  ;   (:event state))
  )

(defn prepare-batch [{:keys [pipeline] :as state}] 
  ;; FIXME invoke catch
  (oo/prepare-batch pipeline state))

(defn write-batch [{:keys [pipeline] :as state}] 
  ;; FIXME invoke catch
  (oo/write-batch pipeline state)

  ; (update state 
  ;         :event
  ;         (fn [event] 
  ;           (lc/invoke-write-batch 
  ;             (s/fn :- os/Event 
  ;               [event :- os/Event]
  ;               (let [rets (merge event )]
  ;                 (trace (:log-prefix event) (format "Wrote %s segments" (count (:segments (:results rets)))))
  ;                 rets))
  ;             event)))
  
  )

(defn handle-exception [task-info log e group-ch outbox-ch id job-id]
  (let [data (ex-data e)
        ;; Default to original exception if Onyx didn't wrap the original exception
        inner (or (.getCause ^Throwable e) e)]
    (if (:onyx.core/lifecycle-restart? data)
      (do (warn (logger/merge-error-keys inner task-info "Caught exception inside task lifecycle. Rebooting the task."))
          (>!! group-ch [:restart-vpeer id]))
      (do (warn (logger/merge-error-keys e task-info "Handling uncaught exception thrown inside task lifecycle - killing this job."))
          (let [entry (entry/create-log-entry :kill-job {:job job-id})]
            ;(println "writing chunk " e inner entry)
            (extensions/write-chunk log :exception inner job-id)
            (>!! outbox-ch entry))))))

(defn input-poll-barriers [{:keys [messenger] :as state}]
  (m/poll messenger)
  state)

(defn emit-barriers [{:keys [event messenger] :as state}]
  ;; TODO, checkpoint state here - do it for all types
  ;; But only if all barriers are seen, or if we're an input and we're going to emit
  
  ;; For input tasks we may need to do it for both input checkpoint as well as state
  ;; Though for input I think we just need to take a copy which we will put in the barrier 
  ;; state ready to be acked? That does seem quite ugly. Maybe we write the state and the 
  ;; checkpoint value here, but then modify the ack state when it actually gets acked.
  (let [{:keys [task-type id]} event] 
    (if (and (#{:function :input} task-type) (m/all-barriers-seen? messenger))
      (let [new-messenger (-> messenger 
                              (m/emit-barrier)
                              (m/unblock-subscriptions!))
            replica-version (m/replica-version new-messenger)
            epoch (m/epoch new-messenger)
            {:keys [job-id task-id slot-id log]} event]
        ;(println "Writing state checkpoint " replica-version epoch (mapv ws/export-state windows-state))
        (when-not (empty? (:windows event)) 
          ;; Make write-checkpoint just take an event
          (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id 
                                       :state 
                                       (mapv ws/export-state (:windows-state state))))
        (cond-> (assoc state :messenger new-messenger)
          (= :input task-type)
          (assoc-in [:barriers replica-version epoch] 
                    {:checkpoint (oi/checkpoint (:pipeline state))
                     :completed? (oi/completed? (:pipeline state))})))
      state)))

(defn ack-barriers [{:keys [messenger] :as state}]
  (if (m/all-barriers-seen? messenger)
    ;; Add me later
    ;;(extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :state {:3 4})
    (assoc state :messenger (m/emit-barrier-ack messenger))
    state))

;; FIXME: create an issue to reduce number of times exhaust input is written
;; Should check whether it's already exhausted for this allocation version
;; Maybe can use an atom though
(defn complete-job! [{:keys [event messenger] :as state}] ;; Fixme StateMonad
  (let [{:keys [job-id task-id]} event
        entry (entry/create-log-entry :exhaust-input {:replica-version (m/replica-version messenger)
                                                      ;:epoch (m/epoch (:messenger state))
                                                      :job job-id 
                                                      :task task-id})]
    (println "Complete job" task-id (:replica-version (:args entry)))
    (>!! (:outbox-ch event) entry)))

(defn backoff-when-drained! [event]
  (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(s/defn assign-windows :- os/Event
  [state]
  (ws/assign-windows state :new-segment))

;; Taken from clojure core incubator
(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn poll-acks [{:keys [event messenger barriers] :as state}]
  (let [new-messenger (m/poll-acks messenger)
        ack-result (m/all-acks-seen? new-messenger)]
    (if ack-result
      (let [{:keys [replica-version epoch]} ack-result]
        (if-let [barrier (get-in barriers [replica-version epoch])] 
          (do
           ;(println "Acking result, barrier:" (into {} barrier) replica-version epoch)
           ;(println barriers)
           (let [{:keys [job-id task-id slot-id log]} event
                 completed? (:completed? barrier)] 
             (when completed?
               (if (not (:exhausted? state))
                 (complete-job! state)
                 (backoff-when-drained! state)))
             (-> state
                 (assoc :exhausted? completed?)
                 (update :barriers dissoc-in [replica-version epoch])
                 (assoc :messenger (m/flush-acks new-messenger)))))
          ;; Maybe shouldn't have flush-acks here
          (assoc state :messenger (m/flush-acks new-messenger))))
      (assoc state :messenger new-messenger))))

(defn before-batch [state]
  (update state :event lc/invoke-before-batch))

(defn after-batch [state]
  (update state :event lc/invoke-after-batch))

(defn recover-stored-checkpoint
  [{:keys [log job-id task-id slot-id] :as event} checkpoint-type recover]
  (let [checkpointed (-> (extensions/read-checkpoints log job-id)
                         (get recover)
                         (get [task-id slot-id checkpoint-type]))]
    (if-not (= :beginning checkpointed)
      checkpointed))
  ; (if (some #{job-id} (:jobs prev-replica))
  ;   (do 
  ;    (when (not= (required-checkpoints prev-replica job-id)
  ;                (required-checkpoints next-replica job-id))
  ;      (throw (ex-info "Slots for input tasks must currently be stable to allow checkpoint resume" {})))
  ;    (let [[[rv e] checkpoints] (max-completed-checkpoints event next-replica)]
  ;      (get checkpoints [task-id slot-id]))))
  )

(defn next-windows-state
  [{:keys [log-prefix task-map windows triggers] :as event} recover]
  (if (:windowed-task? event)
    (let [stored (recover-stored-checkpoint event :state recover)]
      (->> windows
           (mapv (fn [window] (wc/resolve-window-state window triggers task-map)))
           (mapv (fn [stored ws]
                   ;(println "STORED" stored)
                   (if stored
                     (let [recovered (ws/recover-state ws stored)] 
                       (info "Recovered state" stored (:id event))
                       recovered) 
                     ws))
                 (or stored (repeat nil))))
      ;(update :windows-state
      ;         (fn [windows-state] 
      ;           (mapv (fn [ws entries]
      ;                   (-> ws 
      ;                       ;; Restore the accumulated log as a hack
      ;                       ;; To allow us to dump the full state each time
      ;                       (assoc :event-results (mapv ws/log-entry->state-event entries))
      ;                       (ws/play-entry entries)))
      ;                 windows-state
      ;                 (or stored (repeat [])))))
      )))

(defn next-pipeline-state [pipeline event recover]
  (if (= :input (:task-type event)) 
    ;; Do this above, and only once
    (let [stored (recover-stored-checkpoint event :input recover)]
      (info "Recovering checkpoint " stored)
      (println "Recovering checkpoint " (:task-id event) stored)
      (oi/recover pipeline stored))
    pipeline))

(defn fetch-recover [{:keys [event messenger] :as state}]
  (m/poll-recover messenger))

(defn recover-state [{:keys [messenger coordinator replica] :as state} recover]
  (let [old-replica (:replica state)
        {:keys [job-id task-type windows task-id slot-id] :as event} (:event state)
        messenger (if (= task-type :output)
                    (m/emit-barrier-ack messenger)
                    (-> messenger 
                        (m/emit-barrier {:recover recover})
                        (m/unblock-subscriptions!)))
        _ (info "RECOVER " recover task-type task-id slot-id)
        windows-state (next-windows-state event recover)
        next-pipeline (next-pipeline-state (:pipeline state) event recover)
        next-state (assoc (->EventState :start-processing
                                        replica
                                        messenger
                                        coordinator
                                        next-pipeline
                                        {}
                                        windows-state
                                        (:exhausted? state)
                                        (:init-event state)
                                        (:event state))
                          ;; Fix this
                          :state :runnable
                          
                          )]
    (if-not (empty? windows) 
      (ws/assign-windows next-state :recovered)
      next-state)))

(defn try-recover [state]
  (if-let [recover (fetch-recover state)]
    (do
     (info "RECOVERED" recover (:onyx/name (:task-map (:event state))))
     (assoc (recover-state state recover) :state :runnable))
    (assoc state :state :blocked)))

(defn next-state-from-replica [{:keys [messenger coordinator] :as prev-state} replica]
  (let [{:keys [job-id task-type] :as event} (:event prev-state)
        old-replica (:replica prev-state)
        state (assoc prev-state :event event)
        ;; why are we passing in event here
        next-messenger (ms/next-messenger-state! messenger event old-replica replica)
        ;; Coordinator must be transitioned before recovery, as the coordinator
        ;; emits the barrier with the recovery information in 
        next-coordinator (coordinator/next-state coordinator old-replica replica)]
    (assoc prev-state 
           :lifecycle :recover
           :state :runnable
           :replica replica
           :event (:init-event prev-state) 
           :messenger next-messenger 
           :coordinator next-coordinator)))

;; keys, 
;; :lifecycle
;; :state :blocked
;; :state :exhausted
;; :state :running

(defn task-alive? [event]
  (first (alts!! [(:task-kill-ch event) (:kill-ch event)] :default true)))

(defn print-state [{:keys [event] :as state}]
  (let [task-map (:task-map event)] 
    (info "Task state" 
          (:onyx/type task-map) 
          (:onyx/name task-map)
          (:lifecycle state)
          (:state state)
          (cond true ;(#{:read-batch} (:lifecycle state))
                [(:batch event)
                 (:segments (:results event))])))
  state)

(defn next-lifecycle [state]
  (if (= :blocked (:state state))
    state
    (assoc state 
           :lifecycle
           ;; TODO: precompiled state machine for different task types.
           ;; e.g. function tasks skip polling for acks
           (let [task-type (:task-type (:event state))
                 windowed-task? (:windowed-task? (:event state))] 
             (case (:lifecycle state)
               :recover :start-processing
               :start-processing (if (= task-type :input) 
                                   :input-poll-barriers
                                   ;; output doesn't need emit barriers
                                   :emit-barriers)
               :input-poll-barriers :emit-barriers
               :emit-barriers (if (= task-type :input) 
                                :poll-acks
                                :before-batch)
               :poll-acks :before-batch
               :before-batch :read-batch
               :read-batch :apply-fn
               :apply-fn :build-new-segments
               :build-new-segments (if windowed-task? 
                                     :assign-windows
                                     :prepare-batch)
               :assign-windows :prepare-batch
               :prepare-batch :write-batch
               :write-batch :after-batch
               :after-batch (if (= task-type :output) 
                              :ack-barriers
                              :start-processing)
               :ack-barriers :start-processing)))))

(defn transition [state]
    (case (:lifecycle state)
      :recover (try-recover state)
      :start-processing (start-processing state)
      :input-poll-barriers (input-poll-barriers state)
      :emit-barriers (emit-barriers state)
      ;; Set some emitted. Return unfinished from emit-barrier, then turn it into being blocked
      ;; When finally unblocked, then can switch to next-epoch?
      ;; Possibly need to do a prepare step too
      :poll-acks (poll-acks state)
      :before-batch (before-batch state)
      :read-batch (read-batch state)
      :apply-fn (apply-fn state)
      :build-new-segments (build-new-segments state)
      :assign-windows (assign-windows state)
      :prepare-batch (prepare-batch state)
      :write-batch (write-batch state)
      :after-batch (after-batch state)
      :ack-barriers (ack-barriers state)))

(defn next-state [prev-state replica]
  (let [job-id (get-in prev-state [:event :job-id])
        _ (assert job-id)
        old-replica (:replica prev-state)
        old-version (get-in old-replica [:allocation-version job-id])
        new-version (get-in replica [:allocation-version job-id])]
    (if (task-alive? (:init-event prev-state))
      (if-not (= old-version new-version)
        (next-state-from-replica prev-state replica)
        (loop [state prev-state]
          ;(println "Trying " (:lifecycle state))
          (info "Trying " (:lifecycle state)  (:state state))
          (let [new-state (-> state
                              transition
                              next-lifecycle)]
            (print-state new-state)
            (if (or (= :blocked (:state new-state))
                    (= :start-processing (:lifecycle new-state)))
              new-state
              (recur new-state)))))
      (assoc prev-state :lifecycle :killed))))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [init-state ex-f]
  (try
    (assert (:event init-state))
    (let [{:keys [task-kill-ch kill-ch task-information replica-atom opts state]} (:event init-state)] 
      (loop [prev-state init-state 
             replica-val @replica-atom]
        ;(println "Iteration " (:state prev-state))
        (let [state (next-state prev-state replica-val)]
          ; (assert (empty? (.__extmap event)) 
          ;         (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap event))))
          (if-not (= :killed (:lifecycle state)) 
            (do
             (assert (#{:start-processing :recover :write-batch} (:lifecycle state)))
             (recur state @replica-atom))
            prev-state))))
   (catch Throwable e
     (ex-f e)
     init-state)))

(defn build-pipeline [task-map pipeline-data]
  (let [kw (:onyx/plugin task-map)]
    (try
     (if (#{:input :output} (:onyx/type task-map))
       (case (:onyx/language task-map)
         :java (operation/instantiate-plugin-instance (name kw) pipeline-data)
         (let [user-ns (namespace kw)
               user-fn (name kw)
               pipeline (if (and user-ns user-fn)
                          (if-let [f (ns-resolve (symbol user-ns) (symbol user-fn))]
                            (f pipeline-data)))]
           (if pipeline
             (op/start pipeline)
             (throw (ex-info "Failure to resolve plugin builder fn. Did you require the file that contains this symbol?" {:kw kw})))))
       ;; TODO, make this a unique type - extend-type is ugly
       (Object.))
      (catch Throwable e
        (throw e)))))

(defrecord TaskInformation 
  [log job-id task-id workflow catalog task flow-conditions windows triggers lifecycles metadata]
  component/Lifecycle
  (start [component]
    (let [catalog (extensions/read-chunk log :catalog job-id)
          task (extensions/read-chunk log :task job-id task-id)
          flow-conditions (extensions/read-chunk log :flow-conditions job-id)
          windows (extensions/read-chunk log :windows job-id)
          triggers (extensions/read-chunk log :triggers job-id)
          workflow (extensions/read-chunk log :workflow job-id)
          lifecycles (extensions/read-chunk log :lifecycles job-id)
          metadata (extensions/read-chunk log :job-metadata job-id)]
      (assoc component 
             :workflow workflow :catalog catalog :task task :flow-conditions flow-conditions
             :windows windows :triggers triggers :lifecycles lifecycles :metadata metadata)))
  (stop [component]
    (assoc component 
           :catalog nil :task nil :flow-conditions nil :windows nil 
           :triggers nil :lifecycles nil :metadata nil)))

(defn new-task-information [peer task]
  (map->TaskInformation (select-keys (merge peer task) [:log :job-id :task-id :id])))

(defn backoff-until-task-start! [{:keys [kill-ch task-kill-ch opts] :as event}]
  (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
              (not (start-lifecycle? event)))
    (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts))))

(defn backoff-until-covered! [{:keys [id replica job-id kill-ch task-kill-ch opts outbox-ch log-prefix] :as event}]
  (loop [replica-state @replica]
    (when (and (first (alts!! [kill-ch task-kill-ch] :default true))
               (not (common/job-covered? replica-state job-id)))
      (info log-prefix "Not enough virtual peers have warmed up to start the task yet, backing off and trying again...")
      (Thread/sleep (arg-or-default :onyx.peer/job-not-ready-back-off opts))
      (recur @replica))))

(defn start-task-lifecycle! [state ex-f]
  (thread (run-task-lifecycle state ex-f)))

(defn final-state [component]
  (<!! (:task-lifecycle-ch component)))

(defrecord TaskLifeCycle
  [id log messenger job-id task-id replica group-ch log-prefix
   kill-ch outbox-ch seal-ch completion-ch peer-group opts task-kill-ch scheduler-event task-monitoring task-information]
  component/Lifecycle

  (start [component]
    (assert (zero? (count (m/publications messenger))))
    (assert (zero? (count (m/subscriptions messenger))))
    (assert (zero? (count (m/ack-subscriptions messenger))))
    (try
      (let [{:keys [workflow catalog task flow-conditions windows triggers lifecycles metadata]} task-information
            log-prefix (logger/log-prefix task-information)
            task-map (find-task catalog (:name task))
            filtered-windows (vec (wc/filter-windows windows (:name task)))
            window-ids (set (map :window/id filtered-windows))
            filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
            coordinator (coordinator/new-peer-coordinator log (:messenger-group component) opts id job-id group-ch)
            pipeline-data (map->Event 
                            {:id id
                             :job-id job-id
                             :task-id task-id
                             :slot-id (get-in @replica [:task-slot-ids job-id task-id id])
                             :task (:name task)
                             :catalog catalog
                             :workflow workflow
                             :windows filtered-windows
                             :triggers filtered-triggers
                             :flow-conditions flow-conditions
                             :lifecycles lifecycles
                             :metadata metadata
                             :task-map task-map
                             :serialized-task task
                             :log log
                             :monitoring task-monitoring
                             :task-information task-information
                             :outbox-ch outbox-ch
                             :group-ch group-ch
                             :task-kill-ch task-kill-ch
                             :kill-ch kill-ch
                             :peer-opts opts ;; rename to peer-config eventually
                             :fn (operation/resolve-task-fn task-map)
                             :replica-atom replica
                             :log-prefix log-prefix})

            _ (info log-prefix "Warming up task lifecycle" task)

            pipeline-data (->> pipeline-data
                               c/task-params->event-map
                               c/flow-conditions->event-map
                               c/lifecycles->event-map
                               c/task->event-map)

            _ (assert (empty? (.__extmap pipeline-data)) (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap pipeline-data))))

            _ (backoff-until-task-start! pipeline-data)

            ex-f (fn [e] (handle-exception task-information log e group-ch outbox-ch id job-id))
            event (->> pipeline-data
                               lc/invoke-before-task-start
                               ;add-pipeline
                               #_resolve-filter-state
                               #_resolve-log)
            initial-state (map->EventState 
                            {:lifecycle :recover
                             :replica (onyx.log.replica/starting-replica opts)
                             :messenger messenger
                             :coordinator coordinator
                             :pipeline (build-pipeline task-map event)
                             :barriers {}
                             :exhausted? false ;; convert to a state
                             :windows-states (c/event->windows-states event)
                             :init-event event
                             :event event})]
       ;; TODO: we may need some kind of a signal ready to assure that 
       ;; subscribers do not blow past messages in aeron
       ;(>!! outbox-ch (entry/create-log-entry :signal-ready {:id id}))
       (info log-prefix "Enough peers are active, starting the task")
       (let [task-lifecycle-ch (start-task-lifecycle! initial-state ex-f)]
         (s/validate os/Event event)
         (assoc component
                :event event
                :state initial-state
                :log-prefix log-prefix
                :task-information task-information
                :task-kill-ch task-kill-ch
                :kill-ch kill-ch
                :task-lifecycle-ch task-lifecycle-ch)))
     (catch Throwable e
       ;; FIXME in main branch, we weren't wrapping up exception in here
       (handle-exception task-information log e group-ch outbox-ch id job-id)
       component)))

  (stop [component]
    (if-let [task-name (:name (:task (:task-information component)))]
      (info (:log-prefix component) "Stopping task lifecycle")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up"))

    (when-let [event (:event component)]
      ;; Ensure task operations are finished before closing peer connections
      (close! (:kill-ch component))

      ;; FIXME: should try to get last STATE here
      (let [last-state (final-state component)
            last-event (:event last-state)]
        (when-not (empty? (:triggers last-event))
          (ws/assign-windows last-state (:scheduler-event component)))

        (some-> last-state :coordinator coordinator/stop)
        (some-> last-state :messenger component/stop)
        (some-> last-state :pipeline (op/stop event))

        (close! (:task-kill-ch component))

        ; (when-let [state-log (:state-log event)] 
        ;   (state-extensions/close-log state-log event))

        ; (when-let [filter-state (:filter-state event)] 
        ;   (when (exactly-once-task? event)
        ;     (state-extensions/close-filter @filter-state event)))

        ((:compiled-after-task-fn event) event)))

    (assoc component
           :event nil
           :state nil
           :log-prefix nil
           :task-information nil
           :task-kill-ch nil
           :kill-ch nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer task]
  (map->TaskLifeCycle (merge peer task)))
