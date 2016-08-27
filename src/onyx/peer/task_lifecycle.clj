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
            [onyx.peer.event-state :as event-state]
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

(s/defn gen-lifecycle-id
  [state]
  (update state :event assoc :lifecycle-id (uuid/random-uuid)))

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
    (assert f)
    (println "applying to " state)
    (assert (:event 
     (f state)         
              ))
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

(defn write-batch [{:keys [pipeline] :as state}] 
  (update state 
          :event
          (fn [event] 
            (lc/invoke-write-batch 
              (s/fn :- os/Event 
                [event :- os/Event]
                (let [rets (merge event (oo/write-batch pipeline event))]
                  (trace (:log-prefix event) (format "Wrote %s segments" (count (:segments (:results rets)))))
                  rets))
              event))))

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

;; Only poll here for barriers if it's an input task and we won't read the barrier otherwise
(defn poll-input-barriers [{:keys [event messenger] :as state}]
  (when (= (:task-type event) :input) 
    (m/poll messenger))
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
      (let [new-messenger (m/emit-barrier messenger)
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

(defn ack-barriers [{:keys [task-type state] :as event}]
  (if (and (= :output task-type) 
           (m/all-barriers-seen? (:messenger state)))
    ;; Add me later
    ;;(extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :state {:3 4})
    (assoc-in event [:state :messenger] (m/emit-barrier-ack (:messenger state)))
    event))

;; FIXME: create an issue to reduce number of times exhaust input is written
;; Should check whether it's already exhausted for this allocation version
;; Maybe can use an atom though
(s/defn complete-job [{:keys [job-id task-id state] :as event} :- os/Event]
  (throw (Exception.))
  (let [entry (entry/create-log-entry :exhaust-input {:replica-version (m/replica-version (:messenger state))
                                                      ;:epoch (m/epoch (:messenger state))
                                                      :job job-id 
                                                      :task task-id})]
    (println "Complete job" task-id (:replica-version (:args entry)))
    (>!! (:outbox-ch event) entry)))

(defn backoff-when-drained! [event]
  (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(s/defn assign-windows :- os/Event
  [state]
  (if-not (empty?  (:windows (:event state)))
    (ws/assign-windows state :new-segment)
    state))

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
  (let [{:keys [task-type]} event] 
    (if (= :input task-type) 
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
               (when (and completed? (not (:exhausted? state)))
                 (complete-job state)
                 (backoff-when-drained! state))
               (-> state
                   (assoc :exhausted? completed?)
                   (update :barriers dissoc-in [replica-version epoch])
                   (assoc :messenger (m/flush-acks new-messenger)))))
            ;; Maybe shouldn't have flush-acks here
            (assoc state :messenger (m/flush-acks new-messenger))))
        (assoc state :messenger new-messenger)))
    state)))

(defn print-stage [stage state]
  ;; When replica-version is set, set :barriers-seen? on messenger.
  ;; Until barriers-seen? is set, do not allow task-lifecycle loops

  (assert (:event state) state)
  (assert (:apply-fn (:event state)) (pr-str "hmm " stage  (:event state) (:onyx/name (:task-map (:prev-event state)))))

  ;(println  "Stage " stage)
  state)

(defn before-batch [state]
  (update state :event lc/invoke-before-batch))

(defn state-iteration 
  [prev-state replica-val]
  ;; FIXME next-state should copy init-event to event
  (let [next-state (event-state/next-state prev-state replica-val)
        event (:event next-state)]
    (if (and (first (alts!! [(:task-kill-ch event) (:kill-ch event)] :default true))
             (= :processing (:state prev-state)))
      (->> next-state 
           (print-stage 1)
           (gen-lifecycle-id)
           (print-stage 2)
           (poll-input-barriers)
           (emit-barriers)
           (print-stage 3)
           (poll-acks)
           (print-stage 4)
           (before-batch)
           (print-stage 5)
           (read-batch)
           (print-stage 6)
           (apply-fn)
           (print-stage 7)
           (build-new-segments)
           (print-stage 8)
           (assign-windows)
           (write-batch)
           (print-stage 9)
           ;(flow-retry-segments)
           (lc/invoke-after-batch)
           (print-stage 10)
           (ack-barriers))
      next-state)))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [init-state ex-f]
  (try
    (assert (:event init-state))
    (let [{:keys [task-kill-ch kill-ch task-information replica-atom opts state]} (:event init-state)] 
      (loop [prev-state init-state 
             replica-val @replica-atom]
        ;(println "Iteration " (:state prev-state))
        (let [state (state-iteration prev-state replica-val)]
          ; (assert (empty? (.__extmap event)) 
          ;         (str "Ext-map for Event record should be empty at start. Contains: " (keys (.__extmap event))))
          (if (first (alts!! [task-kill-ch kill-ch] :default true))
            (recur state @replica-atom)
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

(defn add-pipeline [{:keys [task-map] :as event}]
  (assoc-in event 
            [:state :pipeline] 
            ))

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

(defn final-event [component]
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
            coordinator (coordinator/new-peer-coordinator log (:messenger-group component) opts id job-id group-ch replica)
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
                            {:state :initial
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
      (let [last-event (final-event component)]
        (when-not (empty? (:triggers event))
          (ws/assign-windows last-event (:scheduler-event component)))

        (some-> last-event :state :coordinator coordinator/stop)
        (some-> last-event :state :messenger component/stop)
        (some-> last-event :state :pipeline (op/stop last-event))

        (close! (:task-kill-ch component))

        ; (when-let [state-log (:state-log event)] 
        ;   (state-extensions/close-log state-log event))

        ; (when-let [filter-state (:filter-state event)] 
        ;   (when (exactly-once-task? event)
        ;     (state-extensions/close-filter @filter-state event)))

        ((:compiled-after-task-fn event) event)))

    (assoc component
           :event nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer task]
  (map->TaskLifeCycle (merge peer task)))
