(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Message ->Barrier ->BarrierAck]]
            [onyx.messaging.messenger :as m]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [io.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(def fragment-limit-receiver 10)
(def global-fragment-limit 10)

(def no-op-error-handler
  (reify ErrorHandler
    ;; FIXME, this should probably cause restarting peers/ peer groups etc
    (onError [this x] (taoensso.timbre/warn x))))

(defn backoff-strategy [strategy]
  (case strategy
    :busy-spin (BusySpinIdleStrategy.)
    :low-restart-latency (BackoffIdleStrategy. 100
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 1000
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 10)
                                                (.toNanos TimeUnit/MICROSECONDS 1000))))


(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defn stream-id [job-id task-id slot-id]
  (hash [job-id task-id slot-id]))

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod m/get-peer-site :aeron
  [peer-config]
  (println "GET PEER SITE" (mc/external-addr peer-config))
  {:address (mc/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

(defrecord AeronMessagingPeerGroup [peer-config]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? peer-config)
          threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading peer-config))
          media-driver-context (if embedded-driver?
                                 (-> (MediaDriver$Context.) 
                                     (.threadingMode threading-mode)
                                     (.dirsDeleteOnStart true)))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))
          bind-addr (common/bind-addr peer-config)
          external-addr (common/external-addr peer-config)
          port (:onyx.messaging/peer-port peer-config)
          poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy peer-config)
          offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy peer-config)
          send-idle-strategy (backoff-strategy poll-idle-strategy-config)
          receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
          compress-f (or (:onyx.messaging/compress-fn peer-config) messaging-compress)
          decompress-f (or (:onyx.messaging/decompress-fn peer-config) messaging-decompress)
          ticket-counters (atom {})
          ctx (.errorHandler (Aeron$Context.) no-op-error-handler)]
      (when embedded-driver? 
        (.addShutdownHook (Runtime/getRuntime) 
                          (Thread. (fn [] 
                                     (.deleteAeronDirectory ^MediaDriver$Context media-driver-context)))))
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :media-driver-context media-driver-context
             :media-driver media-driver
             :compress-f compress-f
             :decompress-f decompress-f
             :ticket-counters ticket-counters
             :port port
             :send-idle-strategy send-idle-strategy)))

  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component
           :bind-addr nil :external-addr nil :media-driver nil :media-driver-context nil 
           :external-channel nil :compress-f nil :decompress-f nil :ticket-counters nil 
           :send-idle-strategy nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessagingPeerGroup {:peer-config peer-config}))

(defn barrier? [v]
  (instance? onyx.types.Barrier v))

(defn message? [v]
  (instance? onyx.types.Message v))

(defn ack? [v]
  (instance? onyx.types.BarrierAck v))

(defn subscripion-ticket 
  [{:keys [replica-version ticket-counters] :as messenger} 
   {:keys [dst-task-id src-peer-id] :as sub}]
  (get-in @ticket-counters [replica-version [src-peer-id dst-task-id]]))

(defn subscription-aligned?
  [sub-ticket]
  (empty? (:aligned sub-ticket)))

;; FIXME, need a way to bail out of sending out the initial barriers
;; as peers may no longer exist
;; Should check some replica atom here to see if anything has changed
;; Or maybe tasks need some flag that says stuff is out of date or task is dead
(defn offer-until-success! [messenger task-slot message]
  (let [publication ^Publication (:publication task-slot)
        buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress message))]
    (while (let [ret (.offer ^Publication publication buf 0 (.capacity buf))] 
             (when (neg? ret) (println "OFFERED, GOT" ret))
             (when (= ret Publication/CLOSED)
               (throw (Exception. "Wrote to closed publication.")))
             (neg? ret))
      (println "Re-offering message, session-id" (.sessionId publication))))
  messenger)

(defn is-next-barrier? [messenger barrier]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (inc (m/epoch messenger)) (:epoch barrier))))

(defn found-next-barrier? [messenger {:keys [barrier] :as subscriber}]
  (let [barrier-val @barrier] 
    (and (is-next-barrier? messenger barrier-val) 
         (not (:emitted? barrier-val)))))

(defn unblocked? [messenger {:keys [barrier] :as subscriber}]
  (let [barrier-val @barrier] 
    (and (= (m/replica-version messenger) (:replica-version barrier-val))
         (= (m/epoch messenger) (:epoch barrier-val))
         (:emitted? barrier-val))))

(defn handle-read-segments
  [messenger results barrier ticket dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)
        ;; FIXME, why 2?
        n-desired-messages 2
        ticket-val @ticket]
    #_(info "handling message " (type message) (into {} message)
          "reading task"
          dst-task-id
          "peer" 
          src-peer-id
          "our epoch"
          (m/epoch messenger)
          "our replica"
          (m/replica-version messenger)
          (is-next-barrier? messenger message))
    (cond (>= (count @results) n-desired-messages)
          ControlledFragmentHandler$Action/ABORT
          (and (= (:dst-task-id message) dst-task-id)
               (= (:src-peer-id message) src-peer-id))
          (cond (and (message? message)
                     (not (nil? @barrier))
                     (< ticket-val offset))
                (do 
                 (assert (= (m/replica-version messenger) (:replica-version message)))
                 ;; FIXME, not sure if this logically works.
                 ;; If ticket gets updated in mean time, then is this always invalid and should be continued?
                 ;; WORK OUT ON PAPER
                 (when (compare-and-set! ticket ticket-val offset)
                   (do 
                    (assert (coll? (:payload message)))
                    (swap! results into (:payload message))))
                 ControlledFragmentHandler$Action/CONTINUE)

                (and (barrier? message)
                     (> (m/replica-version messenger)
                        (:replica-version message)))
                ControlledFragmentHandler$Action/CONTINUE

                (and (barrier? message)
                     (< (m/replica-version messenger)
                        (:replica-version message)))
                ControlledFragmentHandler$Action/ABORT

                (and (barrier? message)
                     (is-next-barrier? messenger message))
                (if (empty? @results)
                  (do 
                   (reset! barrier message)
                   ControlledFragmentHandler$Action/BREAK)  
                  ControlledFragmentHandler$Action/ABORT)

                (and (barrier? message)
                     (= (m/replica-version messenger)
                        (:replica-version message)))
                (throw (Exception. "Should not happen"))

                ;; This can happen when ticketing and we're ignoring the message
                :else 
                ControlledFragmentHandler$Action/CONTINUE)

          (= (:replica-version message) (m/replica-version messenger))
          ControlledFragmentHandler$Action/CONTINUE
          
          :else
          ControlledFragmentHandler$Action/ABORT)))

;; TODO, do not re-ify on every read
(defn controlled-fragment-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

;; NEXT, poll acks + barriers. If you hit a barrier, just ABORT.
;; In read-messages, if you hit a barrier ack, abort

;; In that case probably need a separate message reader for just barriers and acks

(defn poll-messages! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription barrier]} sub-info
        sub-ticket (subscripion-ticket messenger sub-info)]
    ;; May not need to check for alignment here, can prob just do in :recover
    (if (and (subscription-aligned? sub-ticket)
             (unblocked? messenger sub-info))
      (let [results (atom [])
            ;; FIXME, maybe shouldn't reify a controlled fragment handler each time?
            ;; Put the fragment handler in the sub info?
            fh (controlled-fragment-data-handler
                (fn [buffer offset length header]
                  (handle-read-segments messenger results barrier (:ticket sub-ticket) dst-task-id src-peer-id buffer offset length header)))]
        (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
        @results)
      [])))

(defn handle-poll-new-barrier
  [messenger barrier dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)]
    (cond (and (= (:dst-task-id message) dst-task-id)
               (= (:src-peer-id message) src-peer-id)
               (barrier? message)
               ;(nil? @barrier)
               (is-next-barrier? messenger message))
          (do 
          ; (info "GOT NEW BARRIER ON RECOVER" (:id messenger) (into {} message) dst-task-id)
           (reset! barrier message)
           ControlledFragmentHandler$Action/BREAK)

          (or (not= (:dst-task-id message) dst-task-id)
              (not= (:src-peer-id message) src-peer-id)
              (< (:replica-version message) (m/replica-version messenger)))
          ControlledFragmentHandler$Action/CONTINUE

          :else
          ControlledFragmentHandler$Action/ABORT)))

(defn poll-new-replica! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription barrier]} sub-info
        sub-ticket (subscripion-ticket messenger sub-info)]
    ;; May not need to check for alignment here, can prob just do in :recover

    #_(info "POLLING SUB" 
          (subscription-aligned? sub-ticket)
          sub-info
          )
    (if (subscription-aligned? sub-ticket)
      (let [fh (controlled-fragment-data-handler
                (fn [buffer offset length header]
                  (handle-poll-new-barrier messenger barrier dst-task-id src-peer-id buffer offset length header)))]
        (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver))
      (info "SUB NOT ALIGNED")
      
      )))

;; TODO, can possibly take more than one ack at a time from a sub?
(defn handle-poll-acks [messenger barrier-ack dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)]
    (if (and (= (:dst-task-id message) dst-task-id)
             (= (:src-peer-id message) src-peer-id)
             (ack? message)
             (= (m/replica-version messenger) (:replica-version message)))
      (do 
       (info "GOT NEW BARRIER ACK" (into {} message))
       (reset! barrier-ack message)
       ControlledFragmentHandler$Action/BREAK)
      ControlledFragmentHandler$Action/CONTINUE)))

(defn poll-acks! [messenger sub-info]
  (if @(:barrier-ack sub-info)
    messenger
    (let [{:keys [src-peer-id dst-task-id subscription barrier-ack ticket-counter]} sub-info
          fh (controlled-fragment-data-handler
              (fn [buffer offset length header]
                (handle-poll-acks messenger barrier-ack dst-task-id src-peer-id buffer offset length header)))]
      (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
      messenger)))

(defn new-subscription 
  [{:keys [messenger-group id] :as messenger}
   {:keys [job-id src-peer-id dst-task-id slot-id] :as sub-info}]
  (info "new subscriber for " job-id src-peer-id dst-task-id)
  (let [error-handler (reify ErrorHandler
                        (onError [this x] 
                          ;; FIXME: inadequate
                          (taoensso.timbre/warn x "Aeron messaging subscriber error")))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        ;; Maybe use site from sub-info instead?
        bind-addr (:bind-addr messenger-group)
        port (:port messenger-group)
        channel (mc/aeron-channel bind-addr port)
        stream (stream-id job-id dst-task-id slot-id)
        _ (println "Add subscription " channel stream)
        subscription (.addSubscription conn channel stream)]
    (assoc sub-info
           :subscription subscription
           :conn conn
           :barrier-ack (atom nil)
           :barrier (atom nil))))

(defn new-publication 
  [{:keys [messenger-group] :as messenger}
   {:keys [job-id src-peer-id dst-task-id slot-id site] :as pub-info}]
  (let [channel (mc/aeron-channel (:address site) (:port site))
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          (taoensso.timbre/warn "Aeron messaging publication error:" x)))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        stream (stream-id job-id dst-task-id slot-id)
        _ (println "Creating new pub" channel stream)
        pub (.addPublication conn channel stream)]
    (assoc pub-info :conn conn :publication pub)))

(defn add-to-subscriptions [subscriptions sub-info]
  (conj (or subscriptions []) sub-info))

(defn remove-from-subscriptions [subscriptions {:keys [dst-task-id slot-id] :as sub-info}]
  ;; FIXME close subscription
  (update-in subscriptions
             [dst-task-id slot-id]
             (fn [ss] 
               (filterv (fn [s] 
                        (not= (select-keys sub-info [:src-peer-id :dst-task-id :slot-id :site]) 
                              (select-keys s [:src-peer-id :dst-task-id :slot-id :site])))
                      ss))))

(defn remove-from-publications [publications pub-info]
  ;; FIXME close publication
  (filterv (fn [p] 
             (not= (select-keys pub-info [:src-peer-id :dst-task-id :slot-id :site]) 
                   (select-keys p [:src-peer-id :dst-task-id :slot-id :site])))
           publications))

;; TICKETS SHOULD USE session id (unique publication) and position
;; Lookup task, then session id, then position, skip over positions that are lower, use ticket to take higher

;; Stick tickets in peer messenger group in single atom?
;; Have tickets be cleared up when image is no longer available?
;; Use these to manage tickets
;; onAvailableImage
;; onUnavailableImage

(defn flatten-publications [publications]
  (reduce (fn [all [dst-task-id ps]]
            (into all (mapcat (fn [[slot-id pubs]]
                                pubs)
                              ps)))
          []
          publications))

(defn set-barrier-emitted! [subscriber]
  (swap! (:barrier subscriber) assoc :emitted? true))

(defn allocation-changed? [replica job-id replica-version]
  (and (some #{job-id} (:jobs replica))
       (not= replica-version
             (get-in replica [:allocation-version job-id]))))

(defrecord AeronMessenger 
  [messenger-group ticket-counters id replica-version epoch publications subscriptions ack-subscriptions read-index !replica]

  component/Lifecycle
  (start [component]
    (println "Replica is " @!replica)
    (println "Ticket counters " (:ticket-counters messenger-group))
    (assoc component
           :ticket-counters 
           (:ticket-counters messenger-group)))

  (stop [component]
    (run! (fn [pub]
            (.close ^Publication (:publication pub)))
          (flatten-publications publications))
    (run! (fn [sub]
            (.close ^Subscription (:subscription sub)))
          (concat subscriptions ack-subscriptions))
    (assoc component 
           :ticket-counters nil
           :replica-version nil :epoch nil
           :publications nil :subscription nil 
           :ack-subscriptions nil :read-index nil))

  m/Messenger
  (publications [messenger]
    publications)

  (subscriptions [messenger]
    subscriptions)

  (ack-subscriptions [messenger]
    ack-subscriptions)

  (add-subscription
    [messenger sub-info]
    (-> messenger 
        (update :subscriptions add-to-subscriptions (new-subscription messenger sub-info))
        ;; FIXME, switch to swap on shared atom
        (update-in [:tickets (:src-peer-id sub-info) (:dst-task-id sub-info) (:slot-id sub-info)] 
                   #(or % 0))))

  (register-ticket [messenger sub-info]
    (swap! ticket-counters 
           update 
           replica-version 
           (fn [tickets]
             (update (or tickets {}) 
                     [(:src-peer-id sub-info)
                      (:dst-task-id sub-info)]
                     (fn [sub-ticket]
                       (if sub-ticket
                         ;; Already know what peers should be aligned
                         (update sub-ticket :aligned disj id)
                         {:ticket (atom -1)
                          :aligned (disj (set (:aligned-peers sub-info)) id)})))))
    messenger)

  (add-ack-subscription
    [messenger sub-info]
    (update messenger :ack-subscriptions add-to-subscriptions (new-subscription messenger sub-info)))

  (remove-subscription
    [messenger sub-info]
    (.close ^Subscription (:subscription sub-info))
    (update messenger :subscriptions remove-from-subscriptions sub-info))

  (remove-ack-subscription
    [messenger sub-info]
    (.close ^Subscription (:subscription sub-info))
    (update messenger :ack-subscriptions remove-from-subscriptions sub-info))

  (add-publication
    [messenger pub-info]
    (update-in messenger
               [:publications (:dst-task-id pub-info) (:slot-id pub-info)]
               (fn [pbs] 
                 (assert (= id (:src-peer-id pub-info)) [id (:src-peer-id pub-info)] )
                 (conj (or pbs []) 
                       (new-publication messenger pub-info)))))

  (remove-publication
    [messenger pub-info]
    (.close ^Publication (:publication pub-info))
    (update messenger :publications remove-from-publications pub-info))

  (set-replica-version [messenger replica-version]
    (reset! (:read-index messenger) 0)
    (run! (fn [sub] (reset! (:barrier-ack sub) nil)) ack-subscriptions)
    (run! (fn [sub] (reset! (:barrier sub) nil)) subscriptions)
    (-> messenger 
        (assoc :replica-version replica-version)
        ;(update :subscriptions (fn [ss] (mapv #(assoc % :barrier-ack nil :barrier nil) ss)))
        (m/set-epoch 0)))

  (replica-version [messenger]
    (get messenger :replica-version))

  (epoch [messenger]
    epoch)

  (set-epoch 
    [messenger epoch]
    (assoc messenger :epoch epoch))

  (next-epoch
    [messenger]
    (update messenger :epoch inc))

  (poll-acks [messenger]
    (reduce poll-acks! messenger ack-subscriptions))

  (all-acks-seen? 
    [messenger]
    (if (empty? (remove (comp deref :barrier-ack) ack-subscriptions))
      (select-keys @(:barrier-ack (first ack-subscriptions)) 
                   [:replica-version :epoch])))

  (flush-acks [messenger]
    (run! (fn [sub] (reset! (:barrier-ack sub) nil)) ack-subscriptions)
    messenger)

  (poll [messenger]
    ;; TODO, should loop until got enough messages for a batch
    ;(info "LOOPING OVER " (count subscriptions))
    (let [subscriber (get subscriptions (mod @(:read-index messenger) (count subscriptions)))
          messages (poll-messages! messenger subscriber)] 
      (swap! (:read-index messenger) inc)
      (mapv t/input messages)))

  (offer-segments [messenger batch {:keys [dst-task-id slot-id] :as task-slot}]
    ;; Problem here is that if no slot will accept the message we will
    ;; end up having to recompress on the next offer
    ;; Possibly should try more than one iteration before returning
    ;; TODO: should re-use unsafe buffers in aeron messenger
    (let [payload ^bytes (messaging-compress (->Message id dst-task-id slot-id replica-version batch))
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      (loop [pubs (get-in publications [dst-task-id slot-id])]
        (if-let [pub-info (first pubs)]
          (let [ret (.offer ^Publication (:publication pub-info) buf 0 (.capacity buf))]
            (if (neg? ret)
              (recur (rest pubs))
              ;; Return the publication info for the publication that it was sent to
              ;; as evidence of success
              pub-info))))))

  (poll-recover [messenger]
    (loop [sbs subscriptions]
      (let [sub (first sbs)] 
        (when sub 
          (when-not @(:barrier sub)
            (poll-new-replica! messenger sub))
          (recur (rest sbs)))))
    ;(info "ON REC" id (m/all-barriers-seen? messenger))
    (if (m/all-barriers-seen? messenger)
      (let [recover (:recover @(:barrier (first subscriptions)))] 
        (assert (= 1 (count (set (map (comp :recover deref :barrier) subscriptions)))))
        (assert recover)
        recover)))

  (emit-barrier [messenger]
    (onyx.messaging.messenger/emit-barrier messenger {}))

  (emit-barrier [messenger barrier-opts]
    (run! set-barrier-emitted! (:subscriptions messenger))
    (as-> messenger mn
      (m/next-epoch mn)
      (reduce (fn [m p] 
                (info "Emitting barrier " id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                (println "Connected? " (.isConnected (:publication p)))
                (offer-until-success! m p (merge (->Barrier id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                                                 barrier-opts))) 
              mn
              (flatten-publications publications))))

  (all-barriers-seen? [messenger]
    (empty? (remove #(found-next-barrier? messenger %) 
                    subscriptions)))

  (emit-barrier-ack [messenger]
    (run! set-barrier-emitted! subscriptions)
    (as-> messenger mn 
      (reduce (fn [m p] 
                (offer-until-success! m p (->BarrierAck id (:dst-task-id p) (m/replica-version mn) (m/epoch mn)))) 
              mn 
              (flatten-publications publications))
      (m/next-epoch mn))))

(defmethod m/build-messenger :aeron [peer-config messenger-group id !replica]
  (println "Building aeron messenger")
  (map->AeronMessenger {:id id 
                        :peer-config peer-config 
                        :messenger-group messenger-group 
                        :!replica !replica
                        :read-index (atom 0)}))

; (defrecord AeronMessenger
;   [peer-group messenger-group publication-group publications
;    send-idle-strategy compress-f monitoring short-ids acking-ch]
;   component/Lifecycle

;   (start [component]
;     (taoensso.timbre/info "Starting Aeron Messenger")
;     (let [config (:config peer-group)
;           messenger-group (:messenger-group peer-group)
;           publications (atom {})
;           send-idle-strategy (:send-idle-strategy messenger-group)
;           compress-f (:compress-f messenger-group)
;           short-ids (atom {})]
;       (assoc component
;              :messenger-group messenger-group
;              :short-ids short-ids
;              :send-idle-strategy send-idle-strategy
;              :publications publications
;              :compress-f compress-f)))

;   (stop [{:keys [short-ids publications] :as component}]
;     (taoensso.timbre/info "Stopping Aeron Messenger")
;     (run! (fn [{:keys [pub conn]}] 
;             (.close pub)
;             (.close conn)) 
;           (vals @publications))

;     (assoc component
;            :messenger-group nil
;            :send-idle-strategy nil
;            :publications nil
;            :short-ids nil
;            :compress-f nil)))

; #_(defmethod extensions/register-task-peer AeronMessenger
;   [{:keys [short-ids] :as messenger}
;    {:keys [aeron/peer-task-id]}
;    task-buffer]
;   #_(swap! short-ids assoc :peer-task-short-id peer-task-id))

; #_(defmethod extensions/unregister-task-peer AeronMessenger
;   [{:keys [short-ids] :as messenger}
;    {:keys [aeron/peer-task-id]}]
;   #_(swap! short-ids dissoc peer-task-id))

; (defrecord AeronPeerGroup [opts subscribers ticketing-counters compress-f decompress-f send-idle-strategy]
;   component/Lifecycle
;   (start [component]
;     (taoensso.timbre/info "Starting Aeron Peer Group")
;     (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? opts)
;           threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading opts))

;           media-driver-context (if embedded-driver?
;                                  (-> (MediaDriver$Context.) 
;                                      (.threadingMode threading-mode)
;                                      (.dirsDeleteOnStart true)))

;           media-driver (if embedded-driver?
;                          (MediaDriver/launch media-driver-context))

;           bind-addr (common/bind-addr opts)
;           external-addr (common/external-addr opts)
;           port (:onyx.messaging/peer-port opts)
;           poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts)
;           offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy opts)
;           send-idle-strategy (backoff-strategy poll-idle-strategy-config)
;           receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
;           compress-f (or (:onyx.messaging/compress-fn opts) messaging-compress)
;           decompress-f (or (:onyx.messaging/decompress-fn opts) messaging-decompress)
;           ticketing-counters (atom {})
;           ctx (.errorHandler (Aeron$Context.) no-op-error-handler)]
;       (when embedded-driver? 
;         (.addShutdownHook (Runtime/getRuntime) 
;                           (Thread. (fn [] 
;                                      (.deleteAeronDirectory ^MediaDriver$Context media-driver-context)))))
;       (assoc component
;              :bind-addr bind-addr
;              :external-addr external-addr
;              :media-driver-context media-driver-context
;              :media-driver media-driver
;              :compress-f compress-f
;              :decompress-f decompress-f
;              :ticketing-counters ticketing-counters
;              :port port
;              :send-idle-strategy send-idle-strategy)))

;   (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
;     (taoensso.timbre/info "Stopping Aeron Peer Group")

;     (when media-driver (.close ^MediaDriver media-driver))
;     (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
;     (assoc component
;            :bind-addr nil :external-addr nil :media-driver nil :media-driver-context nil 
;            :external-channel nil :compress-f nil :decompress-f nil :ticketing-counters nil 
;            :send-idle-strategy nil)))

; (defmethod clojure.core/print-method AeronPeerGroup
;   [system ^java.io.Writer writer]
;   (.write writer "#<Aeron Peer Group>"))

; (defn aeron-peer-group [opts]
;   (map->AeronPeerGroup {:opts opts}))

; (def possible-ids
;   (set (map short (range -32768 32768))))

; (defn available-ids [used]
;   (clojure.set/difference possible-ids used))

; (defn choose-id [hsh used]
;   (when-let [available (available-ids used)]
;     (nth (seq available) (mod hsh (count available)))))

; (defn allocate-id [peer-id peer-site peer-sites]
;   ;;; Assigns a unique id to each peer so that messages do not need
;   ;;; to send the entire peer-id in a payload, saving 14 bytes per
;   ;;; message
;   (let [used-ids (->> (vals peer-sites)
;                       (filter
;                         (fn [s]
;                           (= (:aeron/external-addr peer-site)
;                              (:aeron/external-addr s))))
;                       (map :aeron/peer-id)
;                       set)
;         id (choose-id peer-id used-ids)]
;     (when-not id
;       (throw (ex-info "Couldn't assign id. Ran out of aeron ids. 
;                       This should only happen if more than 65356 virtual peers have been started up on a single external addr."
;                       peer-site)))
;     id))


; (defn aeron-messenger [peer-config messenger-group]
;   (map->AeronMessenger {:peer-config peer-config :messenger-group messenger-group}))

; #_(defmethod m/peer-site AeronMessenger
;   [messenger]
;   {:aeron/external-addr (:external-addr (:messenger-group messenger))
;    :aeron/port (:port (:messenger-group messenger))})

; (defrecord AeronPeerConnection [channel stream-id peer-task-id])

; ;; Define stream-id as only allowed stream
; (def stream-id 1)

; ; (defmethod m/connection-spec AeronMessenger
; ;   [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/peer-task-id] :as peer-site}]
; ;   (->AeronPeerConnection (mc/aeron-channel external-addr port) stream-id peer-task-id))





; #_(defmethod m/close-partial-subscriber AeronMessenger
;   [{:keys [messenger-group] :as messenger} partial-subscriber]
;   (info "Closing partial subscriber")
;   (.close ^Subscription (:subscription partial-subscriber))
;   (.close ^Aeron (:conn partial-subscriber)))

; (defn rotate [xs]
;   (if (seq xs)
;     (conj (into [] (rest xs)) (first xs))
;     xs))

; (defn task-alive? [event]
;   (first (alts!! [(:kill-ch event) (:task-kill-ch event)] :default true)))

; #_(defmethod m/receive-messages AeronMessenger
;   [messenger {:keys [task-map id task-id task 
;                                 subscription-maps]
;                          :as event}]
;   (let [rotated-subscriptions (swap! subscription-maps rotate)
;         next-subscription (first (filter (comp nil? deref :barrier) rotated-subscriptions))]
;     (if next-subscription
;       (let [{:keys [subscription src-peer-id counter ticket-counter barrier]} next-subscription
;             results (atom [])
;             fh (controlled-fragment-data-handler
;                  (fn [buffer offset length header]
;                    (handle-message barrier results counter ticket-counter id task-id src-peer-id buffer offset length header)))]
;         (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
;         @results)
;       [])))



; (defn write [^Publication pub ^UnsafeBuffer buf]
;   ;; Needs an escape mechanism so it can break if a peer is shutdown
;   ;; Needs an idle mechanism to prevent cpu burn
;   (while (let [ret (.offer pub buf 0 (.capacity buf))] 
;            (when (= ret Publication/CLOSED)
;              (throw (Exception. "Wrote to closed publication.")))
;            (neg? ret))
;     (info "Re-offering message, session-id" (.sessionId pub))))

; #_(defmethod m/offer-segments AeronMessenger
;   [messenger publication batch]
;   (doseq [b batch]
;     (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress b))]
;       (write publication buf))))

; #_(defmethod m/send-barrier AeronMessenger
;   [messenger publication barrier]
;   (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress barrier))]
;     (write publication buf)))

; #_(defmethod m/ack-barrier AeronMessenger
;   [messenger publication ack-message]
;   (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress ack-message))]
;     (write publication buf)))
