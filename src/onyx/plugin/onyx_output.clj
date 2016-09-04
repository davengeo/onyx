(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.peer.grouping :as g]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (prepare-batch [this event])
  (write-batch [this event]))

(defn select-slot [job-task-id-slots hash-group route]
  (if (empty? hash-group)
    -1
    (if-let [hsh (get hash-group route)]
      ;; TODO: slow, not precomputed
      (let [n-slots (inc (apply max (vals (get job-task-id-slots route))))] 
        (mod hsh n-slots))    
      -1)))

;; TODO: split out destinations for retry, may need to switch destinations, can do every thing in a single offer
;; TODO: be smart about sending messages to multiple co-located tasks
;; TODO: send more than one message at a time
(defn send-messages [messenger replica prepared]
  (loop [messages prepared]
    (when-let [[message task-slot] (first messages)] 
      (if (m/offer-segments messenger [message] task-slot)
        (recur (rest messages))
        ;; blocked, return - state will be blocked
        messages))))

(extend-type Object
  OnyxOutput
  (prepare-batch [this {:keys [event replica] :as state}]
    ;; Flatten outputs in preparation for incremental sending in write-batch
    (let [{:keys [id job-id task-id egress-tasks results task->group-by-fn]} event
          segments (:segments results)
          grouped (group-by :flow segments)
          job-task-id-slots (get-in replica [:task-slot-ids job-id])
          output (reduce (fn [accum [flow messages]]
                           (reduce (fn [accum* {:keys [message]}]
                                     (let [hash-group (g/hash-groups message egress-tasks task->group-by-fn)
                                           task-slots (map (fn [route] 
                                                             {:src-peer-id id
                                                              :slot-id (select-slot job-task-id-slots hash-group route)
                                                              :dst-task-id [job-id route]}) 
                                                           flow)]
                                       (reduce conj! 
                                               accum* 
                                               (map (fn [task-slot]
                                                      [message task-slot]) 
                                                    task-slots))))
                                   accum
                                   messages))
                         (transient [])
                         grouped)]
      (assoc state :context (persistent! output))))

  (write-batch [this {:keys [event replica messenger context] :as state}]
    (let [remaining (send-messages messenger replica context)]
      (if (empty? remaining)
        (-> state
            (assoc :context [])
            (assoc :state :runnable))
        (-> state
            (assoc :context remaining)
            (assoc :state :blocked))))))
