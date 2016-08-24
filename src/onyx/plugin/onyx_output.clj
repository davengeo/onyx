(ns onyx.plugin.onyx-output
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.peer.grouping :as g]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.messenger :as m]
            [clj-tuple :as t]))

(defprotocol OnyxOutput
  (write-batch [this event]))

(defn select-slot [job-task-id-slots hash-group route]
  (if (empty? hash-group)
    -1
    (if-let [hsh (get hash-group route)]
      ;; TODO: slow, not precomputed
      (let [n-slots (inc (apply max (vals (get job-task-id-slots route))))] 
        (mod hsh n-slots))    
      -1)))

(defn task-alive? [kill-ch task-kill-ch]
  (first (alts!! [kill-ch task-kill-ch] :default true)))

(defn try-send-until-success! [messenger batch task-slot kill-ch task-kill-ch]
  (loop []
    (when-not (and (m/offer-segments messenger batch task-slot)
                   ;; Need a better way to unwind the send messages stack
                   ;; Try to get rid of reduce?
                   (task-alive? kill-ch task-kill-ch))
      (recur))))

;; FIXME split out destinations for retry, may need to switch destinations, can'd do every thning in a single offer
;; TODO: be smart about sending messages to multiple co-located tasks
;; TODO: send more than one message at a time
(defn send-messages [{:keys [state id job-id task-id egress-tasks task->group-by-fn kill-ch task-kill-ch]} segments]
  (let [{:keys [replica messenger]} state
        grouped (group-by :flow segments)
        job-task-id-slots (get-in replica [:task-slot-ids job-id])]
    (run! (fn [[flow messages]]
            (run! (fn [{:keys [message]}]
                    (let [hash-group (g/hash-groups message egress-tasks task->group-by-fn)
                          task-slots (doall 
                                      (map (fn [route] 
                                             {:src-peer-id id
                                              :slot-id (select-slot job-task-id-slots hash-group route)
                                              :dst-task-id [job-id route]}) 
                                           flow))]
                      (run! (fn [task-slot]
                              (try-send-until-success! messenger [message] task-slot kill-ch task-kill-ch))
                            task-slots)))
                  messages))
          grouped)))

(extend-type Object
  OnyxOutput
  (write-batch [this {:keys [results state] :as event}]
    (let [segments (:segments results)]
      (info "Writing batch " 
            (m/replica-version (:messenger state)) (m/epoch (:messenger state)) 
            (:task-name event) (:task-type event) (vec (:segments results)))
      (send-messages event segments)
      {})))
