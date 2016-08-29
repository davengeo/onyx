(ns onyx.plugin.null
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.plugin.onyx-output :as o]
            [onyx.plugin.onyx-plugin :as p]))

(defrecord NullWriter [event]
  p/OnyxPlugin

  (start [this] this)

  (stop [this event]
    this)

  o/OnyxOutput

  (write-batch
    [_ {:keys [event]}]
    (let [{:keys [results]} event] 
      {:null/not-written (map :message (mapcat :leaves (:tree results)))})))

(defn output [event]
  (map->NullWriter {:event event}))
