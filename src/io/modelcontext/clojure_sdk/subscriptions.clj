(ns io.modelcontext.clojure-sdk.subscriptions
  "Request-scoped subscription streams, never protocol sessions."
  (:require [io.modelcontext.clojure-sdk.protocol :as protocol]
            [jsonrpc4clj.server :as rpc]
            [promesa.core :as p]))

(def method-filter
  {"notifications/tools/list_changed" :toolsListChanged,
   "notifications/prompts/list_changed" :promptsListChanged,
   "notifications/resources/list_changed" :resourcesListChanged,
   "notifications/resources/updated" :resourceSubscriptions})

(defn listen!
  [context params]
  (let [id (:request-id (meta params))
        requested (:notifications params)
        capabilities @(:capabilities context)
        filter (cond-> {}
                 (and (:toolsListChanged requested)
                      (get-in capabilities [:tools :listChanged]))
                   (assoc :toolsListChanged true)
                 (and (:promptsListChanged requested)
                      (get-in capabilities [:prompts :listChanged]))
                   (assoc :promptsListChanged true)
                 (and (:resourcesListChanged requested)
                      (get-in capabilities [:resources :listChanged]))
                   (assoc :resourcesListChanged true)
                 (and (seq (:resourceSubscriptions requested))
                      (get-in capabilities [:resources :subscribe]))
                   (assoc :resourceSubscriptions
                     (:resourceSubscriptions requested)))
        emit! (or (:emit! context)
                  (when-let [server (:server context)]
                    #(rpc/send-notification server %1 %2)))
        pending (p/deferred)
        key (Object.)
        listeners (:listeners context)]
    (if-not (and (some? id)
                 emit!
                 (map? requested)
                 (every? boolean?
                         (vals (select-keys requested
                                            [:toolsListChanged
                                             :promptsListChanged
                                             :resourcesListChanged])))
                 (or (not (contains? requested :resourceSubscriptions))
                     (and (vector? (:resourceSubscriptions requested))
                          (every? string? (:resourceSubscriptions requested)))))
      (protocol/error -32602 "Invalid subscription notification filter")
      (do (when-let [callbacks (:on-cancel (meta params))]
            (swap! callbacks conj #(p/cancel! pending)))
          (locking listeners
            (emit! "notifications/subscriptions/acknowledged"
                   {:_meta {protocol/subscription-key id},
                    :notifications filter})
            (swap! (:listeners context) assoc
              key
              {:id id, :filter filter, :emit! emit!, :pending pending}))
          (p/finally pending (fn [_ _] (swap! (:listeners context) dissoc key)))
          pending))))

(defn publish!
  [context method params]
  (when-let [listeners (:listeners context)]
    (locking listeners
      (doseq [{:keys [id filter emit!]} (vals @listeners)]
        (when (if (= method "notifications/resources/updated")
                (some #{(:uri params)} (:resourceSubscriptions filter))
                (true? (get filter (method-filter method))))
          (emit! method
                 (assoc-in params [:_meta protocol/subscription-key] id)))))))

(defn close!
  [context]
  (doseq [{:keys [id pending]} (vals @(:listeners context))]
    (p/resolve! pending
                {:resultType "complete",
                 :_meta {protocol/subscription-key id}})))
