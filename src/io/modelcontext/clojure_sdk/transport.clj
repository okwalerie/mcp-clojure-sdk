(ns io.modelcontext.clojure-sdk.transport
  "MCP framing rules around jsonrpc4clj's channel endpoint."
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [io.modelcontext.clojure-sdk.specs :as specs]
            [io.modelcontext.clojure-sdk.protocol :as protocol]
            [jsonrpc4clj.server :as rpc]))

(defn request? [m] (and (map? m) (contains? m :id) (string? (:method m))))
(defn valid-id? [id] (or (string? id) (integer? id)))

(defn chan-server
  [{:keys [input-ch output-ch id-valid?], :or {id-valid? valid-id?}, :as opts}]
  (let [incoming (async/chan 16)
        outgoing (async/chan 16)
        active (atom {})
        ids (atom {})
        legacy? (atom false)
        phase (atom :new)
        endpoint (assoc (rpc/chan-server (assoc opts
                                           :input-ch incoming
                                           :output-ch outgoing))
                   :mcp/context* (atom nil))]
    (async/thread
      (loop []
        (if-some [message (async/<!! input-ch)]
          (let [id (:id message)
                method (:method message)
                notification? (and (map? message) (not (contains? message :id)))
                params (or (:params message) {})
                valid? (and (map? message)
                            (= "2.0" (:jsonrpc message))
                            (string? method)
                            (map? params)
                            (or notification? (id-valid? id)))
                reply! (fn [code text]
                         (async/>!! output-ch
                                    {:jsonrpc "2.0",
                                     :id (when (id-valid? id) id),
                                     :error {:code code, :message text}}))]
            (cond
              (= :parse-error message) (reply! -32700 "Parse error")
              (and @legacy? (map? message) (not method) (contains? message :id))
                (async/>!! incoming message)
              (not valid?) (when-not notification?
                             (reply! -32600 "Invalid request"))
              (and notification? (= method "notifications/cancelled"))
                (when (contains? @active (:requestId params))
                  (swap! active assoc-in [(:requestId params) :cancelled?] true)
                  (doseq [cancel! @(get-in @active
                                           [(:requestId params) :on-cancel])]
                    (cancel!))
                  (async/>!! incoming
                             {:jsonrpc "2.0",
                              :method "$/cancelRequest",
                              :params {:id (get-in @active
                                                   [(:requestId params)
                                                    :internal-id])}}))
              notification? (when @legacy?
                              (when (and (= method "notifications/initialized")
                                         (= :awaiting @phase))
                                (reset! phase :ready))
                              (async/>!! incoming message))
              (and (= method "initialize")
                   (not (protocol/modern? params))
                   (not= :new @phase))
                (reply! -32600 "Already initialized")
              (and (not (protocol/modern? params))
                   (not (#{"initialize" "ping" "server/discover"} method))
                   (not= :ready @phase))
                (reply! -32600 "Initialize before sending legacy requests")
              (contains? @active id) (reply! -32600
                                             "Request ID already in flight")
              :else (let [internal-id (str (random-uuid))]
                      (swap! ids assoc internal-id id)
                      (when (= method "initialize")
                        (reset! legacy? true)
                        (when (s/valid? ::specs/initialize-request params)
                          (reset! phase :awaiting)))
                      (swap! active assoc
                        id
                        {:internal-id internal-id,
                         :cancelled? false,
                         :method method,
                         :modern? (protocol/modern? params),
                         :on-cancel (atom [])})
                      (async/>!! incoming
                                 (assoc message
                                   :id internal-id
                                   :params (with-meta params
                                             {:request-id id,
                                              :on-cancel
                                              (get-in @active
                                                      [id :on-cancel])})))))
            (recur))
          (do
            (doseq [state (vals @active) cancel! @(:on-cancel state)] (cancel!))
            (async/close! incoming)))))
    (async/thread
      (loop []
        (if-some [message (async/<!! outgoing)]
          (do
            (cond
              (and (contains? message :id) (not (:method message)))
                (let [internal-id (:id message)
                      id (get @ids internal-id internal-id)
                      message (assoc message :id id)
                      state (get @active id)]
                  (when (and (= "initialize" (:method state)) (:result message))
                    (when-not (= :ready @phase) (reset! phase :awaiting)))
                  (when-not (:cancelled? state) (async/>!! output-ch message))
                  (swap! active dissoc id)
                  (swap! ids dissoc internal-id))
              (contains? message :id) (when @legacy?
                                        (async/>!! output-ch message))
              :else (let [subscription-id (get-in message
                                                  [:params :_meta
                                                   protocol/subscription-key])]
                      (when (or (and subscription-id
                                     (contains? @active subscription-id)
                                     (not (get-in @active
                                                  [subscription-id
                                                   :cancelled?])))
                                (and (nil? subscription-id) @legacy?))
                        (async/>!! output-ch message))))
            (recur))
          (async/close! output-ch))))
    endpoint))
