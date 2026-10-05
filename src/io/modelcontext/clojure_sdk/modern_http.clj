(ns io.modelcontext.clojure-sdk.modern-http
  "Request-scoped Streamable HTTP binding; no protocol sessions."
  (:require [clojure.core.async :as async]
            [io.modelcontext.clojure-sdk.http-headers :as headers]
            [io.modelcontext.clojure-sdk.io-chan :as wire]
            [io.modelcontext.clojure-sdk.protocol :as protocol]
            [jsonrpc4clj.server :as rpc]
            [promesa.core :as p]))

(defn response
  [status id result]
  {:status status,
   :headers {"Content-Type" "application/json"},
   :body (wire/message->json-str
           (merge {:jsonrpc "2.0", :id id}
                  (if (:error result) result {:result result})))})

(defn- subscription-response
  [context message]
  (let [events (async/chan 32)
        callbacks (atom [])
        id (:id message)
        emit! (fn [method params]
                (async/>!! events
                           {:jsonrpc "2.0", :method method, :params params}))
        params (with-meta (:params message)
                 {:request-id id, :on-cancel callbacks})
        pending (rpc/receive-request (:method message)
                                     (assoc context :emit! emit!)
                                     params)]
    (if (map? pending)
      (response 400 id pending)
      (do (p/then pending
                  #(do (async/>!! events {:jsonrpc "2.0", :id id, :result %})
                       (async/close! events)))
          {:status 200,
           :headers {"Content-Type" "text/event-stream",
                     "Cache-Control" "no-cache",
                     "X-Accel-Buffering" "no"},
           :body (fn [output]
                   (try (let [writer (java.io.OutputStreamWriter.
                                       output
                                       java.nio.charset.StandardCharsets/UTF_8)]
                          (loop []
                            (let [[event channel] (async/alts!! [events
                                                                 (async/timeout
                                                                   1000)])]
                              (when (or event (not= channel events))
                                (.write writer
                                        (if event
                                          (str "data: "
                                               (wire/message->json-str event)
                                               "\n\n")
                                          ": keep-alive\n\n"))
                                (.flush writer)
                                (recur)))))
                        (catch java.io.IOException _ nil)
                        (finally (doseq [cancel! @callbacks] (cancel!))
                                 (async/close! events))))}))))

(defn handle
  [context request message]
  (let [method (:method message)
        id (:id message)
        params (:params message)
        tool (get-in @(:tools context) [(:name params) :tool])
        metadata-error (protocol/validate-request params)
        header-error (headers/validate (:headers request) message tool)]
    (cond
      (not (and (map? message)
                (= "2.0" (:jsonrpc message))
                (string? method)
                (or (not (contains? message :id)) (string? id) (integer? id))))
        (response 400 nil (protocol/error -32600 "Invalid request"))
      (and metadata-error (= -32602 (get-in metadata-error [:error :code])))
        (response 400 id metadata-error)
      header-error (response 400 id header-error)
      metadata-error (response 400 id metadata-error)
      (not (contains? message :id)) {:status 202}
      (= "subscriptions/listen" method) (subscription-response context message)
      :else (let [result (p/await (p/promise
                                    (rpc/receive-request method context params))
                                  30000)]
              (response (case (get-in result [:error :code])
                          -32601 404
                          (-32602 -32020 -32021 -32022) 400
                          200)
                        id
                        result)))))

(defn modern-request?
  [request message]
  (let [v (get-in request [:headers "mcp-protocol-version"])]
    (or (protocol/modern? (:params message))
        (= "server/discover" (:method message))
        (and v (not (#{"2024-11-05" "2025-03-26" "2025-06-18"} v))))))

(defn origin-allowed?
  [request allowed-origins]
  (let [origin (get-in request [:headers "origin"])]
    (or (nil? origin) (contains? (set allowed-origins) origin))))
