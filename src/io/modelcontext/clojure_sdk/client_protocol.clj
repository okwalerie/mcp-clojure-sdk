(ns io.modelcontext.clojure-sdk.client-protocol
  "Modern client requests and explicit multi-round-trip responses."
  (:require [io.modelcontext.clojure-sdk.protocol :as protocol]
            [io.modelcontext.clojure-sdk.http-headers :as headers]
            [jsonrpc4clj.server :as rpc]
            [promesa.core :as p]))

(defn metadata
  [state]
  {protocol/version-key protocol/version,
   protocol/client-info-key (:client-info state),
   protocol/capabilities-key (cond-> {}
                               (:sampling-handler state) (assoc :sampling {})
                               (:elicitation-handler state)
                                 (assoc :elicitation {:form {}, :url {}})
                               (seq (:roots state)) (assoc :roots {}))})

(defn- input-responses
  [state requests]
  (into {}
        (map (fn [[id request]]
               [id
                (case (:method request)
                  "roots/list" {:roots (:roots state)}
                  "sampling/createMessage"
                    (if-let [f (:sampling-handler state)]
                      (f (:params request))
                      (throw (ex-info "Sampling not enabled" {})))
                  "elicitation/create"
                    (if-let [f (:elicitation-handler state)]
                      (f (:params request))
                      (throw (ex-info "Elicitation not enabled" {})))
                  (throw (ex-info "Unsupported input request"
                                  {:method (:method request)})))])
          requests)))

(defn request!
  [client method params]
  (let [state @(:state client)]
    (if (not= protocol/version (:protocol-version state))
      (rpc/send-request (:endpoint client) method params)
      (letfn
        [(send [params rounds]
           (let [params
                   (update params :_meta #(merge (metadata @(:state client)) %))
                 pending (rpc/send-request (:endpoint client) method params)]
             (assoc pending
               :p (p/then
                    (:p pending)
                    (fn [result]
                      (case (get result :resultType "complete")
                        "complete"
                          (if (= method "tools/list")
                            (let [tools (filterv (fn [tool]
                                                   (try (headers/annotations
                                                          (:inputSchema tool))
                                                        true
                                                        (catch Exception _
                                                          false)))
                                          (:tools result))]
                              (swap! (:state client) assoc
                                :tools
                                (into {} (map (juxt :name identity) tools)))
                              (assoc result :tools tools))
                            result)
                        "input_required"
                          (do (when (>= rounds 8)
                                (throw (ex-info "MRTR round limit reached"
                                                {:limit 8})))
                              (when-not (protocol/interactive-methods method)
                                (throw (ex-info
                                         "Unexpected input_required result"
                                         {:method method})))
                              (let [retry-params
                                      (cond-> (dissoc params
                                                :inputResponses
                                                :requestState)
                                        (:inputRequests result)
                                          (assoc :inputResponses
                                            (input-responses @(:state client)
                                                             (:inputRequests
                                                               result)))
                                        (contains? result :requestState)
                                          (assoc :requestState
                                            (:requestState result)))]
                                (:p (send retry-params (inc rounds)))))
                        (throw (ex-info "Unknown result type"
                                        {:resultType (:resultType
                                                       result)}))))))))]
        (send params 0)))))

(defn discover!
  [client]
  (rpc/send-request (:endpoint client)
                    "server/discover"
                    {:_meta (metadata @(:state client))}))

(defn adopt!
  [client result]
  (if (some #{protocol/version} (:supportedVersions result))
    (do (swap! (:state client) assoc
          :protocol-version protocol/version
          :initialized? true
          :server-info (get-in result [:_meta protocol/server-info-key])
          :server-capabilities (:capabilities result))
        result)
    {:error {:code -32022,
             :message "No mutually supported modern protocol version",
             :data {:supported (:supportedVersions result)}}}))
