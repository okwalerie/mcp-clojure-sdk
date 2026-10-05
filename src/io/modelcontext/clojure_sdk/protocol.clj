(ns io.modelcontext.clojure-sdk.protocol
  "Per-request MCP 2026-07-28 semantics, independent of transport."
  (:require [clojure.spec.alpha :as s]
            [io.modelcontext.clojure-sdk.specs :as specs]
            [promesa.core :as p]))

(def version "2026-07-28")
(def version-key :io.modelcontextprotocol/protocolVersion)
(def capabilities-key :io.modelcontextprotocol/clientCapabilities)
(def client-info-key :io.modelcontextprotocol/clientInfo)
(def server-info-key :io.modelcontextprotocol/serverInfo)
(def subscription-key :io.modelcontextprotocol/subscriptionId)
(def ^:dynamic *request* nil)
(def removed-methods #{"initialize" "ping" "logging/setLevel"
                       "resources/subscribe" "resources/unsubscribe"})
(def cacheable-methods #{"server/discover" "tools/list" "prompts/list"
                         "resources/list" "resources/read" "resources/templates/list"})
(def interactive-methods #{"tools/call" "prompts/get" "resources/read"})

(defn modern? [params]
  (contains? (:_meta params) version-key))

(defn error
  ([code message] {:error {:code code :message message}})
  ([code message data] {:error {:code code :message message :data data}}))

(defn validate-request [params]
  (let [v (get-in params [:_meta version-key])]
    (cond
      (not (s/valid? ::specs/modern-request-params params))
      (error -32602 "Missing or invalid per-request MCP metadata")
      (not= version v)
      (error -32022 "Unsupported protocol version"
             {:supported (into [version] specs/supported-protocol-versions)
              :requested v}))))

(defn input-required
  "Return an MRTR result. Read inputResponses/requestState from *request* on retry.
   State is opaque to the SDK; applications must authenticate any security-sensitive
   state and must not perform side effects before requesting additional input."
  ([requests] (input-required requests nil))
  ([requests state]
   (cond-> {:resultType "input_required" :inputRequests requests}
     state (assoc :requestState state))))

(defn- input-error [method params result]
  (when (= "input_required" (:resultType result))
    (let [caps (get-in params [:_meta capabilities-key])
          requests (:inputRequests result)
          needed (->> (vals requests)
                      (map #(case (:method %)
                              "roots/list" :roots
                              "sampling/createMessage" :sampling
                              "elicitation/create" :elicitation
                              ::invalid))
                      set)
          missing (remove #(contains? caps %) needed)]
      (cond
        (or (not (interactive-methods method))
            (not (s/valid? ::specs/input-required-result result))
            (contains? needed ::invalid))
        (error -32603 "Invalid input_required result from handler")
        (seq missing)
        (error -32021 "Missing required client capability"
               {:requiredCapabilities (vec (sort (map name missing)))})))))

(defn finish [context method params result]
  (cond
    (:error result)
    (if (= -32002 (get-in result [:error :code]))
      (assoc-in result [:error :code] -32602)
      result)
    (not (map? result)) (error -32603 "Handler did not return an MCP result")
    :else
    (or (input-error method params result)
        (cond-> (-> result
                    (update :resultType #(or % "complete"))
                    (assoc-in [:_meta server-info-key] (:server-info context)))
          (and (cacheable-methods method)
               (not= "input_required" (:resultType result)))
          (merge {:ttlMs 0 :cacheScope "private"})))))

(defn invoke [method context params handler]
  (if (or (modern? params) (= method "server/discover"))
    (or (validate-request params)
        (when (removed-methods method)
          (error -32601 "Method not available in MCP 2026-07-28"))
        (binding [*request* params]
          (p/then (p/promise (handler (assoc context :request params) params))
                  #(finish context method params %))))
    (handler context params)))

(defmacro defrequest
  "Define a JSON-RPC handler with modern per-request validation and results."
  [multifn method args & body]
  (let [[method-arg context-arg params-arg] args]
    `(defmethod ~multifn ~method [~method-arg context# params#]
       (invoke ~method context# params#
               (fn [~context-arg ~params-arg] ~@body)))))
