(ns io.modelcontext.clojure-sdk.http-headers
  "MCP 2026-07-28 mirrored HTTP headers. Header values never authorize a call."
  (:require [clojure.string :as str]
            [io.modelcontext.clojure-sdk.protocol :as protocol])
  (:import [java.nio.charset StandardCharsets]
           [java.util Base64]))

(defn encode-value
  [value]
  (let [s (str value)]
    (if (and (re-matches #"[\x20-\x7e\t]*" s)
             (= s (str/trim s))
             (not (and (str/starts-with? s "=?base64?")
                       (str/ends-with? s "?="))))
      s
      (str "=?base64?"
           (.encodeToString (Base64/getEncoder)
                            (.getBytes s StandardCharsets/UTF_8))
           "?="))))

(defn decode-value
  [value]
  (when (string? value)
    (if (and (str/starts-with? value "=?base64?") (str/ends-with? value "?="))
      (try (String. (.decode (Base64/getDecoder)
                             (subs value 9 (- (count value) 2)))
                    StandardCharsets/UTF_8)
           (catch IllegalArgumentException _ nil))
      (when (and (re-matches #"[\x20-\x7e\t]*" value)
                 (= value (str/trim value)))
        value))))

(defn- field [m k] (if (contains? m k) (get m k) (get m (name k))))

(defn annotations
  "Return [argument-path header-name primitive-type] entries; reject ambiguous schemas."
  [schema]
  (let [entries (atom [])
        names (atom #{})]
    (letfn
      [(walk [node path reachable?]
         (when (map? node)
           (when-let [header (field node :x-mcp-header)]
             (let [type (field node :type)]
               (when-not (and reachable?
                              (string? header)
                              (re-matches #"[!#$%&'*+.^_`|~0-9a-zA-Z-]+" header)
                              (#{"string" "integer" "boolean"} type)
                              (not (contains? @names (str/lower-case header))))
                 (throw (ex-info "Invalid x-mcp-header annotation"
                                 {:header header})))
               (swap! names conj (str/lower-case header))
               (swap! entries conj [path (str "Mcp-Param-" header) type])))
           (doseq [[k v] node]
             (if (= "properties" (name k))
               (doseq [[property child] v]
                 (walk child (conj path property) reachable?))
               (cond (map? v) (walk v path false)
                     (sequential? v) (doseq [child v]
                                       (walk child path false)))))))]
      (walk schema [] true) @entries)))

(defn- argument
  [arguments path]
  (reduce (fn [m k] (field m (keyword (name k)))) arguments path))

(defn headers
  [message tool]
  (let [method (:method message)
        params (:params message)
        name-value (case method
                     "resources/read" (:uri params)
                     ("tools/call" "prompts/get") (:name params)
                     nil)]
    (merge
      {"MCP-Protocol-Version" (get-in params [:_meta protocol/version-key]),
       "Mcp-Method" method}
      (when name-value {"Mcp-Name" (encode-value name-value)})
      (when (and (= "tools/call" method) tool)
        (into {}
              (keep (fn [[path header type]]
                      (when-some [value (argument (:arguments params) path)]
                        (when-not (case type
                                    "string" (string? value)
                                    "boolean" (boolean? value)
                                    "integer" (and (integer? value)
                                                   (<= -9007199254740991
                                                       value
                                                       9007199254740991)))
                          (throw (ex-info "Invalid mirrored parameter"
                                          {:path path})))
                        [header (encode-value value)]))
                    (annotations (:inputSchema tool))))))))

(defn validate
  [request-headers message tool]
  (try (let [actual (into {}
                          (map (fn [[k v]] [(str/lower-case (name k)) v])
                            request-headers))
             expected (headers message tool)]
         (when (some (fn [[k v]]
                       (let [received (get actual (str/lower-case k))]
                         (if (or (= k "Mcp-Name")
                                 (str/starts-with? k "Mcp-Param-"))
                           (not= (decode-value v) (decode-value received))
                           (or (nil? v) (not= v received)))))
                     expected)
           (protocol/error
             -32020
             "Missing, invalid, or mismatched MCP request header")))
       (catch Exception _
         (protocol/error -32020 "Invalid mirrored tool parameter"))))
