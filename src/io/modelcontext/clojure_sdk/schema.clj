(ns io.modelcontext.clojure-sdk.schema
  "Bounded JSON Schema 2020-12 validation. External references are never fetched."
  (:require [clojure.string :as str]
            [io.modelcontext.clojure-sdk.io-chan :as wire])
  (:import [com.fasterxml.jackson.databind ObjectMapper]
           [com.networknt.schema JsonSchema JsonSchemaFactory
            SpecVersion$VersionFlag]
           [java.net URI]))

(def ^:private mapper (ObjectMapper.))
(def ^:private factory
  (JsonSchemaFactory/getInstance SpecVersion$VersionFlag/V202012))

(defn- bounded!
  [schema]
  (let [nodes (atom 0)]
    (letfn
      [(walk [value depth]
         (when (or (> depth 48) (> (swap! nodes inc) 4096))
           (throw (ex-info "JSON Schema complexity limit exceeded" {})))
         (cond
           (map? value)
             (doseq [[k v] value]
               (when (and (#{"$ref" "$dynamicRef" "$recursiveRef"} (name k))
                          (not (and (string? v) (str/starts-with? v "#"))))
                 (throw (ex-info "External JSON Schema references are disabled"
                                 {:reference v})))
               (when (and (= "$schema" (name k))
                          (not
                            (#{"https://json-schema.org/draft/2020-12/schema"
                               "https://json-schema.org/draft/2020-12/schema#"}
                             v)))
                 (throw (ex-info "Unsupported JSON Schema dialect; use 2020-12"
                                 {:dialect v})))
               (walk v (inc depth)))
           (sequential? value) (doseq [v value] (walk v (inc depth)))))]
      (walk schema 0))))

(def ^:private meta-schema
  (delay (.getSchema factory
                     (URI. "https://json-schema.org/draft/2020-12/schema"))))

(defn valid-schema?
  [schema]
  (try (bounded! schema)
       (empty? (.validate ^JsonSchema @meta-schema
                          (.readTree mapper
                                     ^String (wire/message->json-str schema))))
       (catch Exception _ false)))

(defn compile-schema
  [schema]
  (when-not (valid-schema? schema)
    (throw (ex-info "Invalid or unsupported JSON Schema" {})))
  (.getSchema factory
              (.readTree mapper ^String (wire/message->json-str schema))))

(defn valid?
  [^JsonSchema schema value]
  (empty? (.validate schema
                     (.readTree mapper
                                ^String (wire/message->json-str value)))))
