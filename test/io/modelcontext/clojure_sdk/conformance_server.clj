(ns io.modelcontext.clojure-sdk.conformance-server
  (:require [io.modelcontext.clojure-sdk.http-server :as http]
            [io.modelcontext.clojure-sdk.protocol :as protocol]))

(defn -main
  [& [port]]
  (let [handle
          (http/start!
            {:name "clojure-sdk-conformance",
             :version "2026-07-28",
             :capabilities
             {:tools {}, :resources {}, :prompts {}, :completions {}},
             :tools
             [{:name "test_streaming_elicitation",
               :inputSchema {:type "object"},
               :handler (fn [_]
                          (protocol/input-required
                            {"elicit" {:method "elicitation/create",
                                       :params {:mode "form",
                                                :message "Name?",
                                                :requestedSchema
                                                {:type "object"}}}}))}
              {:name "test_missing_capability",
               :inputSchema {:type "object"},
               :handler (fn [_]
                          (protocol/input-required
                            {"sample" {:method "sampling/createMessage",
                                       :params {:messages [], :maxTokens 1}}}))}
              {:name "test_logging_tool",
               :inputSchema {:type "object"},
               :handler (fn [_] [{:type "text", :text "Done"}])}
              {:name "test_simple_text",
               :inputSchema {:type "object"},
               :handler (fn [_]
                          [{:type "text",
                            :text
                            "This is a simple text response for testing."}])}],
             :resources [{:uri "test://static-text",
                          :name "Text resource",
                          :handler (fn [uri]
                                     {:uri uri,
                                      :mimeType "text/plain",
                                      :text "Test resource content"})}],
             :prompts [{:name "test_simple_prompt",
                        :handler (fn [_]
                                   {:messages [{:role "user",
                                                :content {:type "text",
                                                          :text
                                                          "Test prompt"}}]})}]}
            {:port (Integer/parseInt (or port "7989")),
             :allowed-origins #{(str "http://localhost:" (or port "7989"))}})]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable #(http/stop! handle)))
    (println "Conformance server" (:port handle))
    @(promise)))
