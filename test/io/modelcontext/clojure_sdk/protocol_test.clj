(ns io.modelcontext.clojure-sdk.protocol-test
  (:require [clojure.test :refer [deftest is testing]]
            [io.modelcontext.clojure-sdk.protocol :as protocol]
            [io.modelcontext.clojure-sdk.server :as server]
            [io.modelcontext.clojure-sdk.io-chan :as wire]
            [jsonrpc4clj.server :as rpc]))

(def params
  {:_meta {protocol/version-key protocol/version,
           protocol/capabilities-key {}}})
(defn result!
  [method context p]
  (let [r (rpc/receive-request method context p)] (if (map? r) r @r)))

(deftest independent-modern-requests
  (let [context (server/create-context!
                  {:name "test",
                   :version "1",
                   :capabilities {:tools {}},
                   :tools [{:name "echo",
                            :inputSchema {:type "object"},
                            :handler (fn [args] [{:type "text",
                                                  :text (:text args)}])}]})]
    (testing "no initialization required"
      (is (= "complete" (:resultType (result! "tools/list" context params))))
      (is (= [protocol/version "2025-06-18" "2025-03-26" "2024-11-05"]
             (:supportedVersions (result! "server/discover" context params))))
      (is (= {:name "test", :version "1"}
             (get-in (result! "tools/list" context params)
                     [:_meta protocol/server-info-key])))
      (is (= "private" (:cacheScope (result! "tools/list" context params)))))
    (testing "metadata is validated on each request"
      (is (= -32602
             (get-in (result! "server/discover" context {}) [:error :code])))
      (is (= -32022
             (get-in (result! "tools/list"
                              context
                              (assoc-in params
                                [:_meta protocol/version-key]
                                "2099-01-01"))
                     [:error :code])))
      (is (= -32601 (get-in (result! "ping" context params) [:error :code])))
      (is (= "complete" (:resultType (result! "tools/list" context params)))))
    (testing "legacy handshake still selects a legacy revision"
      (is (= "2025-06-18"
             (:protocolVersion (result! "initialize"
                                        context
                                        {:protocolVersion "2026-07-28",
                                         :capabilities {},
                                         :clientInfo {:name "old",
                                                      :version "1"}})))))))

(deftest metadata-wire-roundtrip
  (is (= params (wire/json-str->message (wire/message->json-str params)))))

(deftest mrtr-capability-is-request-scoped
  (let [ctx {:server-info {:name "test", :version "1"}}
        response (protocol/input-required {"name" {:method "elicitation/create",
                                                   :params {}}})]
    (is (= -32021
           (get-in (protocol/finish ctx "tools/call" params response)
                   [:error :code])))
    (is (= "input_required"
           (:resultType (protocol/finish ctx
                                         "tools/call"
                                         (assoc-in params
                                           [:_meta protocol/capabilities-key]
                                           {:elicitation {}})
                                         response))))
    (is (= -32021
           (get-in (protocol/finish ctx "tools/call" params response)
                   [:error :code])))))

(deftest application-keys-are-not-renamed
  (let [m {:_meta {protocol/version-key protocol/version},
           :arguments {:snake_case 1, :kebab-case 2, :MixedCase 3}}]
    (is (= m (wire/json-str->message (wire/message->json-str m))))))

(deftest mrtr-survives-handler-validation
  (let [context (server/create-context!
                  {:name "test",
                   :version "1",
                   :capabilities {:tools {}},
                   :tools [{:name "ask",
                            :inputSchema {:type "object"},
                            :handler (fn [_]
                                       (protocol/input-required
                                         {"answer" {:method
                                                    "elicitation/create",
                                                    :params {}}}))}]})
        request (assoc params
                  :name "ask"
                  :arguments {})]
    (is (= -32021
           (get-in (result! "tools/call" context request) [:error :code])))
    (is (= "input_required"
           (:resultType (result! "tools/call"
                                 context
                                 (assoc-in request
                                   [:_meta protocol/capabilities-key]
                                   {:elicitation {}})))))
    (is (= -32602
           (get-in (result! "tools/list"
                            context
                            {:_meta {protocol/capabilities-key {}}})
                   [:error :code])))))

(deftest reject-unknown-tool-cursor
  (let [context (server/create-context! {:name "test", :version "1"})]
    (is (= -32602
           (get-in
             (result! "tools/list" context (assoc params :cursor "unknown"))
             [:error :code])))
    (is (= -32602
           (get-in (result! "tools/list" context {:cursor "unknown"})
                   [:error :code])))))

(deftest modern-errors-retain-defined-codes
  (let [context (server/create-context! {:name "test", :version "1"})]
    (doseq [[method extra code] [["tools/call" {:name "missing"} -32602]
                                 ["resources/read" {:uri "test://missing"}
                                  -32602]
                                 ["prompts/get" {:name "missing"} -32602]]]
      (is (= code
             (get-in (result! method context (merge params extra))
                     [:error :code]))))
    (is (= -32602
           (get-in (protocol/finish {}
                                    "resources/read"
                                    params
                                    (protocol/error -32002 "Missing"))
                   [:error :code])))))
