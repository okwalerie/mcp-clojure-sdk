(ns io.modelcontext.clojure-sdk.modern-http-test
  (:require [clj-http.client :as http]
            [clojure.test :refer [deftest is]]
            [io.modelcontext.clojure-sdk.http-server :as server]
            [io.modelcontext.clojure-sdk.http-headers :as headers]
            [io.modelcontext.clojure-sdk.protocol :as protocol]
            [io.modelcontext.clojure-sdk.io-chan :as wire]))

(def params
  {:_meta {protocol/version-key protocol/version,
           protocol/capabilities-key {}}})
(def spec
  {:name "modern",
   :version "1",
   :capabilities {:tools {}},
   :tools [{:name "echo",
            :inputSchema {:type "object"},
            :handler (fn [args] [{:type "text", :text (:message args)}])}]})

(deftest header-values
  (doseq [v ["name" " padded " "hello世界" "a\nb" "=?base64?literal?=" ""]]
    (is (= v (headers/decode-value (headers/encode-value v)))))
  (is (nil? (headers/decode-value "=?base64?not base64?="))))

(deftest modern-http-independent-requests
  (let [handle (server/start! spec {:port 0})
        url (str "http://127.0.0.1:" (:port handle) "/mcp")
        message {:jsonrpc "2.0", :id 1, :method "tools/list", :params params}
        post (fn [m extra]
               (http/post url
                          {:headers (merge
                                      {"Content-Type" "application/json",
                                       "Accept"
                                       "application/json, text/event-stream"}
                                      (headers/headers m nil)
                                      extra),
                           :body (wire/message->json-str m),
                           :throw-exceptions false}))]
    (try (let [r (post message {})
               body (wire/json-str->message (:body r))]
           (is (= 200 (:status r)))
           (is (= "complete" (get-in body [:result :resultType])))
           (is (nil? (get-in r [:headers "mcp-session-id"])))
           (is (empty? @(:sessions handle))))
         (let [r (post message {"Mcp-Method" "tools/call"})]
           (is (= 400 (:status r)))
           (is (= -32020
                  (get-in (wire/json-str->message (:body r)) [:error :code]))))
         (is (= 403 (:status (post message {"Origin" "https://evil.example"}))))
         (is (= 405 (:status (http/get url {:throw-exceptions false}))))
         (let [r (post (assoc message :method "not/a/method") {})]
           (is (= 404 (:status r))))
         (is (= 200 (:status (post message {}))))
         (finally (server/stop! handle)))))

(deftest modern-sdk-client-roundtrip
  (let [handle (server/start! spec {:port 0})
        url (str "http://127.0.0.1:" (:port handle) "/mcp")
        run! (requiring-resolve 'io.modelcontext.clojure-sdk.http-client/run!)
        stop! (requiring-resolve
                'io.modelcontext.clojure-sdk.http-client/shutdown!)
        call! (requiring-resolve 'io.modelcontext.clojure-sdk.client/call-tool!)
        await! (requiring-resolve
                 'io.modelcontext.clojure-sdk.client/deref-or-cancel)]
    (try (let [{:keys [client error]}
                 (run! {:name "modern-client", :version "1"}
                       url
                       {:protocol-mode :modern, :timeout-ms 3000})]
           (is (nil? error))
           (when client
             (try (is (= protocol/version (:protocol-version @(:state client))))
                  (is (= "hello"
                         (get-in (await!
                                   (call! client "echo" {:message "hello"})
                                   3000
                                   ::timeout)
                                 [:content 0 :text])))
                  (is (empty? @(:sessions handle)))
                  (finally (stop! client)))))
         (finally (server/stop! handle)))))

(deftest mirrored-tool-parameters
  (let [tool {:inputSchema {:type "object",
                            :properties {"nested" {:type "object",
                                                   :properties {"region"
                                                                {:type "string",
                                                                 :x-mcp-header
                                                                 "Region"}}}}}}
        message {:jsonrpc "2.0",
                 :id 1,
                 :method "tools/call",
                 :params (assoc params
                           :name "echo"
                           :arguments {:nested {:region "世界"}})}
        h (headers/headers message tool)]
    (is (= "世界" (headers/decode-value (get h "Mcp-Param-Region"))))
    (is (nil? (headers/validate h message tool)))
    (is (= -32020
           (get-in (headers/validate (dissoc h "Mcp-Param-Region") message tool)
                   [:error :code]))))
  (is (thrown? Exception
               (headers/annotations
                 {:properties {:x {:type "string", :x-mcp-header "Region"},
                               :y {:type "string", :x-mcp-header "region"}}}))))

(deftest modern-client-multi-round-trip
  (let [rounds (atom 0)
        handle (server/start!
                 {:name "mrtr",
                  :version "1",
                  :capabilities {:tools {}},
                  :tools [{:name "ask",
                           :inputSchema {:type "object"},
                           :handler
                           (fn [_]
                             (swap! rounds inc)
                             (if (seq (:inputResponses protocol/*request*))
                               [{:type "text",
                                 :text (:requestState protocol/*request*)}]
                               (protocol/input-required
                                 {"input" {:method "elicitation/create",
                                           :params {:message "Name?"}}}
                                 "state")))}]}
                 {:port 0})
        run! (requiring-resolve 'io.modelcontext.clojure-sdk.http-client/run!)
        stop! (requiring-resolve
                'io.modelcontext.clojure-sdk.http-client/shutdown!)
        call! (requiring-resolve 'io.modelcontext.clojure-sdk.client/call-tool!)
        await! (requiring-resolve
                 'io.modelcontext.clojure-sdk.client/deref-or-cancel)]
    (try (let [{:keys [client error]}
                 (run! {:name "client", :version "1"}
                       (str "http://127.0.0.1:" (:port handle) "/mcp")
                       {:protocol-mode :modern,
                        :timeout-ms 3000,
                        :elicitation-handler
                        (fn [_] {:action "accept", :content {:name "A"}})})]
           (is (nil? error))
           (when client
             (try (is (= "state"
                         (get-in (await! (call! client "ask" {}) 3000 ::timeout)
                                 [:content 0 :text])))
                  (is (= 2 @rounds))
                  (finally (stop! client)))))
         (finally (server/stop! handle)))))
