(ns io.modelcontext.clojure-sdk.modern-stdio-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [io.modelcontext.clojure-sdk.protocol :as protocol]
            [io.modelcontext.clojure-sdk.server :as server]
            [io.modelcontext.clojure-sdk.transport :as transport]
            [jsonrpc4clj.server :as rpc]))

(def meta-fields
  {protocol/version-key protocol/version, protocol/capabilities-key {}})
(defn receive!
  [out]
  (let [[value port] (async/alts!! [out (async/timeout 2000)])]
    (if (= port out) value ::timeout)))

(deftest subscriptions-and-cancellation
  (let [in (async/chan 16)
        out (async/chan 16)
        endpoint (transport/chan-server {:input-ch in, :output-ch out})
        context (server/create-context! {:name "test", :version "1"})]
    (server/start! endpoint context)
    (try (async/>!! in
                    {:jsonrpc "2.0",
                     :id 1,
                     :method "subscriptions/listen",
                     :params {:_meta meta-fields,
                              :notifications {:toolsListChanged true}}})
         (let [ack (receive! out)]
           (is (= "notifications/subscriptions/acknowledged" (:method ack)))
           (is (= 1 (get-in ack [:params :_meta protocol/subscription-key]))))
         (server/register-tool! context
                                {:name "new", :inputSchema {:type "object"}}
                                identity)
         (is (= "notifications/tools/list_changed" (:method (receive! out))))
         (async/>!! in
                    {:jsonrpc "2.0",
                     :method "notifications/cancelled",
                     :params {:requestId 1}})
         (async/>!! in
                    {:jsonrpc "2.0",
                     :id 2,
                     :method "tools/list",
                     :params {:_meta meta-fields}})
         (is (= 2 (:id (receive! out))))
         (is (empty? @(:listeners context)))
         (finally (async/close! in) (rpc/shutdown endpoint)))))

(deftest parse-errors-and-notifications
  (let [in (async/chan 16)
        out (async/chan 16)
        endpoint (transport/chan-server {:input-ch in, :output-ch out})
        context (server/create-context! {:name "test", :version "1"})]
    (server/start! endpoint context)
    (try (async/>!! in :parse-error)
         (is (= -32700 (get-in (receive! out) [:error :code])))
         (async/>!! in {:jsonrpc "2.0", :method "unknown/notification"})
         (async/>!! in
                    {:jsonrpc "2.0",
                     :id 1,
                     :method "server/discover",
                     :params {:_meta meta-fields}})
         (is (= 1 (:id (receive! out))))
         (finally (async/close! in) (rpc/shutdown endpoint)))))

(deftest full-json-rpc-integer-ids
  (let [in (async/chan 16)
        out (async/chan 16)
        endpoint (transport/chan-server {:input-ch in, :output-ch out})
        context (server/create-context! {:name "test", :version "1"})]
    (server/start! endpoint context)
    (try (doseq [id [-1 (biginteger 1)
                     (biginteger "99999999999999999999999999999999")]]
           (async/>!! in
                      {:jsonrpc "2.0",
                       :id id,
                       :method "server/discover",
                       :params {:_meta meta-fields}})
           (is (= id (:id (receive! out)))))
         (finally (async/close! in) (rpc/shutdown endpoint)))))
