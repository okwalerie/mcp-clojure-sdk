(ns io.modelcontext.clojure-sdk.schema-test
  (:require [clojure.test :refer [deftest is]]
            [io.modelcontext.clojure-sdk.schema :as schema]))

(deftest modern-schema-validation
  (is (schema/valid-schema? {:oneOf [{:type "string"} {:type "integer"}]}))
  (is (schema/valid-schema? true))
  (is (not (schema/valid-schema? {:type "invalid"})))
  (is (not (schema/valid-schema? {:$ref "https://example.com/schema"})))
  (is (not (schema/valid-schema? {:$schema "https://example.com/dialect"})))
  (let [validator (schema/compile-schema {:$defs {:value {:type "integer"}},
                                          :$ref "#/$defs/value"})]
    (is (schema/valid? validator 42))
    (is (not (schema/valid? validator "42")))))
