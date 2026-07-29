(ns kura.node.store-test
  (:require [clojure.test :refer [deftest is testing]]
            [kura.node.contract :as contract]
            [kura.node.memory :as memory]
            [kura.node.store :as store]))

(deftest memory-backend-passes-the-contract
  (let [s (memory/open "mem-contract")]
    (contract/verify s (fn [ok? label] (is ok? label)))))

(deftest independence-must-be-declared
  (testing "no default, because the flattering answer is the one that voids
            the durability argument"
    (is (thrown? #?(:clj Throwable :cljs js/Error)
                 (store/descriptor {:node-id "n" :failure-domain {:provider "b2"}})))
    (is (thrown? #?(:clj Throwable :cljs js/Error)
                 (store/descriptor {:node-id "n" :independence :probably-fine
                                    :failure-domain {:provider "b2"}})))
    (is (thrown? #?(:clj Throwable :cljs js/Error)
                 (store/descriptor {:node-id "n" :independence :independent}))
        "a failure domain is required too")))

(deftest shard-ids-are-validated-at-the-edge
  (is (store/valid-shard-id? "obj-1/0/3"))
  (is (store/valid-shard-id? "a/12/25"))
  (is (not (store/valid-shard-id? "obj-1/0")))
  (is (not (store/valid-shard-id? "obj-1/x/3")))
  (is (not (store/valid-shard-id? "a/b/c/d")))
  (is (= {:object-id "obj-1" :stripe 0 :index 3} (store/parse-shard-id "obj-1/0/3")))
  (is (nil? (store/parse-shard-id "nope"))))

;; --- the property this namespace exists for --------------------------------

(defn- descs [independence n domain-fn]
  (mapv #(store/descriptor {:node-id (str "n-" %)
                            :independence independence
                            :failure-domain (domain-fn %)})
        (range n)))

(deftest twenty-six-pseudo-nodes-on-one-account-are-one-node
  (testing "the Phase 0 hazard, stated as a number rather than assumed away"
    (let [fleet (descs :shared-substrate 26 (fn [_] {:provider "b2" :bucket "kura-0"}))
          a (store/audit fleet 7)]
      (is (= 26 (:backends a)))
      (is (= 1 (:effective-domains a))
          "one bucket is one domain no matter how many key prefixes")
      (is (= 26 (:largest-domain a)))
      (is (false? (:survivable? a)))
      (is (re-find #"durability is that domain" (:note a))))))

(deftest distinct-buckets-in-one-provider-are-still-one-domain
  (testing "a regional outage or a suspended account takes every bucket with it"
    (let [fleet (descs :shared-provider 26 (fn [i] {:provider "b2" :bucket (str "b-" i)}))
          a (store/audit fleet 7)]
      (is (= 1 (:effective-domains a)))
      (is (false? (:survivable? a))))))

(deftest spreading-across-providers-buys-real-domains
  (let [providers ["b2" "r2" "s3" "storj"]
        fleet (mapv #(store/descriptor
                      {:node-id (str "n-" %)
                       :independence :shared-provider
                       :failure-domain {:provider (nth providers (mod % 4))}})
                    (range 26))
        a (store/audit fleet 7)]
    (is (= 4 (:effective-domains a)))
    (is (= 7 (:largest-domain a)) "26 over 4 providers is at most 7 each")
    (is (true? (:survivable? a))
        "exactly at the code's tolerance — survivable, with zero margin")))

(deftest genuinely-independent-nodes-each-count
  (let [fleet (descs :independent 26 (fn [i] {:operator (str "op-" i)}))
        a (store/audit fleet 7)]
    (is (= 26 (:effective-domains a)))
    (is (= 1 (:largest-domain a)))
    (is (true? (:survivable? a)))))

(deftest independence-is-read-from-the-declaration-not-the-node-id
  (testing "eleven shard slots on ONE machine are one failure domain. Keying
            :independent on node-id made them eleven, which meant an audit
            could be satisfied by renaming — found by this repo's own
            local-node demo reporting 13 domains for a three-machine fleet."
    (let [one-box (mapv #(store/descriptor {:node-id (str "fs-" %)
                                            :independence :independent
                                            :failure-domain {:operator "alice" :site "home"}})
                        (range 11))
          a (store/audit one-box 13)]
      (is (= 1 (:effective-domains a)) "one machine, one domain")
      (is (= 11 (:largest-domain a))))
    (testing "and two machines in different places are two"
      (let [two (into (mapv #(store/descriptor {:node-id (str "a-" %)
                                                :independence :independent
                                                :failure-domain {:operator "alice" :site "home"}})
                            (range 5))
                      (mapv #(store/descriptor {:node-id (str "b-" %)
                                                :independence :independent
                                                :failure-domain {:operator "bob" :site "office"}})
                            (range 5)))]
        (is (= 2 (:effective-domains (store/audit two 13))))))))

(deftest audit-of-nothing-is-reported-not-crashed
  (let [a (store/audit [] 7)]
    (is (= 0 (:backends a)))
    (is (= 0 (:largest-domain a)))
    (is (= "no backends" (:note a)))))
