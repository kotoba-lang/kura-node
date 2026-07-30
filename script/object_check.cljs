(ns object-check
  "The object layer against real filesystem stores, including the cases where a
  storage system fails silently.

  Not a unit test because `kura.node.object` composes async stores and the
  interesting failures are in the composition: padding on the last stripe, a
  stripe that needs repair, and a digest that does not match. The first two are
  arithmetic anyone would test; the third is the one that matters, because a
  wrong answer that decodes cleanly is the failure mode with no recovery."
  (:require ["node:crypto" :as crypto]
            ["node:fs/promises" :as fsp]
            ["node:os" :as os]
            ["node:path" :as path]
            [erasure.lrc :as lrc]
            [kura.manifest :as manifest]
            [kura.node.async :as async]
            [kura.node.fs :as fs]
            [kura.node.gf :as gf]
            [kura.node.object :as obj]))

(def layout (lrc/layout {:k 16 :r 4 :g 6}))

(defn- digest [b]
  (-> (.createHash crypto "sha256") (.update b) (.digest "hex")))

(defn- store-set
  "Four independent stores, one per pretend domain."
  [root]
  (mapv (fn [i] (fs/open {:node-id (str "d-" i)
                          :root (path/join root (str "d-" i))
                          :failure-domain {:operator "test" :site (str "site-" i)}
                          :independence :independent
                          :availability :always-on}))
        (range 4)))

(defn- fail! [msg] (println "  FAIL" msg) (set! (.-exitCode js/process) 1))
(defn- ok! [msg] (println "  ok  " msg))

(defn- roundtrip> [stores root label size]
  (let [bytes (gf/->bytes (mapv #(mod (* 7 %) 256) (range size)))
        plan (manifest/plan {:object-id (str "obj-" label) :size size
                             :stripe-bytes (* 16 1024)} layout)
        ;; Round-robin the four stores. Real placement is kura.placement's job;
        ;; what this exercises is that the object layer asks for the right
        ;; shard from the right place.
        store-for (fn [_stripe i] (nth stores (mod i (count stores))))]
    (-> (obj/put-object!> {:plan plan :store-for store-for :digest digest :bytes bytes})
        (.then (fn [r]
                 (println (str "  " label ": " size " B → " (:stripes r) " stripe(s), "
                               (:shards-written r) " shards, "
                               (:physical-bytes r) " physical B"))
                 (-> (obj/get-object> {:plan plan :store-for store-for :digest digest
                                       :expect-digest (:digest r)})
                     (.then (fn [g]
                              (if (and (= (gf/->vec bytes) (gf/->vec (:bytes g)))
                                       (= size (gf/blength (:bytes g))))
                                (ok! (str label " round-trips byte for byte"))
                                (fail! (str label " bytes differ")))
                              {:plan plan :digest (:digest r) :store-for store-for}))))))))

(defn- kill> [store-for plan n]
  (js/Promise.all
   (clj->js (map #(-> (async/-delete-shard!> (store-for 0 %)
                                             (manifest/shard-id (:object-id plan) 0 %))
                      (.catch (fn [_] false)))
                 (range n)))))

(defn -main []
  (let [root (path/join (os/tmpdir) (str "kura-obj-" (.now js/Date)))
        stores (store-set root)]
    (-> (roundtrip> stores root "exact" (* 16 1024))
        ;; A size that is not a multiple of stripe-bytes, so the last stripe is
        ;; padded and `:size` has to hide it.
        (.then (fn [_] (roundtrip> stores root "ragged" (+ (* 16 1024) 777))))
        (.then (fn [_] (roundtrip> stores root "multi" (+ (* 3 16 1024) 5))))
        (.then (fn [{:keys [plan digest store-for]}]
                 ;; Repair path: destroy the measured limit and read again.
                 (-> (kill> store-for plan 7)
                     (.then (fn [_] (obj/get-object> {:plan plan :store-for store-for
                                                      :digest object-check/digest
                                                      :expect-digest digest})))
                     (.then (fn [g]
                              (if (= 7 (:repaired g))
                                (ok! "7 shards destroyed, stripe repaired on read")
                                (fail! (str "expected 7 repaired, got " (:repaired g))))
                              {:plan plan :digest digest :store-for store-for})))))
        (.then (fn [{:keys [plan digest store-for]}]
                 ;; Past the distance: must throw, not return partial bytes.
                 (-> (kill> store-for plan 8)
                     (.then (fn [_] (obj/get-object> {:plan plan :store-for store-for
                                                      :digest object-check/digest
                                                      :expect-digest digest})))
                     (.then (fn [_] (fail! "8 destroyed should have thrown")))
                     (.catch (fn [e]
                               (if (re-find #"past the code's distance" (.-message e))
                                 (ok! "8 shards destroyed refuses rather than guessing")
                                 (fail! (str "wrong error: " (.-message e)))))))))
        (.then (fn [_]
                 ;; The check that matters: a wrong digest must be refused even
                 ;; though the stripes decode without complaint.
                 (let [size (* 16 1024)
                       bytes (gf/->bytes (mapv #(mod % 256) (range size)))
                       plan (manifest/plan {:object-id "obj-digest" :size size
                                            :stripe-bytes (* 16 1024)} layout)
                       store-for (fn [_s i] (nth stores (mod i (count stores))))]
                   (-> (obj/put-object!> {:plan plan :store-for store-for
                                          :digest object-check/digest :bytes bytes})
                       (.then (fn [_] (obj/get-object>
                                       {:plan plan :store-for store-for
                                        :digest object-check/digest
                                        :expect-digest (apply str (repeat 64 "0"))})))
                       (.then (fn [_] (fail! "a wrong digest should have thrown")))
                       (.catch (fn [e]
                                 (if (re-find #"digest mismatch" (.-message e))
                                   (ok! "a wrong digest is refused, though the stripes decoded fine")
                                   (fail! (str "wrong error: " (.-message e))))))))))
        (.then (fn [_] (fsp/rm root #js {:recursive true :force true})))
        (.catch (fn [e] (fail! (str "threw: " (.-message e))))))))

(-main)
