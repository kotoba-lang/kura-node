(ns multi-domain-durability
  "Store an object across REAL independent failure domains, destroy shards,
  repair, and check the bytes.

  Every durability claim in this project has so far been proved either by
  algebra (`erasure.codec-test`) or against two rented buckets in one Worker
  (`kura.conformance.durability`). Neither touches a self-hosted node, because
  until `kura.node.http-node` existed nothing could place a shard on one.

  What this adds: the shards of one object spread over a rented bucket in
  us-west, a Mac mini in Fukuoka, a laptop's external SSD in the same room, and
  a Linux box in Saitama ~1000 km away on a different ISP and ASN. Then real
  DELETEs, a repair driven by `erasure`'s plan, and a byte-for-byte comparison.

  It destroys only shards it wrote itself, under its own object id."
  (:require ["node:process" :as process]
            [erasure.lrc :as lrc]
            [erasure.matrix :as matrix]
            [kura.node.async :as async]
            [kura.node.gf :as gf]
            [kura.node.http-fetch :as hf]
            [kura.node.http-node :as hn]
            [kura.node.s3-async :as s3a]
            [kura.node.crypto-noble :as nc]
            [kura.node.store :as store]))

(def layout (lrc/layout {:k 16 :r 4 :g 12}))   ; launch: n=32, 2.0x
(def shard-bytes 512)

(defn- object-id [run] (str "kura-multidomain-" run))
(defn- shard-id [run i] (str (object-id run) "/0/" i))

(defn- source-shards [run]
  (mapv (fn [i] (gf/->bytes (mapv #(mod (+ (* run 97) (* i 131) (* % 29)) 256)
                                  (range shard-bytes))))
        (range (:k layout))))

(defn- encode [data]
  (into (vec data)
        (concat
         (map (fn [q] (gf/xor-shards (map #(nth data %) (lrc/group-members layout q))
                                     shard-bytes))
              (range (:l layout)))
         (map (fn [row] (gf/apply-row row data shard-bytes))
              (matrix/cauchy-rows (:k layout) (:g layout))))))

(defn- run-step [shards {:keys [op target targets reads]}]
  (case op
    :local (assoc shards target (gf/xor-shards (map #(nth shards %) reads) shard-bytes))
    :recompute-global
    (assoc shards target
           (gf/apply-row (nth (matrix/cauchy-rows (:k layout) (:g layout))
                              (lrc/global-parity-index layout target))
                         (subvec shards 0 (:k layout)) shard-bytes))
    :global
    (let [m (mapv #(lrc/generator-row layout %) reads)
          inv (matrix/invert m)
          observed (mapv #(nth shards %) reads)
          rec (mapv (fn [row] (gf/apply-row row observed shard-bytes)) inv)]
      (reduce (fn [acc j] (assoc acc j (nth rec j))) shards targets))))

(defn- repair [present]
  (let [erased (into #{} (keep-indexed (fn [i s] (when (nil? s) i)) present))
        plan (lrc/recovery-plan layout erased)]
    (when (:recoverable? plan)
      {:plan plan
       :shards (reduce run-step (mapv #(or % (gf/alloc shard-bytes)) present)
                       (:steps plan))})))

;; --- the fleet -------------------------------------------------------------

(defn- backends> []
  (let [http (hf/fetch-http)
        b2 (s3a/open {:node-id "b2" :http http :crypto (nc/noble-crypto)
                      :key-id (.-B2_KEY_ID js/process.env)
                      :secret (.-B2_APP_KEY js/process.env)
                      :endpoint "https://s3.us-west-004.backblazeb2.com"
                      :host "s3.us-west-004.backblazeb2.com"
                      :bucket "kura-phase0-b2" :region "us-west-004"
                      :now-fn hf/now-iso
                      :prefix "kura" :independence :shared-provider
                      :availability :always-on
                      :failure-domain {:provider "backblaze-b2"}})]
    (-> (js/Promise.all
         (clj->js (map (fn [[nm base]]
                         (-> (hn/descriptor> http base)
                             (.then (fn [d] [nm (hn/open {:http http :base base :descriptor d})]))))
                       [["judah" "http://100.113.200.45:8410"]
                        ["air-ssd" "http://127.0.0.1:8411"]
                        ["gad" "http://100.82.98.110:8410"]])))
        ;; NOT js->clj: Promise.all resolves to a JS array whose elements are
        ;; already CLJS vectors holding CLJS records, and js->clj would walk
        ;; into the records and flatten them into seqs.
        (.then (fn [pairs] (into {"b2" b2} (vec pairs)))))))

(defn- assign
  "Shard index -> backend, respecting the per-domain cap. Domains, not nodes:
  judah and air-ssd share a site, so together they get one domain's budget."
  [stores]
  (let [tol (lrc/max-tolerated-erasures layout)
        ;; 32 shards, 3 domains, cap 13 -> 11/11/10.
        plan (concat (repeat 11 ["b2" (get stores "b2")])
                     ;; fleet-site-1's 11 split across the two boxes in the room
                     (map (fn [i] (if (even? i) ["judah" (get stores "judah")]
                                      ["air-ssd" (get stores "air-ssd")]))
                          (range 11))
                     (repeat 10 ["gad" (get stores "gad")]))]
    (assert (= (:n layout) (count plan)))
    (assert (<= 11 tol) (str "cap 11 must be within tolerance " tol))
    (vec plan)))

(defn- put-all> [assigned run shards]
  (js/Promise.all
   (clj->js (map-indexed (fn [i s] (async/-put-shard!> (second (nth assigned i)) (shard-id run i) s))
                         shards))))

(defn- read-all> [assigned run]
  (js/Promise.all
   (clj->js (map (fn [i] (-> (async/-get-shard> (second (nth assigned i)) (shard-id run i))
                             (.catch (fn [_] nil))))
                 (range (:n layout))))))

(defn- del> [assigned run idxs]
  (js/Promise.all
   (clj->js (map #(-> (async/-delete-shard!> (second (nth assigned %)) (shard-id run %))
                      (.catch (fn [_] false)))
                 idxs))))

(defn- scenario> [assigned run nm kill expect]
  (let [data (source-shards run)
        expect-vecs (mapv gf/->vec data)
        all (encode data)]
    (-> (put-all> assigned run all)
        (.then (fn [_] (del> assigned run kill)))
        (.then (fn [_] (read-all> assigned run)))
        (.then (fn [got]
                 (let [present (mapv #(when (and % (pos? (gf/blength %))) %) (js->clj got))
                       missing (into (sorted-set) (keep-indexed (fn [i s] (when (nil? s) i)) present))
                       r (repair present)
                       recovered (boolean (and r (= expect-vecs
                                                    (mapv gf/->vec (subvec (:shards r) 0 (:k layout))))))]
                   (-> (del> assigned run (range (:n layout)))
                       (.then (fn [_]
                                (println (str "  " nm))
                                (println (str "    destroyed " (count kill)
                                              " · observed missing " (count missing)
                                              " · recoverable " (boolean r)
                                              " · bytes match " recovered
                                              (if (= expect (boolean r)) "  OK" "  UNEXPECTED")))
                                (when-not (= expect (boolean r)) (set! (.-exitCode js/process) 1))
                                (when (and expect (not recovered)) (set! (.-exitCode js/process) 1))
                                nil)))))))))

(defn -main []
  (-> (backends> )
      (.then (fn [stores]
               (let [assigned (assign stores)
                     descs (mapv #(store/-descriptor (second %)) assigned)
                     a (store/audit descs (lrc/max-tolerated-erasures layout))]
                 (println "layout   :" (select-keys layout [:k :r :g :l :n])
                          "multiplier" (/ (:n layout) (:k layout)))
                 (println "domains  :" (:effective-domains a)
                          "· awake" (:always-on-domains a)
                          "· largest" (:largest-domain a) "/" (:tolerated a)
                          "· survivable" (:survivable? a))
                 (doseq [[d ns] (:domains a)] (println "   " d "->" (count ns) "shards"))
                 (println)
                 (-> (scenario> assigned 1 "1 shard lost — the 99% case" [5] true)
                     (.then #(scenario> assigned 2 "a local group plus its parity" [0 1 2 3 16] true))
                     (.then #(scenario> assigned 3 "7 arbitrary — measured limit of the n=26 code" (range 7) true))
                     (.then #(scenario> assigned 4 "11 arbitrary — one whole domain's worth" (range 11) true))
                     (.then #(scenario> assigned 5 "14 — past the bound, must refuse" (range 14) false))))))
      (.catch (fn [e] (println "ERROR" (.-message e)) (set! (.-exitCode js/process) 1)))))

(-main)
