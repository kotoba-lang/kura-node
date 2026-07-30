(ns kura.node.object
  "Store an object and get it back — the operation, not the arithmetic.

  `kura.manifest` has always had the geometry: how many stripes, how many
  shards, what a shard is called, what the amplification comes to. What did
  **not** exist anywhere was a function that takes bytes and puts them on a
  fleet, or takes an object id and returns the bytes. That logic existed twice,
  both times inside a demonstration script — `kura.conformance.durability` and
  `kura-node/script/multi_domain_durability.cljs` — which is why the network
  could prove durability and could not serve anybody. A capability that only
  exists inside its own demo is a capability nobody else can call.

  **The read verifies.** Erasure decoding with a wrong shard produces wrong
  bytes and no error: the algebra is perfectly happy to solve the system it was
  handed. So `get-object>` takes the digest `put-object!>` returned and refuses
  to hand back anything that does not match it. A storage layer that cannot
  tell you whether it returned what you stored is not finished, and the place
  to catch that is here rather than in whatever the caller does next.

  **Placement is somebody else's decision.** `store-for` maps a shard to the
  store that holds it, so this namespace never chooses — `kura.placement`
  chooses, under caps, and the choice is auditable there. Passing stores in
  also means a test can hand it four in-memory stores and mean it."
  (:require [erasure.lrc :as lrc]
            [erasure.matrix :as matrix]
            [kura.node.async :as async]
            [kura.node.gf :as gf]
            [kura.manifest :as manifest]))

;; --- coding ----------------------------------------------------------------
;; Moved here from two demonstration scripts. The duplication was the tell: the
;; same twenty lines written twice means the library was missing a function.

(defn- data-shards
  "Split one stripe into the k data shards, zero-padded.

  Padding rather than a ragged final shard, for the reason `manifest/plan`
  gives: a short last shard is representable and then every offset calculation
  downstream needs a special case for it. `:size` on the plan is what makes the
  padding invisible to a reader."
  [bytes offset {:keys [shard-bytes layout]}]
  (mapv (fn [i]
          (let [from (+ offset (* i shard-bytes))
                buf (gf/alloc shard-bytes)]
            (dotimes [j shard-bytes]
              (let [src (+ from j)]
                (when (< src (gf/blength bytes))
                  (gf/bset! buf j (gf/bget bytes src)))))
            buf))
        (range (:k layout))))

(defn encode
  "Data shards plus local and global parity — the full n-shard set."
  [data {:keys [shard-bytes layout]}]
  (into (vec data)
        (concat
         (map (fn [q] (gf/xor-shards (map #(nth data %) (lrc/group-members layout q))
                                     shard-bytes))
              (range (:l layout)))
         (map (fn [row] (gf/apply-row row data shard-bytes))
              (matrix/cauchy-rows (:k layout) (:g layout))))))

(defn- run-step [shard-bytes layout shards {:keys [op target targets reads]}]
  (case op
    :local
    (assoc shards target (gf/xor-shards (map #(nth shards %) reads) shard-bytes))

    :recompute-global
    (assoc shards target
           (gf/apply-row (nth (matrix/cauchy-rows (:k layout) (:g layout))
                              (lrc/global-parity-index layout target))
                         (subvec shards 0 (:k layout))
                         shard-bytes))

    :global
    (let [m (mapv #(lrc/generator-row layout %) reads)
          inv (matrix/invert m)
          observed (mapv #(nth shards %) reads)
          recovered (mapv (fn [row] (gf/apply-row row observed shard-bytes)) inv)]
      (reduce (fn [acc j] (assoc acc j (nth recovered j))) shards targets))))

(defn repair
  "Rebuild what is missing from what survived, following `erasure`'s plan.

  Returns nil when the erasure pattern is past the code's distance. **Nil, not
  a best effort** — a partially reconstructed stripe is bytes that look like
  data, and handing those to a caller is worse than failing, because the caller
  has no way to tell."
  [present {:keys [shard-bytes layout]}]
  (let [erased (into #{} (keep-indexed (fn [i s] (when (nil? s) i)) present))
        plan (lrc/recovery-plan layout erased)]
    (when (:recoverable? plan)
      {:plan plan
       :repaired (count erased)
       :shards (reduce (partial run-step shard-bytes layout)
                       (mapv #(or % (gf/alloc shard-bytes)) present)
                       (:steps plan))})))

;; --- the operation ---------------------------------------------------------

(def default-max-inflight
  "How many shard writes are in flight at once, by default.

  Not n. Firing all 32 writes of a stripe simultaneously is what an unloaded
  test tolerates and a real fleet does not: the first ingest of real data failed
  every multi-megabyte object with `fetch failed`, while the same source read
  succeeded on its own. The reads were innocent — 32-way concurrency against
  remote backends was exhausting something (sockets, or a provider's per-client
  limit), and no amount of retrying fixes a load level that is itself the
  problem.

  8 because that is one domain's share of a 32-shard stripe under an even
  spread, so a batch touches every backend once rather than hammering one."
  8)

(defn- put-batch!>
  "Write one batch and wait for every write in it to settle.

  allSettled rather than Promise.all, and the distinction is load-bearing:
  Promise.all rejects on the first failure while its siblings are still in
  flight, so a caller that cleans up on failure deletes what has landed and the
  stragglers land AFTER the cleanup — leaving exactly the orphans the cleanup
  existed to prevent, non-deterministically. Found by a test that asserted the
  cleanup left nothing and kept finding seven shards."
  [{:keys [plan store-for]} stripe indexed]
  (-> (js/Promise.allSettled
       (clj->js (map (fn [[i shard]]
                       (async/-put-shard!> (store-for stripe i)
                                           (manifest/shard-id (:object-id plan) stripe i)
                                           shard))
                     indexed)))
      (.then (fn [rs]
               ;; index -> reason, not just the index set. The count tells the
               ;; caller whether the object is readable; the reason tells whoever
               ;; is debugging WHY a backend refused, and losing it costs hours —
               ;; `fetch failed` with the cause discarded sent this ingest down
               ;; two wrong diagnoses before the third one measured it.
               (into {}
                     (keep-indexed (fn [j r]
                                     (when (= "rejected" (.-status ^js r))
                                       [(first (nth (vec indexed) j))
                                        (.-reason ^js r)])))
                     (vec rs))))))

(defn- put-stripe!>
  "Write all n shards of one stripe in bounded batches, and report which failed.

  Every batch is attempted even after one fails, because the caller's decision
  is about the erasure pattern of what is MISSING and it cannot make that
  decision from a partial picture. Returns the set of shard indices that did not
  land."
  [{:keys [plan max-inflight] :as ctx} stripe bytes]
  (let [offset (* stripe (:stripe-bytes plan))
        all (encode (data-shards bytes offset plan) plan)
        batches (partition-all (or max-inflight default-max-inflight)
                               (map-indexed vector all))]
    (reduce (fn [p batch]
              (.then p (fn [failed]
                         (-> (put-batch!> ctx stripe batch)
                             (.then (fn [more] (merge failed more)))))))
            (js/Promise.resolve {})
            batches)))

(defn delete-object!>
  "Remove every shard of `(:object-id plan)`. Tolerant of absence, because the
  reason to call this is usually that the write did not finish."
  [{:keys [plan store-for]}]
  (js/Promise.all
   (clj->js
    (for [stripe (range (:stripes plan))
          i (range (:n (:layout plan)))]
      (-> (async/-delete-shard!> (store-for stripe i)
                                 (manifest/shard-id (:object-id plan) stripe i))
          (.catch (fn [_] false)))))))

(defn put-object!>
  "Write `bytes` as `(:object-id plan)` across the fleet.

  `store-for` is `(fn [stripe shard-index] store)`. `digest` is
  `(fn [bytes] string)` — required, because the receipt it produces is the only
  thing that makes a later read checkable.

  **A write succeeds when the object is readable, not when every shard lands.**
  Requiring all n makes a write exactly as fragile as the least reliable backend
  in the fleet, which is the opposite of what an erasure code is for: one flaky
  provider then fails objects that the code could have absorbed without noticing.
  Measured, not theorised — B2 closes its keep-alive socket after about sixteen
  PUTs and a fresh connection failed the same way, so with all-or-nothing every
  multi-megabyte object failed while the code could tolerate 13 of 32 losses.

  So the test is the erasure pattern: if `erasure`'s recovery plan can rebuild
  what is missing, the object is stored and **the receipt says which shards are
  absent** so repair can fill them in. If it cannot, the write fails and cleans
  up. `:allow-degraded` must be passed to opt into this — the default fails
  closed, because a caller that did not ask to trade redundancy for availability
  should not silently get that trade."
  [{:keys [plan store-for digest bytes allow-degraded] :as ctx}]
  (assert (fn? store-for) "store-for is required: (fn [stripe index] store)")
  (assert (fn? digest)
          (str "digest is required. Without it a read cannot verify what it "
               "reconstructed, and erasure decoding fails silently — the "
               "algebra solves whatever system it is handed."))
  (-> (reduce (fn [p stripe]
                (.then p (fn [acc]
                           (-> (put-stripe!> ctx stripe bytes)
                               (.then (fn [failures]
                                        (let [missing (set (keys failures))
                                              rec (lrc/recovery-plan (:layout plan) missing)]
                                          (when-not (and (:recoverable? rec)
                                                         (or allow-degraded (empty? missing)))
                                            (let [why (some-> (first (vals failures)) (.-message))]
                                              (throw (doto (js/Error.
                                                            (str (count missing) " of "
                                                                 (:n (:layout plan))
                                                                 " shards failed in stripe " stripe
                                                                 (if (:recoverable? rec)
                                                                   " — recoverable, but :allow-degraded was not set"
                                                                   " — past the code's distance")
                                                                 (when why (str "; first: " why))))
                                                       (aset "cause" (first (vals failures)))))))
                                          (assoc acc stripe failures))))))))
              (js/Promise.resolve {})
              (range (:stripes plan)))
      (.catch (fn [e]
                (-> (delete-object!> ctx)
                    (.catch (fn [_] nil))
                    (.then (fn [_] (throw e))))))
      (.then (fn [missing-by-stripe]
               {:object-id (:object-id plan)
                :missing-shards (into {} (remove (comp empty? val))
                                      (update-vals missing-by-stripe #(set (keys %))))
                ;; WHY each shard is missing, not only which. A degraded write is
                ;; not an error, so nothing throws and the reasons had nowhere to
                ;; go — the third time in one session that a cause was discarded
                ;; and a diagnosis had to start over. A write that quietly
                ;; tolerates a failure owes the operator the failure.
                :missing-reasons (into {}
                                       (comp (remove (comp empty? val))
                                             (map (fn [[st fs]]
                                                    [st (update-vals fs #(some-> % (.-message)))])))
                                       missing-by-stripe)
                :degraded? (boolean (some seq (vals missing-by-stripe)))
                :size (:size plan)
                :stripes (:stripes plan)
                :shards-written (* (:stripes plan) (:n (:layout plan)))
                :physical-bytes (* (:stripes plan) (:stripe-bytes plan)
                                   (/ (:n (:layout plan)) (:k (:layout plan))))
                :digest (digest bytes)}))))

(defn- get-stripe>
  "Fetch one stripe's shards, distinguishing ABSENT from UNREACHABLE.

  This is the same distinction `kura.node.http-node` makes at the store — 404 is
  absence, anything else is a transport fact — and the first version of this
  function threw it away with `(.catch (fn [_] nil))`, complete with a comment
  arguing that at this layer both just mean \"not in hand\". That comment was
  wrong and it cost a real verification run: 56 objects had been written and
  accepted as recoverable, and the read reported one of them as *past the code's
  distance, 18 shards missing, 13 tolerated*. Nothing was missing. Five shards
  were absent and thirteen fetches had failed in transit, and the read had no way
  to say so.

  The consequences of confusing them are not symmetric. Reporting a flaky network
  as data loss sends an operator looking for a disk that died, and tells a repair
  scheduler to rebuild shards that are sitting there intact. So both are erasures
  for the purposes of decoding — the bytes are equally not in hand — and the
  error, if it comes to one, says which is which."
  [{:keys [plan store-for]} stripe]
  (-> (js/Promise.allSettled
       (clj->js (map (fn [i]
                       (async/-get-shard> (store-for stripe i)
                                          (manifest/shard-id (:object-id plan) stripe i)))
                     (range (:n (:layout plan))))))
      (.then (fn [rs]
               (let [results (vec rs)
                     shard-of (fn [i]
                                (let [r (nth results i)]
                                  (when (= "fulfilled" (.-status ^js r))
                                    (let [v (.-value ^js r)]
                                      (when (and v (pos? (gf/blength v))) v)))))
                     present (mapv shard-of (range (:n (:layout plan))))
                     unreachable (into {}
                                       (keep (fn [i]
                                               (let [r (nth results i)]
                                                 (when (= "rejected" (.-status ^js r))
                                                   [i (some-> (.-reason ^js r) (.-message))]))))
                                       (range (:n (:layout plan))))
                     absent (into #{} (remove unreachable)
                                  (keep-indexed (fn [i s] (when (nil? s) i)) present))
                     erased (into (set (keys unreachable)) absent)]
                 (if (empty? erased)
                   {:shards present :repaired 0 :unreachable {}}
                   (if-let [r (repair present plan)]
                     {:shards (:shards r) :repaired (:repaired r) :unreachable unreachable}
                     (throw (js/Error.
                             (str "stripe " stripe " of " (:object-id plan)
                                  " cannot be reconstructed: " (count absent)
                                  " shard(s) absent and " (count unreachable)
                                  " unreachable, " (count erased) " total against "
                                  (lrc/max-tolerated-erasures (:layout plan))
                                  " tolerated"
                                  (when (seq unreachable)
                                    (str ". Unreachable is a transport fact, not data loss — "
                                         "first: " (val (first unreachable))))))))))))))

(defn get-object>
  "Read `(:object-id plan)` back, repairing stripes as needed.

  Verifies against `expect-digest` and throws on mismatch rather than returning
  bytes it cannot vouch for."
  [{:keys [plan digest expect-digest] :as ctx}]
  (assert (fn? digest) "digest is required")
  (assert (and (string? expect-digest) (seq expect-digest))
          (str "expect-digest is required — the digest put-object!> returned. "
               "Reading without it is reading without knowing, and a silent "
               "wrong answer from a storage system is the failure mode with no "
               "recovery."))
  (-> (reduce (fn [p stripe]
                (.then p (fn [acc]
                           (-> (get-stripe> ctx stripe)
                               (.then (fn [r] (-> acc
                                                  (update :parts conj (:shards r))
                                                  (update :repaired + (:repaired r))
                                                  (update :unreachable + (count (:unreachable r))))))))))
              (js/Promise.resolve {:parts [] :repaired 0 :unreachable 0})
              (range (:stripes plan)))
      (.then (fn [{:keys [parts repaired unreachable]}]
               (let [k (:k (:layout plan))
                     bytes (gf/concat-shards (mapcat #(subvec % 0 k) parts)
                                             (:size plan))
                     d (digest bytes)]
                 (when-not (= d expect-digest)
                   (throw (js/Error.
                           (str "digest mismatch for " (:object-id plan)
                                ": stored " expect-digest ", reconstructed " d
                                ". The stripes decoded without error, which is "
                                "exactly why this check exists."))))
                 {:bytes bytes :digest d :repaired repaired
                  ;; Reported so a caller can tell "the code repaired 8 shards
                  ;; because 8 disks lost them" from "the code repaired 8 shards
                  ;; because 8 fetches failed", which are different problems with
                  ;; different fixes.
                  :unreachable unreachable})))))
