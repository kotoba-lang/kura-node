(ns kura.node.store
  "What a storage node must be able to do, and what it must admit about
  itself.

  The protocol half is unremarkable — put, get, delete, list, size. The half
  that matters is `:failure-domain` and `:independence`, and it is mandatory
  for the same reason `kotobase.storage`'s ref profile is mandatory: **the
  failure mode of guessing is silent.**

  ADR-2607299200 section 1 gets a 1.625x storage multiplier to ten nines by
  assuming shard losses are independent. Phase 0 stands up pseudo-nodes on
  rented backends — B2, R2, S3, Storj — because that is how you measure a
  real node-loss rate before opening to third parties. But **twenty-six
  pseudo-nodes on one B2 account are one node.** The code cannot tell; it sees
  twenty-six shards in twenty-six places and reports seven-erasure tolerance
  that does not exist. What you actually have is B2's durability, and you have
  paid a 1.625x multiplier for the privilege of not getting the code's.

  So a backend declares the domain it shares and how independent it claims to
  be, `independence-profiles` is a closed set, and `kura.node.store/audit`
  reports when a placement's backends collapse into fewer domains than the
  code needs. Declaring is cheap; discovering is a durability event."
  (:require [clojure.string :as str]))

;; --- the protocol ----------------------------------------------------------

(defprotocol IShardStore
  (-put-shard! [store shard-id bytes]
    "Idempotently persist a shard. Returns `{:shard-id :bytes-written}`.")
  (-get-shard [store shard-id]
    "Return the shard's bytes, or nil when absent.")
  (-get-range [store shard-id offset length]
    "Return `length` bytes from `offset` within the shard, or nil when
     absent. Separate from `-get-shard` because the whole point of the
     systematic layout is that a range read fetches a range — a backend that
     can only serve whole objects must say so via `:capabilities`, not
     silently read 4 MiB to return 1 KiB.")
  (-delete-shard! [store shard-id]
    "Remove a shard. Returns true when something was removed.")
  (-list-shards [store prefix]
    "Shard ids under `prefix`, sorted. Used to rebuild the audit tree.")
  (-shard-size [store shard-id]
    "Byte length, or nil when absent. Must not require reading the body."))

(defprotocol INodeIdentity
  (-descriptor [store]
    "`{:node-id :failure-domain :independence :capabilities}` — see below."))

;; --- what a backend must admit --------------------------------------------

(def capabilities
  "Closed set. `:range-read` means `-get-range` fetches only the range from
  the underlying store; without it the caller knows a range read costs a whole
  shard and can decide whether that is acceptable rather than discovering it
  in a bandwidth bill."
  #{:range-read :list :delete :size-without-read :durable-put})

(def independence-profiles
  "How independent this backend's failures are from other backends'.

  - `:independent` — its own hardware, power and operator. Losing another
    node in the placement says nothing about losing this one. This is what
    the durability model in ADR-2607299200 section 1 assumes of every shard.

  - `:shared-provider` — a distinct bucket, region or account inside one
    provider. Correlated by that provider's control plane, billing and
    outages. Usable, but a placement must not count two of these as two
    domains.

  - `:shared-substrate` — same bucket or same disk, distinguished only by
    key prefix. **Exactly one failure domain no matter how many shards land
    on it.** This is what a Phase 0 pseudo-node fleet on one account really
    is, and naming it is the entire reason this field exists.

  The set is closed and the choice mandatory because a backend that declines
  to answer would default to the flattering option, and the flattering option
  is the one that silently voids the durability argument."
  #{:independent :shared-provider :shared-substrate})

(defn descriptor
  "Build and validate a node descriptor."
  [{:keys [node-id failure-domain independence] caps :capabilities
    :or {caps #{}}}]
  (assert (and (string? node-id) (seq node-id)) "node-id is required")
  (assert (contains? independence-profiles independence)
          (str "independence must be one of " independence-profiles))
  (assert (and (map? failure-domain) (seq failure-domain))
          "failure-domain is required, e.g. {:provider \"b2\" :account \"x\"}")
  (assert (every? #(contains? capabilities %) caps)
          (str "unknown capability in " caps))
  {:node-id node-id
   :failure-domain failure-domain
   :independence independence
   :capabilities (set caps)})

(defn store? [x]
  (and (satisfies? IShardStore x) (satisfies? INodeIdentity x)))

;; --- the honest count ------------------------------------------------------

(defn- domain-key
  "The key two backends must share to be one failure domain.

  For `:shared-substrate` that is the whole declared domain — same bucket is
  same bucket. For `:shared-provider` it is the provider alone, since a
  regional outage or a suspended account takes every bucket with it. An
  `:independent` backend is its own domain and is keyed by node id."
  [{:keys [node-id failure-domain independence]}]
  (case independence
    :independent [:independent node-id]
    :shared-provider [:provider (get failure-domain :provider)]
    :shared-substrate [:substrate (into (sorted-map) failure-domain)]))

(defn effective-domains
  "How many genuinely independent failure domains a set of backends provides,
  and which backends collapse together."
  [descriptors]
  (let [grouped (group-by domain-key descriptors)]
    {:count (count grouped)
     :domains (into (sorted-map)
                    (map (fn [[k ds]] [(str/join "/" (map str k))
                                       (mapv :node-id ds)]))
                    grouped)}))

(defn audit
  "Whether a placement's backends can support `tolerated` arbitrary losses.

  A code that survives 7 losses needs the shards spread over enough real
  domains that no single domain holds more than 7. Reports the shortfall
  rather than refusing, because Phase 0 deliberately runs on pseudo-nodes
  that fail this check — the point is that the number is visible in the
  measurement, not that the measurement is forbidden."
  [descriptors tolerated]
  (let [{n-domains :count domains :domains} (effective-domains descriptors)
        worst (if (seq domains) (apply max (map #(count (val %)) domains)) 0)]
    {:backends (count descriptors)
     :effective-domains n-domains
     :largest-domain worst
     :tolerated tolerated
     :survivable? (<= worst tolerated)
     :note (cond
             (zero? (count descriptors)) "no backends"
             (<= worst tolerated) "every domain is inside the code's tolerance"
             :else (str "one domain holds " worst " shards but the code tolerates "
                        tolerated " — durability is that domain's, not the code's"))
     :domains domains}))

;; --- shard ids -------------------------------------------------------------

(defn valid-shard-id?
  "Shard ids come from `kura.manifest/shard-id` as `<object>/<stripe>/<index>`.
  Validated at the edge so a backend never has to guess whether a key it was
  handed is one of ours."
  [s]
  (boolean (and (string? s) (re-matches #"[^/]+/\d+/\d+" s))))

(defn parse-shard-id [s]
  (when-let [[_ obj stripe idx] (re-matches #"([^/]+)/(\d+)/(\d+)" s)]
    {:object-id obj
     :stripe #?(:clj (parse-long stripe) :cljs (js/parseInt stripe 10))
     :index #?(:clj (parse-long idx) :cljs (js/parseInt idx 10))}))
