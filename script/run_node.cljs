(ns run-node
  "A runnable kura storage node.

      nbb --classpath \"src:script:...\" script/run_node.cljs \\
        --root ~/kura-data --port 8080 --node-id my-node \\
        --operator alice --site home --availability intermittent

  `kura.node.fs` made a self-hosted node *possible*; this makes it a command.
  The distance between those two is the whole reason the fleet is still short a
  failure domain — a backend nobody can start is not a node.

  **What it serves.** The async shard-store contract over HTTP, plus the
  descriptor and a self-check. A coordinator probes it exactly the way
  `kura.conformance` probes a rented bucket, so a self-hosted node is not a
  special case anywhere in the system.

  **What it does NOT do, and must not be mistaken for.**

  - *No authentication.* Phase 0 places no customer data and takes no bond, so
    there is nothing here to steal. The moment either changes, every mutating
    request must carry a signed order (`kura.order/admit` already decides this)
    and this script must refuse to start without a verifier. That refusal is
    written as an assertion below rather than left as a TODO, because a node
    that quietly serves unauthenticated writes with real data on it is the
    failure this whole project keeps trying not to ship.
  - *No public address.* Binding a port is not being reachable. A node behind
    NAT needs a tunnel or a forwarded port; the coordinator cannot probe what
    it cannot reach. Whether that unreachability reads as a fault depends on
    `--availability`: an `intermittent` node is *expected* to go quiet, and the
    fleet caps how much of an object may depend on it precisely so that it can.
  - *No redundancy of its own.* One disk, no RAID. That is fine — absorbing a
    lost shard is what the erasure code is for — but the operator should know
    that is the arrangement rather than assume the network protects their disk."
  (:require ["node:http" :as http]
            ["node:process" :as process]
            [kura.node.async :as async]
            [kura.node.fs :as fs]
            [kura.node.store :as store]))

(defn- arg [flag default]
  (let [v (vec (.-argv process))
        i (.indexOf v flag)]
    (if (neg? i) default (nth v (inc i) default))))

(defn- flag? [f] (not (neg? (.indexOf (vec (.-argv process)) f))))

(defn- json-res [res status body]
  (.writeHead res status #js {"content-type" "application/json; charset=utf-8"})
  (.end res (js/JSON.stringify (clj->js body) nil 2)))

(defn- read-body> [req]
  (js/Promise.
   (fn [resolve reject]
     (let [chunks (atom [])]
       (.on req "data" #(swap! chunks conj %))
       (.on req "error" reject)
       (.on req "end" #(resolve (js/Buffer.concat (clj->js @chunks))))))))

(defn- shard-id-from [path]
  (second (re-matches #"^/shard/(.+)$" path)))

(defn- handle> [s req res]
  (let [url (js/URL. (.-url req) "http://localhost")
        path (.-pathname url)
        method (.-method req)
        sid (shard-id-from path)]
    (cond
      (= path "/descriptor")
      (js/Promise.resolve (json-res res 200 (store/-descriptor s)))

      (= path "/self-check")
      ;; A node should find its own rot before a coordinator's audit does —
      ;; failing an audit is a slashing event, and this is the cheap way to
      ;; learn the same thing.
      (-> (async/run> s)
          (.then (fn [r] (json-res res (if (zero? (:failed r)) 200 500) r))))

      (and sid (= method "PUT"))
      (-> (read-body> req)
          (.then (fn [b] (async/-put-shard!> s sid (js/Uint8Array. b))))
          (.then (fn [r] (json-res res 201 r)))
          (.catch (fn [e] (json-res res 400 {:error (.-message e)}))))

      (and sid (= method "GET"))
      (let [range (.get (.-searchParams url) "range")]
        (-> (if range
              (let [[o l] (map js/parseInt (.split range ","))]
                (async/-get-range> s sid o l))
              (async/-get-shard> s sid))
            (.then (fn [b]
                     (if b
                       (do (.writeHead res 200 #js {"content-type" "application/octet-stream"})
                           (.end res (js/Buffer.from b)))
                       (json-res res 404 {:error "absent"}))))
            (.catch (fn [e] (json-res res 500 {:error (.-message e)})))))

      (and sid (= method "DELETE"))
      (-> (async/-delete-shard!> s sid)
          (.then (fn [removed] (json-res res (if removed 200 404) {:removed removed}))))

      (= path "/shards")
      (-> (async/-list-shards> s (or (.get (.-searchParams url) "prefix") ""))
          (.then (fn [ids] (json-res res 200 {:shards ids :count (count ids)}))))

      :else
      (js/Promise.resolve
       (json-res res 200
                 {:what "a kura storage node"
                  :routes {"GET /descriptor" "who this node claims to be"
                           "GET /self-check" "run the shard-store contract against this disk"
                           "GET /shards?prefix=" "what is held"
                           "PUT /shard/<id>" "store"
                           "GET /shard/<id>[?range=off,len]" "fetch"
                           "DELETE /shard/<id>" "remove"}
                  :phase-0-caveats
                  ["no authentication — Phase 0 holds no customer data and takes no bond"
                   "binding a port is not being reachable; behind NAT you need a tunnel"
                   "one disk, no redundancy of its own — the erasure code absorbs a loss,
                    the network does not protect your disk"]})))))

(defn -main []
  (let [root (arg "--root" nil)
        port (js/parseInt (arg "--port" "8080"))
        node-id (arg "--node-id" "self-hosted-1")
        operator (arg "--operator" nil)
        site (arg "--site" nil)
        availability (keyword (arg "--availability" nil))]
    (assert root "--root is required (a directory this node may write to)")
    (assert operator
            (str "--operator is required. It is not decoration: the audit reads "
                 "the declared failure domain to decide how many INDEPENDENT "
                 "domains a fleet has, and two nodes that share an operator and "
                 "a site are one domain however they are named."))
    (assert site
            (str "--site is required, and it names a PLACE, not a machine. "
                 "Ten machines in one room are one failure domain: one power "
                 "feed, one uplink, one flood. Passing a hostname here — which "
                 "is the obvious thing to reach for, and which this project's "
                 "own first deployment did — makes each box look like its own "
                 "domain and inflates the fleet's durability by however many "
                 "boxes are in the room. Use somewhere you could lose all at "
                 "once: \"tokyo-office\", \"home\", \"osaka-colo\"."))
    (assert (contains? store/availabilities availability)
            (str "--availability is required and must be always-on or "
                 "intermittent. This is not a service tier, it is a fact about "
                 "the machine, and the fleet needs it to decide two different "
                 "things: how many shards may sit here (placement caps sleeping "
                 "nodes so every object stays readable from always-on nodes "
                 "alone), and whether an unanswered probe is a fault or a "
                 "closed lid. Declaring intermittent costs you nothing — it "
                 "means fewer shards land here and you are NOT penalised for "
                 "sleeping. Claiming always-on and then sleeping is the case "
                 "that gets penalised, so a laptop should say intermittent. "
                 "A machine that stays powered and reachable — a server, a mini "
                 "on a shelf — says always-on."))
    ;; The refusal promised in the namespace docstring, as an assertion rather
    ;; than a TODO.
    (assert (not (flag? "--accept-customer-data"))
            (str "This build cannot accept customer data: it has no order "
                 "verifier, so it would serve unauthenticated writes. Wire "
                 "kura.order/admit and a coordinator public key first."))
    (let [s (fs/open {:node-id node-id
                      :root root
                      :failure-domain {:operator operator :site site}
                      :independence :independent
                      :availability availability})]
      (-> (.createServer http (fn [req res] (handle> s req res)))
          (.listen port
                   (fn []
                     (println "kura node listening on" (str "http://0.0.0.0:" port))
                     (println "  root      :" root)
                     (println "  descriptor:" (pr-str (store/-descriptor s)))
                     (println)
                     (println "  Phase 0: no auth, no customer data, no bond.")
                     (println "  Reachability is yours to arrange — a bound port is not a")
                     (println "  public address, and the coordinator cannot probe what it")
                     (println "  cannot reach.")))))))

(-main)
