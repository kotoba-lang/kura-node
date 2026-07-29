# kura-node

The mechanism layer of the [kura](https://github.com/kotoba-lang/kura) storage
network: hold shards, move bytes, answer audits, count what you are owed.

Design: [ADR-2607299200](https://github.com/com-junkawasaki/root/blob/main/90-docs/adr/2607299200-kura-erasure-coded-storage-network.edn).
`kura` decides; this executes. Placement, repair scheduling and payment
amounts are not here and must not migrate here.

## The thing to read first

**A backend must declare how independent its failures are, and there is no
default.**

```clojure
(store/descriptor {:node-id "b2-0"
                   :failure-domain {:provider "b2" :bucket "kura-0"}
                   :independence :shared-substrate})   ; required
```

ADR section 1 gets a 1.625× storage multiplier to ten nines by assuming shard
losses are independent. Phase 0 stands up pseudo-nodes on rented backends —
B2, R2, S3, Storj — because you cannot pick a multiplier without a measured
node-loss rate, and you cannot measure one without running the real planes
against something. But **twenty-six pseudo-nodes on one B2 account are one
node.** The code cannot tell; it sees twenty-six shards in twenty-six places
and reports a seven-erasure tolerance that does not exist. What you have is
B2's durability, and you paid 1.625× not to get the code's.

So `independence-profiles` is a closed set — `:independent`,
`:shared-provider`, `:shared-substrate` — the choice is mandatory, and
`store/audit` reports the consequence:

```clojure
(store/audit fleet 7)
;; {:backends 26 :effective-domains 1 :largest-domain 26 :survivable? false
;;  :note "one domain holds 26 shards but the code tolerates 7 —
;;         durability is that domain's, not the code's"}
```

That is not a flaw in Phase 0. It is the fact Phase 0 exists to make visible.
The conformance suite deliberately does **not** check the declaration — whether
two buckets share a control plane is a fact about the world, not about an API.
It is a claim its operator is accountable for.

## The fast path, and why it is allowed to exist

`erasure.gf/scale-add` is the definition: a vector of integers, one field
multiply per byte through log/exp lookups. It is the oracle and it is far too
slow to move a petabyte. `kura.node.gf` is what a node runs — one 256-byte
table per coefficient, so the inner loop is an indexed load and an xor:

```
acc[i] ^= mul_table_c[xs[i]]
```

ADR section 5 designates this as the **one** piece of mechanism a host provider
may replace, on condition it proves byte-equality against the definition.
`kura.node.gf-test` is that condition: the coefficient sweep is exhaustive over
all 256 field elements, the table check is exhaustive over all 65,536 products,
and the encode comparison runs the real k=16/r=4/g=6 layout shard for shard.

The tables are built from `erasure.gf/mul-def` — the table-free
Russian-peasant definition — not from `erasure.gf/mul`, which is itself
table-driven. A fast path derived from the slow path's own tables could share
its bugs and never know.

**Signed bytes are a real hazard here.** A JVM `byte[]` holds 200 as −56, and
−56 is not a table index. Every read masks with `0xff`; the suite runs on both
hosts precisely because `Uint8Array` does not have this problem and the JVM
does.

## What else is here

- **`kura.node.store`** — the backend protocol, the independence declaration,
  and the domain-collapse audit.
- **`kura.node.contract`** — the conformance suite, shipped in `src` (not
  `test`) so a third party writing a backend can actually run it.
- **`kura.node.memory`** — reference backend. Declares `:shared-substrate`,
  because that is what one heap is.
- **`kura.node.s3`** — B2 / R2 / S3 / Storj Gateway-MT. Zero I/O and zero
  crypto: `IHttp` and `ICrypto` are injected and signing comes from
  `kotoba-lang/sigv4`, so request shaping is a pure function testable without
  credentials. Range reads use a real `Range` header — a backend that reads
  4 MiB to return 1 KiB gives back the whole benefit of the systematic layout
  (ADR section 7). Listing re-filters client-side, because "S3-compatible" is
  a claim rather than a conformance result and a backend that accepts the
  `prefix` parameter and ignores it would have the audit tree commit to shards
  this node does not hold.
- **`kura.node.audit`** — commit to what is *on the store*, not to a manifest;
  a node that commits to what it was supposed to have passes its own audit
  while missing shards. Takes **two** hashers, because shard bodies are buffers
  and `merkle-sum`'s node preimage is text — conflating them fails loudly on
  the JVM and quietly elsewhere. `self-check` is a full local scrub: sampling
  is what the *coordinator* does instead of scrubbing, not what the node does.
- **`kura.node.accounting`** — the byte limit enforced per chunk rather than
  once, because a node that checks at the start and then streams has not
  checked. Refusal is a return value, not an exception: hitting the limit is
  the ordinary end of a transfer for a client that asked for more than it paid
  for, and a node that throws there has that path exercised by every over-eager
  client on the network.

## Live: the signature is proven against a real service

`kura.node.s3` is pure by design — request shaping is a function of its
inputs, testable without credentials. `kura.node.host-node` is the other half:
`node:crypto` and `fetch`, the one file in this repo that is host-specific.

`script/live_sigv4_check.cljs` closes the loop by signing a **read-only**
ListObjectsV2 against a real S3-compatible bucket. A wrong signature returns
403, so a 200 proves the whole SigV4 ladder — canonical request, credential
scope, the four-step key derivation, the authorization header — end to end.

```
$ B2_KEY_ID=... B2_APP_KEY=... B2_BUCKET=... nbb --classpath "src:..." \
    script/live_sigv4_check.cljs
status: 200
SIGV4 OK — service accepted the signature
```

Verified against Backblaze B2 (`s3.us-west-004.backblazeb2.com`) on
2026-07-29. Read-only on purpose: the credential to hand is scoped to an
existing archive bucket, and writing conformance fixtures into somebody's
archive to test our own code is not a trade this repo makes. Full read/write
conformance needs a bucket of its own — see the gap below.

**The SigV4 hazard this file exists to contain:** the key-derivation ladder
feeds each HMAC's *raw* digest into the next. Hex-encoding between steps
produces a signature that is wrong in a way no unit test catches, because both
sides of a self-consistent test agree — you find out when a real service
returns 403. `-hmac-sha256` returns a Buffer and only `-hex` ever stringifies.

### Known gap

No live read/write conformance run yet. `kura.node.contract/verify` passes
against the in-memory and faked-HTTP backends, which proves the adapter's
logic; it does not prove that a particular provider honours `Range`, returns
`content-length` on HEAD, or respects the `prefix` parameter on list. Those
are exactly the claims "S3-compatible" makes and does not guarantee — and the
reason `-list-shards` re-filters client-side. Phase 0 needs a dedicated bucket
to close this.

## Tests

```bash
clojure -M:test
clojure -M:cljs -m cljs.main --target node -m kura.node.cljs-runner
clojure -M:lint
```

## License

MIT.
