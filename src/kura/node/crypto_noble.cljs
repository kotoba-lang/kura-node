(ns kura.node.crypto-noble
  "`ICrypto` over `@noble/hashes` — synchronous, and the same code in Node, a
  Worker and a browser.

  **Why not WebCrypto.** A Worker has `crypto.subtle`, which is the obvious
  choice and the wrong one here: `subtle.digest` and `subtle.sign` are
  **async**, and `kura.node.s3/sign` is synchronous because SigV4 is a
  straight-line derivation with no I/O in it. Making the signer async to
  accommodate the host's crypto API would push promises through every caller
  of a pure function. A synchronous audited implementation is the smaller
  change, and `@noble/hashes` is already what `kotobase-protocols-worker` uses
  for exactly this reason.

  **Why not `node:crypto`.** It does not resolve in a Worker build without the
  `nodejs_compat` flag, and reaching for a compatibility shim to get a hash
  function is carrying a platform dependency to avoid a 12-line file.

  Buffers stay raw across the HMAC ladder — SigV4 feeds each step's digest
  into the next, and hex-encoding between steps produces a signature that is
  wrong in a way no self-consistent test catches."
  (:require ["@noble/hashes/hmac" :refer [hmac]]
            ["@noble/hashes/sha2" :refer [sha256]]
            ["@noble/hashes/utils" :as nutils]
            [kura.node.s3 :as s3]))

(defn- ->u8 [x]
  (cond
    (string? x) (.encode (js/TextEncoder.) x)
    (instance? js/Uint8Array x) x
    :else (js/Uint8Array. x)))

(defrecord NobleCrypto []
  s3/ICrypto
  (-sha256-hex [_ b] (nutils/bytesToHex (sha256 (->u8 b))))
  (-hmac-sha256 [_ k m] (hmac sha256 (->u8 k) (->u8 m)))
  (-hex [_ b] (nutils/bytesToHex (->u8 b)))
  (-utf8 [_ s] (.encode (js/TextEncoder.) s)))

(defn noble-crypto [] (->NobleCrypto))
