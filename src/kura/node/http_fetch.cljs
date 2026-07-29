(ns kura.node.http-fetch
  "`IHttp` over the global `fetch` — Node 18+, a Cloudflare Worker, a browser.

  Split out of `kura.node.host-node` when the Worker build failed on that
  namespace's `node:crypto` import: the transport was never Node-specific,
  only the crypto next to it was. Nothing here touches a platform builtin.

  `redirect: manual` is not a preference. A redirect that the client follows
  arrives at the new URL carrying a signature computed over the old one, so it
  fails authentication in a way that reads like a credential problem. Better
  to see the 3xx."
  (:require [kura.node.s3 :as s3]))

(defrecord FetchHttp []
  s3/IHttp
  (-request [_ {:keys [method url headers body]}]
    (-> (js/fetch url
                  (clj->js (cond-> {:method (-> method name .toUpperCase)
                                    :headers headers
                                    :redirect "manual"}
                             (and body (pos? (.-length ^js body)))
                             (assoc :body body))))
        (.then (fn [resp]
                 (-> (.arrayBuffer ^js resp)
                     (.then (fn [ab]
                              {:status (.-status ^js resp)
                               ;; Object.fromEntries rather than the Headers
                               ;; iterator: `es6-iterator-seq` is a
                               ;; ClojureScript-compiler builtin that nbb does
                               ;; not carry, and this runs on both.
                               :headers (into {}
                                              (map (fn [[k v]] [(.toLowerCase k) v]))
                                              (js->clj (js/Object.fromEntries
                                                        (.entries (.-headers ^js resp)))))
                               :body (js/Uint8Array. ab)}))))))))

(defn fetch-http [] (->FetchHttp))

(defn now-iso
  "Current instant as ISO-8601. Passed to the signer explicitly, because
  `sigv4` never reads a clock and inheriting that keeps a signing test a pure
  function of its inputs."
  []
  (.toISOString (js/Date.)))
