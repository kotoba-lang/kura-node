(ns kura.node.cljs-runner
  "Portable suite under a real ClojureScript host.

    clojure -M:cljs -m cljs.main --target node -m kura.node.cljs-runner

  Worth running here specifically: `kura.node.gf` is where the two hosts differ
  most — a JVM `byte[]` is signed and a `Uint8Array` is not — so the
  byte-equality gate against `erasure.gf` means something different, and more,
  on each side."
  (:require [clojure.test :as t :refer [run-tests]]
            [kura.node.accounting-test]
            [kura.node.audit-test]
            [kura.node.gf-test]
            [kura.node.s3-test]
            [kura.node.store-test]))

#?(:cljs
   (defmethod t/report [:cljs.test/default :end-run-tests] [m]
     (when-not (t/successful? m)
       (set! (.-exitCode js/process) 1))))

(defn -main []
  (run-tests 'kura.node.store-test 'kura.node.gf-test 'kura.node.audit-test
             'kura.node.accounting-test 'kura.node.s3-test))
