(ns dogma.t-core
  (:require [midje.sweet :refer :all]
            [dogma.core :refer :all]
            [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [taoensso.timbre :as timbre]))

; logging util
(defn gen-timbre-test-config [log-level]
  {:min-level log-level
   :output-fn (fn [{:keys [msg_]}]
                (let [tname (.getName (Thread/currentThread))
                      tname (subs tname 0 (min 20 (count tname)))]
                  (str \[ tname "] " (force msg_))))})

;basic sepl works - towers of hanoi (fun!)
#_(let [num-disks 3
        towers-of-hanoi (atom {:a (into [] (reverse (range 1 (inc num-disks))))
                               :b []
                               :c []})
        pop-disk (fn [from] (swap! towers-of-hanoi update from pop))
        push-disk (fn [disk to] (swap! towers-of-hanoi update to conj disk))
        move-disk (fn [disk from to] (pop-disk from) (push-disk disk to))]
    (letfn [(recursion-step [disks from to other]
              (let [all-but-bottom-disk (dec disks)
                    bottom-disk disks]
                (solve all-but-bottom-disk from other to)
                (move-disk bottom-disk from to)
                (solve all-but-bottom-disk other to from)))
            (solve [disks from to other]
              (if (= 1 disks)
                (move-disk 1 from to)
                (recursion-step disks from to other)))]
      (solve num-disks :a :c :b)))

;lazy towers of hanoi
(let [
      num-disks 3
      towers-of-hanoi (atom {:a (into [] (reverse (range 1 (inc num-disks))))
                             :b []
                             :c []})
      pop-n-push (fn [disk from to]
                   (swap! towers-of-hanoi update from pop)
                   (swap! towers-of-hanoi update to conj disk))
      ;side-effects
      move-disk (fn [state [disk from to]]
                  (if async? (async/go (pop-n-push disk from to))
                             (pop-n-push disk from to)))
      ;pure biz logic
      recursion-step (fn [disks from to other]
                       (let [all-but-bottom-disk (dec disks)
                             bottom-disk disks
                             next-steps '()]
                         (-> next-steps
                             (->step :solve [all-but-bottom-disk from other to])
                             (->step :move-disk [bottom-disk from to])
                             (->step :solve [all-but-bottom-disk other to from]))))
      solve (fn [[disks from to other] outcome]
              (if (= 1 disks)
                [(->step :move-disk [1 from to])]
                (recursion-step disks from to other)))
      ;flows
      flows {:solve     {:process-fn solve}
             :move-disk {:side-effect-fn move-disk}}

      ;solve for num-disks
      triggers [(->trigger :solve [num-disks :a :b :c])]]

  (timbre/with-merged-config
    (gen-timbre-test-config :error)
    (set-async false)
    (fact (->> (lazy-sepl flows triggers nil)
               (filter #(= :move-disk (get-in % [:trigger :type])))
               (map (juxt :trigger :state)))
          => '([{:args [1 :a :b] :depth 3 :type :move-disk} {:a [3 2] :b [1] :c []}]
               [{:args [2 :a :c] :depth 2 :type :move-disk} {:a [3] :b [1] :c [2]}]
               [{:args [1 :b :c] :depth 3 :type :move-disk} {:a [3] :b [] :c [2 1]}]
               [{:args [3 :a :b] :depth 1 :type :move-disk} {:a [] :b [3] :c [2 1]}]
               [{:args [1 :c :a] :depth 3 :type :move-disk} {:a [1] :b [3] :c [2]}]
               [{:args [2 :c :b] :depth 2 :type :move-disk} {:a [1] :b [3 2] :c []}]
               [{:args [1 :a :b] :depth 3 :type :move-disk} {:a [] :b [3 2 1] :c []}]))))

;lazy fib, why not
(let [fib (atom [0 1])
      ;side-effects
      load-fn (fn [state args] (if async? (async/go (take-last 2 @fib))
                                          (take-last 2 @fib)))
      store-fn (fn [state args] (if async? (async/go (swap! fib conj args))
                                           (swap! fib conj args)))
      ;pure biz logic
      calc-next-fib (fn [[n-2 n-1]] (+ n-2 n-1))

      ;flows
      flows {:fib   {:side-effect-fn load-fn
                     :process-fn     (fn [args side-effect-outcome]
                                       (when (pos? args)
                                         (let [next-fib (calc-next-fib side-effect-outcome)
                                               next-steps '()]
                                           (-> next-steps
                                               (->trigger :store next-fib)
                                               (->trigger :fib (dec args))))))}
             :store {:side-effect-fn store-fn}}

      ;solve for 5
      triggers [(->trigger :fib 5)]]

  (timbre/with-merged-config
    (gen-timbre-test-config :error)
    (set-async false)
    (fact (->> (lazy-sepl flows triggers nil)
               (filter #(= :store (get-in % [:trigger :type])))
               (map (juxt :trigger :state)))
          => '([{:args 1 :depth 1 :type :store} [0 1 1]]
               [{:args 2 :depth 2 :type :store} [0 1 1 2]]
               [{:args 3 :depth 3 :type :store} [0 1 1 2 3]]
               [{:args 5 :depth 4 :type :store} [0 1 1 2 3 5]]
               [{:args 8 :depth 5 :type :store} [0 1 1 2 3 5 8]]))))

;basic sepl works
(let [
      ;side-effects
      load-fn (fn [state args] (if async? (async/go (read-string state))
                                          (read-string state)))
      store-fn (fn [state args] (if async? (async/go (str args))
                                           (str args)))
      ;pure biz logic
      pure-biz-logic (fn [a b] (+ a b))
      ;end-to-end logical flow: adder (load state -> calc addition -> store new state)
      flows {:add   {:side-effect-fn load-fn
                     :process-fn     (fn [args side-effect-outcome]
                                       (let [outcome (pure-biz-logic args side-effect-outcome)]
                                         [(->trigger :store outcome)]))}
             :store {:side-effect-fn store-fn}}
      ;input: 1, 2, 3..
      triggers [(->trigger :add "1")
                (->trigger :add "2")
                (->trigger :add "3")]]
  (timbre/with-merged-config
    (gen-timbre-test-config :error)
    (letfn [(run-sepl-fn [] (sepl flows triggers "0"
                                  :->ser-args str
                                  :->deser-args read-string))
            (verify-fn [result] (fact result => "6"))]
      ;the 'simple' case, sync
      (set-async false)
      (verify-fn (run-sepl-fn))
      ;but even if any side effect is async, the flow runs just the same
      (set-async true)
      (async/take! (run-sepl-fn) verify-fn))))

;basic lazy sepl works
(let [
      ;side-effects
      load-fn (fn [state args] (if async? (async/go (read-string state))
                                          (read-string state)))
      store-fn (fn [state args] (if async? (async/go (str args))
                                           (str args)))
      ;pure biz logic
      pure-biz-logic (fn [a b] (+ a b))
      ;end-to-end logical flow: adder (load state -> calc addition -> store new state)
      flows {:add   {:side-effect-fn load-fn
                     :process-fn     (fn [args side-effect-outcome]
                                       (let [outcome (pure-biz-logic args side-effect-outcome)]
                                         [(->trigger :store outcome)]))}
             :store {:side-effect-fn store-fn}}
      ;input: 1, 2, 3..
      triggers [(->trigger :add "1")
                (->trigger :add "2")
                (->trigger :add "3")]]
  (timbre/with-merged-config
    (gen-timbre-test-config :error)
    (letfn [(run-sepl-fn [] (lazy-sepl flows triggers "0"
                                       :->ser-args str
                                       :->deser-args read-string))
            (verify-fn [result]
              (fact (map (juxt :trigger :state) result) => '([{:args "1" :type :add} "0"]
                                                             [{:args "1" :depth 1 :type :store} "1"]
                                                             [{:args "2" :type :add} "1"]
                                                             [{:args "3" :depth 1 :type :store} "3"]
                                                             [{:args "3" :type :add} "3"]
                                                             [{:args "6" :depth 1 :type :store} "6"])))]
      ;the 'simple' case, sync
      (set-async false)
      (verify-fn (async-resolving-lazy-seq (run-sepl-fn)))
      ;but even if any side effect is async, the flow runs just the same
      (set-async true)
      (verify-fn (async-resolving-lazy-seq (run-sepl-fn)))
      )))

;basic async sepl works
(let [
      ;side-effects
      load-fn (fn [state args] (if async? (async/go (read-string state))
                                          (read-string state)))
      store-fn (fn [state args] (if async? (async/go (str args))
                                           (str args)))
      ;pure biz logic
      pure-biz-logic (fn [a b] (+ a b))
      ;end-to-end logical flow: adder (load state -> calc addition -> store new state)
      flows {:add   {:side-effect-fn load-fn
                     :process-fn     (fn [args side-effect-outcome]
                                       (let [outcome (pure-biz-logic args side-effect-outcome)]
                                         [(->trigger :store outcome)]))}
             :store {:side-effect-fn store-fn}}
      ;input: 1, 2, 3..
      triggers [(->trigger :add "1")
                (->trigger :add "2")
                (->trigger :add "3")]]
  (timbre/with-merged-config
    (gen-timbre-test-config :error)
    (letfn [(run-sepl-fn [] (async-sepl flows triggers "0"
                                        :->ser-args str
                                        :->deser-args read-string))
            (verify-fn [result]
              (fact (map :trigger result) => '({:args "1" :type :add}
                                               {:args "1" :depth 1 :type :store}
                                               {:args "2" :type :add}
                                               {:args "3" :depth 1 :type :store}
                                               {:args "3" :type :add}
                                               {:args "6" :depth 1 :type :store})))]
      ;the 'simple' case, sync
      (set-async false)
      (verify-fn (ch->lazy-seq (run-sepl-fn)))
      ;but even if any side effect is async, the flow runs just the same
      (set-async true)
      (verify-fn (ch->lazy-seq (run-sepl-fn)))
      )))

;max iterations limit
(let [flows {:loop {:side-effect-fn (fn [state _] state)
                    :process-fn     (fn [args side-effect-outcome] [(->trigger :loop true)])}}
      triggers [(->trigger :loop true)]
      verify-fn #(fact (type %) => AssertionError)]
  (set-async false)
  (verify-fn (sepl flows triggers false :max-iterations 5))
  (set-async true)
  (async/take! (sepl flows triggers false :max-iterations 5) verify-fn))

;max depth limit
(let [flows {:loop {:side-effect-fn (fn [state _] state)
                    :process-fn     (fn [args side-effect-outcome] [(->trigger :loop true)])}}
      triggers [(->trigger :loop true)]
      verify-fn #(fact (type %) => AssertionError)]
  (set-async false)
  (verify-fn (sepl flows triggers false :max-depth 5))
  (set-async true)
  (async/take! (sepl flows triggers false :max-depth 5) verify-fn))

;max duration limit
(let [flows {:loop {:side-effect-fn (fn [state _] state)
                    :process-fn     (fn [args side-effect-outcome] [(->trigger :loop true)])}}
      triggers [(->trigger :loop true)]
      verify-fn #(fact (type %) => AssertionError)]
  (set-async false)
  (verify-fn (sepl flows triggers false :max-duration 5))
  (set-async true)
  (async/take! (sepl flows triggers false :max-duration 5) verify-fn))

;graph units

;link flow
(timbre/with-merged-config
  (gen-timbre-test-config :error)
  (with-redefs [get-time-ms (constantly 0)]
    (let [f (get-in core-flows [:link-flow :process-fn])
          ref (gen-ref "n2/123" :n2/->n1 {:me "ta"})]
      (tabular (fact "link-flow contract" (with-redefs [get-time-ms (constantly 1)]
                                            (f ?args ?curr-value)) => ?expected)

               ?id ?args ?curr-value ?expected

               ;new link, *:1 ref, no meta-fn, no meta
               "new link, *:1 ref, no meta-fn, no meta"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {}}
               nil
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value nil :value {:created 1 :data {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1} :modified 1 :version 1}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n1/123", :attribute :n1/->n2, :event :boot, :event-data {[:id] "n1/123"}}}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;new link, *:1 ref, with meta-fn, no meta
               "new link, *:1 ref, with meta-fn, no meta"
               {:node "n1/123" :attribute (->with-meta (->ref :n2/->n1 :id :n1/->n2) #(do {:source %})) :target-node "n2/123" :target-attribute :n2/->n1 :meta {}}
               nil
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {:source "n1/123"} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value nil :value {:created 1 :data {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1} :modified 1 :version 1}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n1/123", :attribute :n1/->n2, :event :boot, :event-data {[:id] "n1/123"}}}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;new link, *:1 ref, no meta-fn, with meta
               "new link, *:1 ref, no meta-fn, with meta"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {:me "ta"}}
               nil
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 1 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value nil :value {:created 1 :data {:created 1 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :modified 1 :version 1}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n1/123", :attribute :n1/->n2, :event :boot, :event-data {[:id] "n1/123"}}}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;existing link, *:1 ref, no meta-fn, no meta
               "existing link, *:1 ref, no meta-fn, no meta"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {}}
               {:created 0 :data {:created 0 :id "n2/123" :modified 0 :symmetric-attribute :n2/->n1} :modified 0 :version 0}
               '({:args {:attribute :n2/->n1 :event :visit :event-data nil :node "n2/123"} :type :notify-event-flow})

               ;existing link with meta, *:1 ref, no meta-fn, with meta
               "existing link with meta, *:1 ref, no meta-fn, with meta"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {:me "ta"}}
               {:created 0 :data {:created 0 :id "n2/123" :me "ta" :modified 0 :symmetric-attribute :n2/->n1} :modified 0 :version 0}
               '({:args {:attribute :n2/->n1 :event :visit :event-data nil :node "n2/123"} :type :notify-event-flow})

               ;existing link, *:1 ref, no meta-fn, with meta
               "existing link, *:1 ref, no meta-fn, with meta"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {:me "ta"}}
               {:created 0 :data {:created 0 :id "n2/123" :modified 0 :symmetric-attribute :n2/->n1} :modified 0 :version 0}
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 0 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value {:created 0 :data {:created 0 :id "n2/123" :modified 0 :symmetric-attribute :n2/->n1} :modified 0 :version 0} :value {:created 0 :data {:created 0 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :modified 1 :version 0}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;existing other link, *:1 ref, no meta-fn, with meta
               "existing other link, *:1 ref, no meta-fn, with meta"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {:me "ta"}}
               {:created 0 :data {:created 0 :id "n2/999" :modified 0 :symmetric-attribute :n2/->n1} :modified 0 :version 0}
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 1 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value {:created 0 :data {:created 0 :id "n2/999" :modified 0 :symmetric-attribute :n2/->n1} :modified 0 :version 0} :value {:created 0 :data {:created 1 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :modified 1 :version 0}} :type :mutate-flow}
                 {:args {:attribute :n2/->n1 :node "n2/999" :target-attribute :n1/->n2 :target-node "n1/123"} :type :unlink-flow}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;new link, *:* ref, no meta-fn, no meta
               "new link, *:* ref, no meta-fn, no meta"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {}}
               nil
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value nil :value {:created 1 :data {["n2/123" :n2/->n1] {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1}} :modified 1 :version 1}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n1/123", :attribute :n1/->n2, :event :boot, :event-data {[:id] "n1/123"}}}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;new link, *:* ref, with meta-fn, no meta
               "new link, *:* ref, with meta-fn, no meta"
               {:node "n1/123" :attribute (->with-meta (->refs :n2/->n1 :id :n1/->n2) #(do {:source %})) :target-node "n2/123" :target-attribute :n2/->n1 :meta {}}
               nil
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {:source "n1/123"} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value nil :value {:created 1 :data {["n2/123" :n2/->n1] {:created 1 :id "n2/123" :modified 1 :symmetric-attribute :n2/->n1}} :modified 1 :version 1}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n1/123", :attribute :n1/->n2, :event :boot, :event-data {[:id] "n1/123"}}}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;new link, *:* ref, no meta-fn, with meta
               "new link, *:* ref, no meta-fn, with meta"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {:me "ta"}}
               nil
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 1 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value nil :value {:created 1 :data {["n2/123" :n2/->n1] {:created 1 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1}} :modified 1 :version 1}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n1/123", :attribute :n1/->n2, :event :boot, :event-data {[:id] "n1/123"}}}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;existing link, *:* ref, no meta-fn, no meta
               "existing link, *:* ref, no meta-fn, no meta"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {}}
               {:created 0 :data {["n2/123" :n2/->n1] {:created 0 :id "n2/123" :modified 0 :symmetric-attribute :n2/->n1}} :modified 0 :version 0}
               '({:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;existing link with meta, *:* ref, no meta-fn, with meta
               "existing link with meta, *:* ref, no meta-fn, with meta"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1 :meta {:me "ta"}}
               {:created 0 :data {["n2/123" :n2/->n1] {:created 0 :id "n2/123" :me "ta" :modified 0 :symmetric-attribute :n2/->n1}} :modified 0 :version 0}
               '({:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})

               ;existing link, *:* ref, no meta-fn, with meta
               "existing link, *:* ref, no meta-fn, with meta"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) #_{:id :n1/->n2 :type :refs} :target-node "n2/123" :target-attribute :n2/->n1 :meta {:me "ta"}}
               {:created 0 :data {["n2/123" :n2/->n1] {:created 0 :id "n2/123" :modified 0 :symmetric-attribute :n2/->n1}} :modified 0 :version 0}
               '({:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                 {:args {:attribute :n2/->n1 :event :discovery :event-data nil :node "n2/123"} :type :notify-event-flow}
                 {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                 {:args {:attribute :n2/->n1 :meta {} :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :link-flow}
                 {:args {:attribute :n1/->n2 :new-value {:created 0 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1} :node "n1/123" :old-value {:created 0 :data {["n2/123" :n2/->n1] {:created 0 :id "n2/123" :modified 0 :symmetric-attribute :n2/->n1}} :modified 0 :version 0} :value {:created 0 :data {["n2/123" :n2/->n1] {:created 0 :id "n2/123" :me "ta" :modified 1 :symmetric-attribute :n2/->n1}} :modified 1 :version 0}} :type :mutate-flow}
                 {:type :notify-event-flow, :args {:node "n2/123", :attribute :n2/->n1, :event :visit, :event-data nil}})))))

;unlink flow
(timbre/with-merged-config
  (gen-timbre-test-config :error)
  (with-redefs [get-time-ms (constantly 1)]
    (let [f (get-in core-flows [:unlink-flow :process-fn])
          ref (gen-ref "n2/123" :n2/->n1 {:me "ta"})]
      (tabular (fact "unlink-flow contract" (f ?args ?curr-value) => ?expected)

               ?id ?args ?curr-value ?expected

               ;no link, *:1 ref
               "no link, *:1 ref"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1}
               nil
               nil

               ;no link, *:* ref
               "no link, *:* ref"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1}
               nil
               nil

               ;other link, *:* ref
               "other link, *:* ref"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1}
               {:created 0 :data {[1 2] {}} :modified 0 :version 0}
               nil

               ;existing ref, *:1 ref
               "existing ref, *:1 ref"
               {:node "n1/123" :attribute (->ref :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1}
               {:created 0 :data ref :modified 0 :version 0}
               [{:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow} {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                {:args {:attribute :n2/->n1 :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :unlink-flow}
                {:args {:attribute :n1/->n2 :new-value (->ref-id ref) :node "n1/123" :old-value {:created 0 :data ref :modified 0 :version 0} :value {:created 0 :data nil :modified 1 :version 0}} :type :mutate-flow}]

               ;existing ref, *:* ref
               "existing ref, *:* ref"
               {:node "n1/123" :attribute (->refs :n2/->n1 :id :n1/->n2) :target-node "n2/123" :target-attribute :n2/->n1}
               {:created 0 :data {(->ref-id ref) ref} :modified 0 :version 0}
               [{:args {:attribute :n1/->n2 :event :modified :event-data {:attribute :n1/->n2 :node "n1/123"} :node "n1/123"} :type :notify-event-flow}
                {:args {:attribute :n1/->n2 :node "n1/123"} :type :notify-flow}
                {:args {:attribute :n2/->n1 :node "n2/123" :target-attribute :n1/->n2 :target-node "n1/123"} :type :unlink-flow}
                {:args {:attribute :n1/->n2 :new-value (->ref-id ref) :node "n1/123" :old-value {:created 0 :data {(->ref-id ref) ref} :modified 0 :version 0} :value {:created 0 :data {} :modified 1 :version 0}} :type :mutate-flow}]))))

(def sync-http-mock (fn [_ & [cb]]
                      (let [rsp (cb _)]
                        ;(prn rsp)
                        ;(prn "sync-http" _)
                        (deliver (promise) rsp))))

(def async-http-mock (fn [_ & [cb]]
                       (let [rsp (cb _)]
                         ;(prn rsp)
                         ;(prn "async-http" _)
                         (async/go rsp))))

(defn default-post-fn [{:keys [blueprints] :as graph}]
  (if (instance? Throwable graph)
    (.printStackTrace graph)
    (map-vals (fn [node]
                (into {}
                      (map (fn [[attribute {:keys [data] :as value}]]
                             (let [t (:type (get-attribute blueprints attribute))]
                               (case t
                                 :ref [attribute (get data :id)]
                                 :refs [attribute (map :id (vals data))]
                                 [attribute data])))
                           (dissoc node :id :type :version :created :modified))))
              (:nodes graph))))

(defn test-graph [blueprints triggers expected-result
                  & {:keys [sync async post-fn mock-http log-level tear-down-fn]
                     :or   {sync true async true post-fn default-post-fn mock-http true log-level :error}}]
  (timbre/with-merged-config
    (gen-timbre-test-config log-level)
    (with-redefs [get-time-ms (constantly 0)]
      (when sync
        (set-async false)
        (fact
          (post-fn
            (if mock-http
              (with-redefs [http/request sync-http-mock]
                (build-graph blueprints triggers))
              (build-graph blueprints triggers))) => expected-result)
        (when tear-down-fn (tear-down-fn)))
      (when async
        (set-async true)
        (fact
          (post-fn
            (if mock-http
              (with-altered-var-root #'http/request async-http-mock
                                     (async/<!!
                                       (build-graph blueprints triggers)))
              (async/<!!
                (build-graph blueprints triggers)))) => expected-result)))))

(defn test-graph-steps [blueprints triggers expected-result
                        & {:keys [sync async post-fn mock-http log-level tear-down-fn]
                           :or   {sync true async true post-fn default-post-fn mock-http true log-level :error}}]
  (timbre/with-merged-config
    (gen-timbre-test-config log-level)
    (with-redefs [get-time-ms (constantly 0)]
      (when sync
        (set-async false)
        (fact
          (post-fn
            (if mock-http
              (with-redefs [http/request sync-http-mock]
                (->steps blueprints triggers))
              (->steps blueprints triggers))) => expected-result)
        (when tear-down-fn (tear-down-fn)))
      (when async
        (set-async true)
        (fact
          (post-fn
            (if mock-http
              (with-altered-var-root #'http/request async-http-mock
                                     (into [] (async-resolving-lazy-seq (->steps blueprints triggers))))
              (into [] (async-resolving-lazy-seq (->steps blueprints triggers))))) => expected-result)))))

(defn test-graph-async-steps [blueprints triggers expected-result
                              & {:keys [sync async post-fn mock-http log-level tear-down-fn]
                                 :or   {sync true async true post-fn default-post-fn mock-http true log-level :error}}]
  (timbre/with-merged-config
    (gen-timbre-test-config log-level)
    (with-redefs [get-time-ms (constantly 0)]
      (when sync
        (set-async false)
        (fact
          (post-fn
            (if mock-http
              (with-redefs [http/request sync-http-mock]
                (into [] (ch->lazy-seq (->async-steps blueprints triggers))))
              (into [] (ch->lazy-seq (->async-steps blueprints triggers))))) => expected-result)
        (when tear-down-fn (tear-down-fn)))
      (when async
        (set-async true)
        (fact
          (post-fn
            (if mock-http
              (with-altered-var-root #'http/request async-http-mock
                                     (into [] (ch->lazy-seq (->async-steps blueprints triggers))))
              (into [] (ch->lazy-seq (->async-steps blueprints triggers))))) => expected-result)))))

;basic mvp - graph meta
(let [
      blueprints
      {:node/attribute (->static)}

      triggers
      (->update [] "node/123" :node/attribute "val")

      expected
      {:nodes            1,
       :node-types       '("node"),
       :events-listeners {"node"
                          {:boot     '(:created :version :id),
                           :modified '(:modified)}},
       :blueprints-brief
                         {:node/attribute '(:version :type :value-type :notify-events :diff-fn :set-fn :unset-fn :id),
                          :id             '(:unset-fn :sources :notify-events :type :diff-fn :derefable? :id :set-fn :on-events :eval-fn :version :listeners :notifiers :value-type :async?),
                          :type           '(:unset-fn :sources :notify-events :type :diff-fn :derefable? :id :set-fn :eval-fn :version :notifiers :value-type :async?),
                          :version        '(:unset-fn :sources :notify-events :type :diff-fn :derefable? :id :set-fn :on-events :eval-fn :version :notifiers :value-type :async?),
                          :created        '(:unset-fn :sources :notify-events :type :diff-fn :derefable? :id :set-fn :on-events :eval-fn :version :notifiers :value-type :async?),
                          :modified       '(:unset-fn :sources :notify-events :type :diff-fn :derefable? :id :set-fn :on-events :eval-fn :version :notifiers :value-type :async?)}}]

  (test-graph blueprints
              triggers
              expected
              :post-fn (fn [{:keys [blueprints nodes] :as graph}]
                         (merge (select-keys blueprints [:node-types :events-listeners])
                                {:blueprints-brief (map-vals keys (dissoc blueprints :node-types :events-listeners))}
                                {:nodes (count nodes)}))))

;basic mvp - nodes
(let [
      blueprints
      {:node/attribute (->static)}

      triggers
      (->update [] "node/123" :node/attribute "val")

      expected
      {:created        {:created 0 :data 0 :modified 0 :version 1}
       :id             {:created 0 :data "node/123" :modified 0 :version 1}
       :modified       {:created 0 :data 0 :modified 0 :version 1}
       :type           {:created 0 :data "node" :modified 0 :version 1}
       :version        {:created 0 :data 1 :modified 0 :version 1}
       :node/attribute {:created 0 :data "val" :modified 0 :version 1}}]

  (test-graph blueprints
              triggers
              expected
              :post-fn (fn [graph] (get! graph {:node "node/123"}))))

;basic mvp.. much cleaner :)
(let [
      blueprints
      {:node/attribute (->static)}

      triggers
      (->update [] "node/123" :node/attribute "val")

      expected
      {"node/123" {:node/attribute "val"}}]

  (test-graph blueprints
              triggers
              expected
              ;:async false
              ;:log-level :info
              )
  (test-graph-steps blueprints
                    triggers
                    '(["node/123" :node/attribute]
                      ["node/123" :created]
                      ["node/123" :version]
                      ["node/123" :id]
                      ["node/123" :type]
                      ["node/123" :modified])
                    ;:async false
                    ;:log-level :info
                    :post-fn (comp distinct
                                   (partial map (juxt :node :attribute))
                                   (partial map :args)
                                   (partial map :trigger)))
  (test-graph-async-steps blueprints
                          triggers
                          '(["node/123" :node/attribute]
                            ["node/123" :created]
                            ["node/123" :version]
                            ["node/123" :id]
                            ["node/123" :type]
                            ["node/123" :modified])
                          ;:async false
                          ;:log-level :info
                          :post-fn (comp distinct
                                         (partial map (juxt :node :attribute))
                                         (partial map :args)
                                         (partial map :trigger))))

;2 nodes
(let [
      blueprints
      {:node/attribute (->static)}

      triggers
      (-> []
          (->update "node/1" :node/attribute "val1")
          (->update "node/2" :node/attribute "val2"))

      expected
      {"node/1" {:node/attribute "val1"}
       "node/2" {:node/attribute "val2"}}]

  (test-graph blueprints triggers expected))

;2 attributes
(let [
      blueprints
      {:node/attribute1 (->static)
       :node/attribute2 (->static)}

      triggers
      (-> []
          (->update "node/123" :node/attribute1 "val1")
          (->update "node/123" :node/attribute2 "val2"))

      expected
      {"node/123" {:node/attribute1 "val1" :node/attribute2 "val2"}}]

  (test-graph blueprints triggers expected))

;locally evaluated attribute
(let [
      blueprints
      {:node/attribute  (->static)
       :node/has-value? (->dynamic some? [[:node/attribute]])}

      triggers
      (->update [] "node/123" :node/attribute "val")

      expected
      {"node/123" {:node/attribute "val" :node/has-value? true}}]

  (test-graph blueprints triggers expected))

;locally evaluated attribute from dynamic binding
(let [
      blueprints
      {:node/default (->static)
       :node/val     (->dynamic #(or %1 %2) [[:val] [:node/default]])}

      triggers
      (-> []
          (->update "node/123" :node/default 1)
          (->eval "node/123" :node/val {[:val] 2}))

      expected
      {"node/123" {:node/default 1 :node/val 2}}]

  (test-graph blueprints triggers expected))

;sanity - dynamic binding doesn't participate in listener deduction
(let [
      blueprints
      {:node/static  (->static)
       :node/dynamic (->dynamic (fn [& args] args) [[:val] [:node/static]])}]

  (fact
    (get-in (deduce-listeners blueprints) [:node/static :listeners])
    => [[:node/dynamic]]))

;multi-sourced, locally evaluated attribute
(let [
      blueprints
      {:node/val1 (->static)
       :node/val2 (->static)
       :node/sum  (->dynamic (fnil + 0 0) [[:node/val1] [:node/val2]])}

      triggers
      (-> []
          (->update "node/123" :node/val1 1)
          (->update "node/123" :node/val2 2))

      expected
      {"node/123" {:node/val1 1 :node/val2 2 :node/sum 3}}]

  (test-graph blueprints triggers expected))

;multiple notification paths
(let [
      blueprints
      {:node/val   (->static)
       :node/val+1 (->dynamic inc [[:node/val]])
       :node/val-1 (->dynamic dec [[:node/val]])}

      triggers
      (->update [] "node/123" :node/val 4)

      expected
      {"node/123" {:node/val 4 :node/val+1 5 :node/val-1 3}}]

  (test-graph blueprints triggers expected))

;attributes composition
(let [
      blueprints
      {:node/val      (->static)
       :node/val+1    (->dynamic inc [[:node/val]])
       :node/val+1>0? (->dynamic pos? [[:node/val+1]])}

      triggers
      (->update [] "node/123" :node/val 0)

      expected
      {"node/123" {:node/val 0 :node/val+1 1 :node/val+1>0? true}}]

  (test-graph blueprints triggers expected))

;sanity - sources -> listeners
(fact "deduce-listeners"
      (let [
            blueprints
            {:a/val (->static)
             :a/->b (->ref :b/->a)
             :b/->a (->ref :a/->b)
             :b/val (->dynamic identity [[:b/->a :a/val]])}]

        (->> (deduce-listeners blueprints)
             (filter-vals :listeners)
             (map-vals :listeners)))

      => {:a/val '([:a/->b :b/val])
          :b/->a '([:b/val])})

;1-to-1 refs, simple ('graph neighborhood' evaluated attribute):
; b.val = a.val + 1
; link nodes 'a' -> 'b', and set a.val = 1, in any order! to produce a consistent result!!
(let [
      blueprints
      {:a/val (->static)
       :a/->b (->ref :b/->a)
       :b/->a (->ref :a/->b)
       :b/val (->dynamic (fnil inc 0) [[:b/->a :a/val]])}

      permutation-of-2-triggers-out-of-4-possible-permutations!
      (let [a->b_or_b->a (rand-nth [(->link [] "a/aaa" :a/->b "b/bbb" :b/->a {})
                                    (->link [] "b/bbb" :b/->a "a/aaa" :a/->b {})])
            a:val=2 (->update [] "a/aaa" :a/val 2)]

        (shuffle
          (concat a->b_or_b->a a:val=2)))

      consistent-outcome!!
      {"a/aaa" {:a/->b "b/bbb" :a/val 2}
       "b/bbb" {:b/->a "a/aaa" :b/val 3}}]

  (test-graph blueprints
              permutation-of-2-triggers-out-of-4-possible-permutations!
              consistent-outcome!!
              ;:async false
              ;:log-level :info
              ))

;1-to-1 refs, more elaborated
; (multi-sourced, multiple notification paths, attributes composition, 'graph neighborhood' evaluations):
; b.val = a.val + 1
; c.val = b.val * a.val
; link nodes 'a' -> 'b' -> 'c', and set a.val = 1, in any order! to produce a consistent result!!
(let [
      blueprints
      {:a/val (->static)
       :a/->b (->ref :b/->a)

       :b/->a (->ref :a/->b)
       :b/->c (->ref :c/->b)
       :b/val (->dynamic (fnil inc 0) [[:b/->a :a/val]])

       :c/->b (->ref :b/->c)
       :c/val (->dynamic (fnil * 0 0) [[:c/->b :b/val] [:c/->b :b/->a :a/val]])}

      permutation-of-3-triggers-out-of-12-possible-permutations!
      (let [a->b_or_b->a (rand-nth [(->link [] "a/aaa" :a/->b "b/bbb" :b/->a {})
                                    (->link [] "b/bbb" :b/->a "a/aaa" :a/->b {})])
            b->c_or_c->b (rand-nth [(->link [] "b/bbb" :b/->c "c/ccc" :c/->b {})
                                    (->link [] "c/ccc" :c/->b "b/bbb" :b/->c {})])
            a:val=2 (->update [] "a/aaa" :a/val 2)]

        (shuffle
          (concat a->b_or_b->a
                  a:val=2
                  b->c_or_c->b)))

      consistent-outcome!!
      {"a/aaa" {:a/->b "b/bbb" :a/val 2}
       "b/bbb" {:b/->a "a/aaa" :b/->c "c/ccc" :b/val 3}
       "c/ccc" {:c/->b "b/bbb" :c/val 6}}]

  (test-graph blueprints
              permutation-of-3-triggers-out-of-12-possible-permutations!
              consistent-outcome!!))

;1-to-many refs:
; link parent node with child nodes (in any direction), setting a transitive value on the parent mid-way
; then link some child nodes (in any direction) to another parent
(let [
      blueprints
      {
       :parent/val      (->static)
       :parent/children (->refs :child/parent)

       :child/parent    (->ref :parent/children)
       :child/val       (->dynamic identity [[:child/parent :parent/val]])}

      triggers
      (-> []
          (->link "parent/pp" :parent/children "child/cc1" :child/parent {})
          (->link "child/cc2" :child/parent "parent/pp" :parent/children {})
          (->update "parent/pp" :parent/val true)
          (->link "parent/pp" :parent/children "child/cc3" :child/parent {})
          (->link "child/cc4" :child/parent "parent/pp" :parent/children {})
          (->link "parent/other" :parent/children "child/cc2" :child/parent {})
          (->link "child/cc4" :child/parent "parent/other" :parent/children {}))

      expected
      {"child/cc1"    {:child/parent "parent/pp" :child/val true}
       "child/cc2"    {:child/parent "parent/other" :child/val nil}
       "child/cc3"    {:child/parent "parent/pp" :child/val true}
       "child/cc4"    {:child/parent "parent/other" :child/val nil}
       "parent/other" {:parent/children '("child/cc2" "child/cc4")}
       "parent/pp"    {:parent/children '("child/cc1" "child/cc3") :parent/val true}}]

  (test-graph blueprints triggers expected))

;large graph:
; link parent node with 5000 child nodes (in any direction), setting a transitive value on the parent at a random point
; assert that 5000 child nodes in the resulting graph are set with the parent transitive value
; takes ~10s for the two runs - sync + async :(
(time (let [
            blueprints
            {:parent/val      (->static)
             :parent/children (->refs :child/parent)
             :child/parent    (->ref :parent/children)
             :child/val       (->dynamic identity [[:child/parent :parent/val]])}

            triggers
            (shuffle (reduce #(apply ->link %1 %2)
                             (->update [] "parent/pp" :parent/val true)
                             (map #(let [child-node (str "child/cc" %)]
                                     (if (even? %)
                                       [child-node :child/parent "parent/pp" :parent/children {}]
                                       ["parent/pp" :parent/children child-node :child/parent {}]))
                                  (range 5000))))]

        (test-graph blueprints
                    triggers
                    5000
                    ;:async false
                    ;:log-level :error
                    :post-fn (fn [graph]
                               #_(->> {:node "parent/pp" :attribute {:sources [[:parent/children :child/val]]}}
                                      (get-sources-data! graph)
                                      (first)
                                      (filter some?)
                                      (count))
                               #_(count (str graph))
                               (->> (get-refs graph "parent/pp" :parent/children)
                                    (map #(get! graph % :child/val))
                                    (filter :data)
                                    (count))))))

;notifications
(let [clock (atom 1)
      tick-fn #(swap! clock inc)
      blueprints
      {:a/->b            (->ref :b/->a)

       ;fires :discovery on change (once)
       ;fires :visit on visit (2 per first trigger, 1 more per each followup trigger = 4 in total)
       ;fires :ping on change (once)
       :b/->a            (->fire-event (->ref :a/->b) :ping)

       ;listens to change in :b/->a specifically (1)
       :b/links          (->on-change (->dynamic (fnil inc 0) [[:this]]) [[:b/->a]])

       ;listens to :boot events (1 - only triggers on first :boot)
       ;fires :ping (1)
       :b/boot-time      (->fire-event (->on-boot (->dynamic tick-fn [])) :ping)

       ;listens to :discovery events (1)
       ;first :ping (1)
       :b/last-discovery (->fire-event (->on-discovery (->dynamic tick-fn [])) :ping)

       ;listens to :discovery events (1)
       :b/discoveries    (->on-event (->dynamic (fnil inc 0) [[:this]]) :discovery)

       ;listens to :visit events
       ;fires :ping (4)
       :b/last-visit     (->fire-event (->on-visit (->dynamic tick-fn [])) :ping)

       ;listens to :visit events (4)
       :b/visits         (->on-event (->dynamic (fnil inc 0) [[:this]]) :visit)

       ;listens to :modified events
       ;therefore does NOT fire :modified
       :b/last-modified  (->on-event (->dynamic tick-fn []) :modified)

       ;listens to :visit events (20)
       :b/mods           (->on-event (->dynamic (fnil inc 0) [[:this]]) :modified)

       ;listens to :ping events (1 per :b/->a, 1 per :b/boot-time, 1 per :b/last-discovery, 4 per :b/last-visit = 7)
       :b/pings          (->on-event (->dynamic (fnil inc 0) [[:this]]) :ping)
       }

      triggers
      (-> []
          (->link "a/aaa" :a/->b "b/bbb" :b/->a {})
          (->link "a/aaa" :a/->b "b/bbb" :b/->a {})
          (->link "a/aaa" :a/->b "b/bbb" :b/->a {}))

      expected
      {"a/aaa" {:a/->b "b/bbb"}
       "b/bbb" {:b/->a            "a/aaa"
                :b/boot-time      3
                :b/last-discovery 24
                :b/last-visit     32
                :b/last-modified  35
                :b/mods           20
                :b/discoveries    1
                :b/links          1
                :b/visits         4
                :b/pings          7}}]

  (test-graph blueprints
              triggers
              expected
              ;:sync false
              ;:log-level :info
              :tear-down-fn #(reset! clock 1)))

;sources.. many-to-many.. dynamic refs.. with meta.. with notifications back and forth..
(let [
      blueprints
      {:aaa/bsources     (->refs :bbb/->aaa's)
       :aaa/csources     (->refs :ccc/->aaa's)
       :aaa/sources      (->dynamic merge [[:aaa/bsources] [:aaa/csources]])
       :aaa/eval-bbb+ccc (->dynamic (fn [[{bdat :node}] [{cdat :node}]] [bdat cdat]) [[:aaa/bsources :bbb/val] [:aaa/csources :ccc/val]])
       :aaa/data         (->static)

       :bbb/data         (->static)
       :bbb/val          (->static)
       :bbb/eval-aaa     (->dynamic (partial apply (fnil inc -1)) [[:bbb/->aaa's :aaa/data]])
       :bbb/->aaa's      (-> (->refs :aaa/bsources)
                             (->make-dynamic :node [[:bbb/data]])
                             (->with-meta #(do {:source-node         %
                                                :symmetric-attribute :bbb/->aaa's
                                                :extra               {}})))

       :ccc/data         (->static)
       :ccc/val          (->static)
       :ccc/eval-aaa     (->dynamic (partial map str) [[:ccc/->aaa's :aaa/data]])
       :ccc/->aaa's      (-> (->refs :aaa/csources)
                             (->make-dynamic :nodes [[:ccc/data]])
                             (->with-meta #(do {:source-node         %
                                                :symmetric-attribute :ccc/->aaa's
                                                :extra               {}})))}
      triggers
      (-> []
          (->update "bbb/123" :bbb/data {:node "aaa/123"})
          (->update "ccc/123" :ccc/data {:nodes ["aaa/123" "aaa/456"]})
          (->update "bbb/123" :bbb/val {:node "aaa/123"})
          (->update "ccc/123" :ccc/val {:node "aaa/123"})
          (->update "aaa/123" :aaa/data 1))

      expected
      {"aaa/123" {:aaa/bsources     '("bbb/123")
                  :aaa/csources     '("ccc/123")
                  :aaa/data         1
                  :aaa/eval-bbb+ccc ["aaa/123" "aaa/123"]
                  :aaa/sources      '("bbb/123" "ccc/123")}
       "aaa/456" {:aaa/csources     '("ccc/123")
                  :aaa/eval-bbb+ccc [nil "aaa/123"]
                  :aaa/sources      '("ccc/123")}
       "bbb/123" {:bbb/->aaa's  '("aaa/123")
                  :bbb/data     {:node "aaa/123"}
                  :bbb/eval-aaa 2
                  :bbb/val      {:node "aaa/123"}}
       "ccc/123" {:ccc/->aaa's  '("aaa/123" "aaa/456")
                  :ccc/data     {:nodes ["aaa/123" "aaa/456"]}
                  :ccc/eval-aaa '("1" "")
                  :ccc/val      {:node "aaa/123"}}}]

  (test-graph blueprints
              triggers
              expected
              :post-fn (fn [graph]
                         (map-vals (fn [node]
                                     (cond-> node
                                             (contains? node :aaa/sources)
                                             (update :aaa/sources #(map :id (vals %)))))
                                   (default-post-fn graph)))))

;crawler prototype - how cute!
; but takes ~5s for the two runs - sync + async :(
(time (letfn [
              (calc-min-crawl-depth [sources-depths]
                (inc (apply min sources-depths)))

              (parse-url [id]
                (second (clojure.string/split id #"/" 2)))

              (fetch [url depth]
                (when (and url ((fnil <= Integer/MAX_VALUE) depth 4))
                  (timbre/info "discovered" depth url)
                  (let [http-get (with-callback http/get :callback :body)]
                    (http-get url :callback))))

              (href->web-resource-id [url href]
                (cond->> href
                         (not (clojure.string/starts-with? href "http")) (str url)
                         :to-web-resource (str "web-resource/")))
              (extract-hrefs [html url]
                (when html
                  (->> (re-seq #"href=\"([^\"]*)" html)
                       (map second)
                       (map (partial href->web-resource-id url))
                       (take 1))))

              (post-fn [graph] "just to beautify the result a bit.."
                (if (instance? Throwable graph)
                  (.printStackTrace graph)
                  (map (fn [[id web-resource]]
                         (-> {:url id}
                             (assoc :depth (get-in web-resource [:web-resource/min-crawl-depth :data]))
                             (assoc :sources (map (comp (juxt :id :via) second) (get-in web-resource [:web-resource/sources :data])))
                             (assoc :hrefs (map (comp :id second) (get-in web-resource [:web-resource/hrefs :data])))))
                       (:nodes graph))))]

        (let [
              blueprints
              {:web-resource/min-crawl-depth (->dynamic calc-min-crawl-depth [[:web-resource/sources :web-resource/min-crawl-depth]])
               :web-resource/url             (-> (->dynamic parse-url [[:id]]))
               :web-resource/html            (-> (->dynamic fetch [[:web-resource/url] [:web-resource/min-crawl-depth]])
                                                 (->async)
                                                 (->tts "14d"))
               :web-resource/hrefs           (-> (->refs :web-resource/sources)
                                                 (->make-dynamic extract-hrefs [[:web-resource/html] [:web-resource/url]])
                                                 (->with-meta (constantly {:via "(via href)"})))
               :web-resource/sources         (->refs :web-resource/hrefs)}

              triggers
              (-> []
                  ;(->update "web-resource/http://google.com" :web-resource/url "http://google.com")
                  (->update "web-resource/http://google.com" :web-resource/min-crawl-depth 0))

              expected
              '({:depth   0
                 :url     "web-resource/http://google.com"
                 :hrefs   ("web-resource/http://www.google.co.il/imghp?hl=iw&tab=wi")
                 :sources ()}
                {:depth   1
                 :url     "web-resource/http://www.google.co.il/imghp?hl=iw&tab=wi"
                 :hrefs   ("web-resource/https://www.google.co.il/webhp?tab=iw")
                 :sources (["web-resource/http://google.com" "(via href)"])}
                {:depth   2
                 :url     "web-resource/https://www.google.co.il/webhp?tab=iw"
                 :hrefs   ("web-resource/https://www.google.co.il/imghp?hl=iw&tab=wi")
                 :sources (["web-resource/http://www.google.co.il/imghp?hl=iw&tab=wi" "(via href)"]
                           ["web-resource/https://www.google.co.il/imghp?hl=iw&tab=wi" "(via href)"])}
                {:depth   3
                 :url     "web-resource/https://www.google.co.il/imghp?hl=iw&tab=wi"
                 :hrefs   ("web-resource/https://www.google.co.il/webhp?tab=iw")
                 :sources (["web-resource/https://www.google.co.il/webhp?tab=iw" "(via href)"])})]

          (test-graph blueprints
                      triggers
                      expected
                      ;:async false
                      ;:log-level :info
                      :mock-http false
                      :post-fn post-fn))))

;altogether now
(let [
      blueprints
      {;car
       :car/colors       (->static)
       :car/has-colors?  (->dynamic (comp not empty?) [[:car/colors]])
       :car/num-colors   (->dynamic count [[:car/colors]])
       :car/owner        (->ref :owner/car)
       :car/passengers   (->refs :passenger/car)
       ;owner
       :owner/name       (->static)
       :owner/car        (->ref :car/owner)
       ;passenger
       :passenger/name   (->static)
       :passenger/car    (->ref :car/passengers)
       :passenger/owner? (->dynamic #(and (some? %1) (= %1 %2))
                                    [[:passenger/name] [:passenger/car :car/owner :owner/name]])}
      triggers
      (-> []
          (->update "car/123" :car/colors ["blue" "pink"])
          (->update "owner/aaa" :owner/name "aaa")
          (->link "car/123" :car/owner "owner/aaa" :owner/car {})
          (->link "car/123" :car/owner "owner/bbb" :owner/car {})
          (->update "passenger/xxx" :passenger/name "aaa")
          (->link "car/123" :car/passengers "passenger/xxx" :passenger/car {})
          (->link "car/123" :car/passengers "passenger/yyy" :passenger/car {})
          (->link "car/123" :car/passengers "passenger/zzz" :passenger/car {})
          (->link "passenger/yyy" :passenger/car "car/999" :car/passengers {})
          (->update "passenger/yyy" :passenger/name "bbb")
          (->update "passenger/zzz" :passenger/name "ccc")
          (->link "car/123" :car/owner "owner/aaa" :owner/car {}))

      expected
      {"car/123"       {:car/colors      ["blue" "pink"]
                        :car/has-colors? true
                        :car/num-colors  2
                        :car/owner       "owner/aaa"
                        :car/passengers  '("passenger/xxx" "passenger/zzz")}
       "car/999"       {:car/passengers '("passenger/yyy")}
       "owner/aaa"     {:owner/car "car/123" :owner/name "aaa"}
       "owner/bbb"     {:owner/car nil}
       "passenger/xxx" {:passenger/car    "car/123"
                        :passenger/name   "aaa"
                        :passenger/owner? true}
       "passenger/yyy" {:passenger/car    "car/999"
                        :passenger/name   "bbb"
                        :passenger/owner? false}
       "passenger/zzz" {:passenger/car    "car/123"
                        :passenger/name   "ccc"
                        :passenger/owner? false}}]

  (test-graph blueprints triggers expected))
