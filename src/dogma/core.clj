(ns dogma.core
  (:require [clojure.core.async :as async]
            [org.httpkit.client :as http]
            [taoensso.timbre :as timbre])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (java.time Duration)))

;general util

(defn map-vals [f m]
  (->> m
       (map (fn [[k v]] [k (f v)]))
       (into {})))

(defn filter-vals [f m]
  (->> m
       (filter (comp f second))
       (into {})))

(defn collify [arg]
  (cond (coll? arg) arg
        arg [arg]))

(defn get-time-ms [] (System/currentTimeMillis))

;eg. 3d or 10m or 1s..
(defn duration-in-millis [duration-str]
  (let [normalized-duration-str (clojure.string/upper-case duration-str)]
    (cond->> normalized-duration-str
             (not (clojure.string/includes? normalized-duration-str "D")) (str \T)
             :prefix-P (str \P)
             :parse (Duration/parse)
             :in-ms (.toMillis))))

(defmacro with-altered-var-root [v val body]
  `(let [original-val# (var-get ~v)]
     (try
       (alter-var-root ~v (constantly ~val))
       ~body
       (finally
         (alter-var-root ~v (constantly original-val#))))))

;async util

(defmacro go-wrap?
  [async? & body]
  `(if ~async?
     (async/go ~@body)
     ~@body))

(defn put-n-close! [chan val]
  (async/put! chan val (fn [& args] (async/close! chan))))

(defn safely-put-n-close! [chan val]
  (if (some? val)
    (put-n-close! chan val)
    (async/close! chan)))

(defn with-callback [f & {:keys [callback err-callback on-caller?]
                          :or   {callback     identity
                                 err-callback identity
                                 on-caller?   true}}]
  (fn [& args]
    (if (some #{:callback :err-callback} args)
      (let [out (async/promise-chan)
            args' (replace {:callback     (comp (partial safely-put-n-close! out) callback)
                            :err-callback (comp (partial safely-put-n-close! out) err-callback)} args)]
        (go-wrap? (not on-caller?) (apply f args'))
        out)
      (go-wrap? (not on-caller?) (apply f args)))))

(defn async-resolver [coll]
  (if (some (partial instance? ManyToManyChannel) coll)
    (let [out (async/chan)]
      (letfn [(resolve-into [resolved [elem & unresolved]]
                (if (instance? ManyToManyChannel elem)
                  (async/take! elem #(resolve-into resolved (conj unresolved %)))
                  (if (not-empty unresolved)
                    (resolve-into (conj resolved elem) unresolved)
                    (put-n-close! out (conj resolved elem)))))]
        (resolve-into [] coll))
      out)
    coll))

(defn async-resolving-lazy-seq [[head & tail]]
  (lazy-seq
    (when head
      (if (instance? ManyToManyChannel head)
        (let [head' (async/<!! head)]
          (if (seq? head')
            (lazy-cat (async-resolving-lazy-seq head')
                      (async-resolving-lazy-seq tail))
            (cons head' (async-resolving-lazy-seq tail))))
        (cons head (async-resolving-lazy-seq tail))))))

(defn ch->lazy-seq [ch]
  (lazy-seq
    (when-let [head (async/<!! ch)]
      (cons head (ch->lazy-seq ch)))))

(defn async-weaver [f]
  (fn [& args]
    (if (some (partial instance? ManyToManyChannel) args)
      (let [out (async/promise-chan)]
        (letfn [(safely-exec [f args]
                  (try (if-let [thrown (first (filter (partial instance? Throwable) args))]
                         thrown (apply f args))
                       (catch Throwable t t)))
                (weave [f args]
                  (let [val (safely-exec f args)]
                    (if (instance? ManyToManyChannel val)
                      (async/take! val #(weave identity [%]))
                      (safely-put-n-close! out val))))]
          (async/take! (async-resolver args) (partial weave f)))
        out)
      (if-let [t (first (filter (partial instance? Throwable) args))]
        (throw t) (apply f args)))))

(defmacro async->
  "Threads the expr through the forms. Inserts x as the
  second item in the first form, making a list of it if it is not a
  list already. If there are more forms, inserts the first form as the
  second item in second form, etc."
  {:added "1.0"}
  [x & forms]
  (loop [x x, forms forms]
    (if forms
      (let [form (first forms)
            threaded (if (seq? form)
                       (with-meta `((async-weaver ~(first form)) ~x ~@(next form)) (meta form))
                       (list (async-weaver form) x))]
        (recur threaded (next forms)))
      x)))

(defmacro async->>
  "Threads the expr through the forms. Inserts x as the
  last item in the first form, making a list of it if it is not a
  list already. If there are more forms, inserts the first form as the
  last item in second form, etc."
  {:added "1.1"}
  [x & forms]
  (loop [x x, forms forms]
    (if forms
      (let [form (first forms)
            threaded (if (seq? form)
                       (with-meta `((async-weaver ~(first form)) ~@(next form) ~x) (meta form))
                       (list (async-weaver form) x))]
        (recur threaded (next forms)))
      x)))

(defmacro cond-async->
  "Takes an expression and a set of test/form pairs. Threads expr (via ->)
  through each form for which the corresponding test
  expression is true. Note that, unlike cond branching, cond-> threading does
  not short circuit after the first true test expression."
  {:added "1.5"}
  [expr & clauses]
  (assert (even? (count clauses)))
  (let [g (gensym)
        steps (map (fn [[test step]] `(if ~test (async-> ~g ~step) ~g))
                   (partition 2 clauses))]
    `(let [~g ~expr
           ~@(interleave (repeat g) (butlast steps))]
       ~(if (empty? steps)
          g
          (last steps)))))

(defmacro cond-async->>
  "Takes an expression and a set of test/form pairs. Threads expr (via ->>)
  through each form for which the corresponding test expression
  is true.  Note that, unlike cond branching, cond->> threading does not short circuit
  after the first true test expression."
  {:added "1.5"}
  [expr & clauses]
  (assert (even? (count clauses)))
  (let [g (gensym)
        steps (map (fn [[test step]] `(if ~test (async->> ~g ~step) ~g))
                   (partition 2 clauses))]
    `(let [~g ~expr
           ~@(interleave (repeat g) (butlast steps))]
       ~(if (empty? steps)
          g
          (last steps)))))

(defn map-async [& args]
  (async-resolver (apply map args)))

(defn mapcat-async [& args]
  (async->> (apply map-async args)
            (apply concat)))

(def async? false)
(defn set-async [enabled?]
  (alter-var-root (var async?) (constantly enabled?)))

;sepl

(defn ->trigger
  ([type args] {:type type :args args})
  ([triggers type args] (conj triggers {:type type :args args})))
(def ->step ->trigger)
(defn add-triggers [new-triggers triggers {:keys [depth] :as source-trigger} ->ser-args]
  (->> new-triggers
       (map #(update % :args ->ser-args))
       (map #(assoc % :depth ((fnil inc 0) depth)))
       (into triggers)))

(defn update-tracker
  ([] (update-tracker nil nil))
  ([tracker {:keys [type depth] :as trigger}]
   (if depth
     (-> tracker
         (update :iterations inc)
         (update :max-depth max depth)
         (update-in [:flows type :iterations] (fnil inc 0))
         (update-in [:flows type :max-depth] (fnil max 0) depth)
         (update-in [:flows type :depths] (fnil conj #{}) depth))
     {:iterations 0 :trigger trigger :flows {} :max-depth 0 :start-time (System/currentTimeMillis)})))

(defn assert-sepl-computation-thresholds
  [{:keys [iterations max-depth start-time] :as tracker} max-iterations max-computation-depth max-duration]
  (assert (<= iterations max-iterations) tracker)
  (assert (<= max-depth max-computation-depth) tracker)
  (assert (<= (- (System/currentTimeMillis) start-time) max-duration) tracker))

;trigger => [type, args]
;side-effect [state, args] => outcome (pull data from state / calc new state; ie read/write)
;process [args, outcome] => triggers
;flow => [type, side-effect, process]

;the loop goes: trigger -> side-effect -> process -> and so on and on..

;base algorithm for execution [triggers, state] =>
;loop( [[type, args] & more-triggers], state)
;  when no more triggers return state
;  type => [side-effect, process]
;  outcome=side-effect(state, args)
;  if process
;    recur( process(args, outcome) + more-triggers, state )   ;<=read-only
;  else recur( more-triggers, outcome )                       ;<=write-only
(defn sepl [flows triggers state
            & {:keys [->ser-args ->deser-args max-iterations max-depth max-duration]
               :or   {->ser-args     identity
                      ->deser-args   identity
                      max-iterations Integer/MAX_VALUE
                      max-depth      Integer/MAX_VALUE
                      max-duration   Integer/MAX_VALUE}}]
  (go-wrap? async?
            (try (loop [[{:keys [type args depth] :as trigger} & more-triggers] (apply list triggers)
                        state state
                        tracker (update-tracker)]
                   (when-not depth
                     (when-let [init-trigger (:trigger tracker)]
                       (timbre/info (:type init-trigger) "-done->" (select-keys tracker [:iterations :max-depth #_:flows]))))
                   (assert-sepl-computation-thresholds tracker max-iterations max-depth max-duration)
                   (if (nil? trigger)
                     state
                     (let [{:keys [side-effect-fn process-fn]} (get flows type)
                           args' (->deser-args args)
                           outcome (when side-effect-fn (side-effect-fn state args'))
                           chan? (instance? ManyToManyChannel outcome)
                           outcome' (cond-> outcome
                                            (and chan? async?) (async/<!)
                                            (and chan? (not async?)) (async/<!!))]
                       (if (instance? Throwable outcome')
                         (throw outcome')
                         (let [triggers' (if process-fn
                                           (-> (process-fn args' outcome')
                                               (add-triggers more-triggers trigger ->ser-args))
                                           more-triggers)
                               state' (if process-fn state outcome')
                               tracker' (update-tracker tracker trigger)]
                           (recur triggers' state' tracker'))))))
                 (catch Throwable t t))))

(defn async-sepl [flows triggers state
                  & {:keys [->ser-args ->deser-args max-iterations max-depth max-duration aot]
                     :or   {->ser-args     identity
                            ->deser-args   identity
                            max-iterations Integer/MAX_VALUE
                            max-depth      Integer/MAX_VALUE
                            max-duration   Integer/MAX_VALUE
                            aot 16}}]
  (let [out (async/chan aot)]
    (async/go
      (try (loop [[{:keys [type args depth] :as trigger} & more-triggers] (apply list triggers)
                  state state
                  tracker (update-tracker)]
             (when-not depth
               (when-let [init-trigger (:trigger tracker)]
                 (timbre/info (:type init-trigger) "-done->" (select-keys tracker [:iterations :max-depth #_:flows]))))
             (assert-sepl-computation-thresholds tracker max-iterations max-depth max-duration)
             (if (nil? trigger)
               (async/close! out)
               (let [{:keys [side-effect-fn process-fn]} (get flows type)
                     args' (->deser-args args)
                     outcome (when side-effect-fn (side-effect-fn state args'))
                     chan? (instance? ManyToManyChannel outcome)
                     outcome' (cond-> outcome
                                      (and chan? async?) (async/<!)
                                      (and chan? (not async?)) (async/<!!))]
                 (if (instance? Throwable outcome')
                   (safely-put-n-close! out outcome')
                   (let [triggers' (if process-fn
                                     (-> (process-fn args' outcome')
                                         (add-triggers more-triggers trigger ->ser-args))
                                     more-triggers)
                         state' (if process-fn state outcome')
                         tracker' (update-tracker tracker trigger)]
                     (async/>! out {:trigger             trigger
                                    :triggers            triggers'
                                    :state               state'
                                    :tracker             tracker'
                                    :side-effect-outcome outcome'})
                     (recur triggers' state' tracker'))))))
           (catch Throwable t
             (put-n-close! out t))))
    out))

;blocking :(
;blocking/parking/callback wrapper?

(defn lazy-sepl [flows triggers state
                 & {:keys [->ser-args ->deser-args max-iterations max-depth max-duration]
                    :or   {->ser-args     identity
                           ->deser-args   identity
                           max-iterations Integer/MAX_VALUE
                           max-depth      Integer/MAX_VALUE
                           max-duration   Integer/MAX_VALUE}}]
  (letfn [(foo [[{:keys [type args depth] :as trigger} :as triggers] state tracker]
            (when-not depth
              (when-let [init-trigger (:trigger tracker)]
                (timbre/info (:type init-trigger) "-done->" (select-keys tracker [:iterations :max-depth #_:flows]))))
            (assert-sepl-computation-thresholds tracker max-iterations max-depth max-duration)
            (when trigger
              (let [{:keys [side-effect-fn process-fn]} (get flows type)
                    args' (->deser-args args)
                    outcome (when side-effect-fn (side-effect-fn state args'))
                    state' (if process-fn state outcome)
                    tracker' (update-tracker tracker trigger)]
                (let [next-steps (async-weaved-goo triggers state' tracker' outcome)]
                  (if (seq? next-steps)
                    next-steps
                    (seq [next-steps]))))))
          (async-weaved-foo [& args] (apply (async-weaver foo) args))
          (goo [[{:keys [type args] :as trigger} & more-triggers] state' tracker' outcome']
            (let [{:keys [process-fn]} (get flows type)
                  args' (->deser-args args)]
              (if (instance? Throwable outcome')
                (throw outcome')
                (let [triggers' (if process-fn
                                  (-> (process-fn args' outcome')
                                      (add-triggers more-triggers trigger ->ser-args))
                                  more-triggers)]
                  (cons {:trigger             trigger
                         :triggers            triggers'
                         :state               state'
                         :tracker             tracker'
                         :side-effect-outcome outcome'}
                        (lazy-seq (async-weaved-foo triggers' state' tracker')))))))
          (async-weaved-goo [& args] (apply (async-weaver goo) args))]
    (foo (apply list triggers) state (update-tracker))))

;execution try-catch on each step
;exception can be accompanied with 'stacktrace', or prev state, etc

;graph attributes

(defn prep-attribute-value [value version data]
  (let [now-ms (get-time-ms)]
    (cond-> value
            (nil? value) (assoc :version version :created now-ms)
            :touch (assoc :modified now-ms)
            :mutate! (assoc :data data))))

(defn gen-ref [node-id attribute meta]
  (merge meta (let [now (get-time-ms)]
                {:id                  node-id
                 :symmetric-attribute attribute
                 :created             now
                 :modified            now})))
(defn sanitize-ref [ref] (dissoc ref :created :modified))
(def ->ref-id (juxt :id :symmetric-attribute))
(defn refs-not= [ref1 ref2] (not= (sanitize-ref ref1) (sanitize-ref ref2)))
(defn lookup-ref
  ([ref-data ref-type node attribute] (lookup-ref ref-data ref-type (gen-ref node attribute {})))
  ([ref-data ref-type ref]
   (if (= ref-type :ref)
     ref-data
     (get ref-data (->ref-id ref)))))

(defn get-ref [refs ref] (get refs (->ref-id ref)))

(defn get-attribute [attributes attribute-id] (get attributes attribute-id))
(defn attribute->node-type [attribute] (namespace attribute))
(defn ->node-type [node-id] (first (clojure.string/split node-id #"/")))
(def default-diff-fn not=)
(defn default-set-fn [old-value new-value] new-value)
(def default-unset-fn (constantly nil))
(def default-eval-fn identity)
(def default-meta-fn (constantly {}))
;(defn ->attribute-get [graph attribute key & [default-value]] (get-in graph [:attributes attribute key] default-value))
;(def ->attribute-diff-fn #(->attribute-get %1 %2 :diff-fn default-diff-fn))
;(def ->attribute-set-fn #(->attribute-get %1 %2 :set-fn default-set-fn))
;(def ->attribute-unset-fn #(->attribute-get %1 %2 :unset-fn default-unset-fn))
;(def ->attribute-symmetry #(->attribute-get %1 %2 :symmetric-attribute))
(def ->ref-attribute? #{:ref :refs})
(def ->attribute-id :id)
(def default-version 1)
(defn get-version [] default-version)
(def default-attribute-flags {:version       default-version
                              :type          :static
                              :value-type    :anything
                              ;:notify-boot? true
                              :notify-events [:modified]
                              :diff-fn       default-diff-fn
                              :set-fn        default-set-fn
                              :unset-fn      default-unset-fn})
(def default-dynamic-attribute-flags {:type       :dynamic
                                      :async?     false
                                      :derefable? false
                                      :eval-fn    default-eval-fn})
(def default-ref-attribute-flags {:type       :ref
                                  :value-type :node
                                  :meta-fn    default-meta-fn})
(def default-refs-attribute-flags {:type     :refs
                                   :set-fn   (fn set-refs-fn [refs ref] (assoc refs (->ref-id ref) ref))
                                   :unset-fn dissoc})
(def default-coll-attribute-flags {:diff-fn  (comp not contains?)
                                   :set-fn   (fnil conj #{})
                                   :unset-fn disj})
(defn ->attribute
  [& {:keys [version type value-type diff-fn set-fn unset-fn listeners] :as args}]
  (merge default-attribute-flags args))
(def ->static ->attribute)
(defn ->dynamic [f sources & {:keys [async? derefable?] :as args}]
  (->> {:notifiers sources :sources sources :eval-fn f}
       (merge default-dynamic-attribute-flags args)
       (seq)
       (apply concat)
       (apply ->attribute)))
(defn ->ref [symmetric-attribute & {:keys [meta-fn] :as args}]
  (->> {:symmetric-attribute symmetric-attribute :diff-fn refs-not=}
       (merge default-ref-attribute-flags args)
       (seq)
       (apply concat)
       (apply ->attribute)))
(defn ->refs [symmetric-attribute & {:keys [] :as args}]
  (->> {}
       (merge default-refs-attribute-flags args)
       (seq)
       (apply concat)
       (apply ->ref symmetric-attribute)))
(defn ->make-dynamic [attr f sources] (merge attr {:eval-fn f :notifiers sources :sources sources}))
(defn ->with-meta [ref-attr meta-fn] (merge ref-attr {:meta-fn meta-fn}))
(defn ->immutable [dynamic-attr] (merge dynamic-attr {:diff-fn (fn [curr-val _] (nil? curr-val))}))
(def ->idempotent ->immutable)
(def ->once ->immutable)
(def ->const ->immutable)
(defn ->async [dynamic-attr] (merge dynamic-attr {:async? true}))
(defn ->derefable [dynamic-attr & [timeout-ms timeout-val]]
  (merge dynamic-attr {:derefable? true :timeout-ms timeout-ms :timeout-val timeout-val}))
(defn ->on-change [dynamic-attr notifiers] (update dynamic-attr :notifiers concat notifiers))
(defn ->tts [dynamic-attr period-str] (merge dynamic-attr {:tts period-str}))
(defn ->fire-event [attr event] (update attr :notify-events conj event))
(defn ->on-event [attr event] (-> attr
                                  (update :on-events conj event)
                                  (update :notify-events (partial remove #{event}))))
(defn ->on-discovery [attr] (->on-event attr :discovery))
(defn ->on-visit [attr] (->on-event attr :visit))
(defn ->on-boot [attr] (->immutable (->on-event attr :boot)))
(defn ->boot [attr] (merge (->on-boot attr) {#_#_:notify-boot? true}))

;graph triggers
(defn ->update [triggers node attribute value]
  (->trigger triggers :update-flow {:node node :attribute attribute :new-value value}))
(defn ->link [triggers source-node source-attribute target-node target-attribute meta]
  (->trigger triggers :link-flow {:node             source-node
                                  :attribute        source-attribute
                                  :target-node      target-node
                                  :target-attribute target-attribute
                                  :meta             meta}))
(defn ->unlink [triggers source-node source-attribute target-node target-attribute]
  (->trigger triggers :unlink-flow {:node             source-node
                                    :attribute        source-attribute
                                    :target-node      target-node
                                    :target-attribute target-attribute}))
(defn ->mutate [triggers node attribute mutate-fn {:keys [data] :as old-value} new-value version]
  (let [data' (mutate-fn data new-value)
        value (prep-attribute-value old-value version data')
        args {:node node :attribute attribute :old-value old-value :new-value new-value :value value}]
    (->trigger triggers :mutate-flow args)))
(defn ->notify [triggers node attribute]
  (->trigger triggers :notify-flow {:node node :attribute attribute}))
(defn ->notify-event
  ([triggers node attribute event] (->notify-event triggers node attribute event nil))
  ([triggers node attribute event event-data]
   (->trigger triggers :notify-event-flow {:node node :attribute attribute :event event :event-data event-data})))
(defn ->eval
  ([triggers node attribute] (->eval triggers node attribute {}))
  ([triggers node attribute external-binding]
   (->trigger triggers :eval-flow {:node node :attribute attribute :external-binding external-binding})))
(defn ->side-effect [triggers node attribute side-effect callback-trigger-fn]
  (->trigger triggers :side-effect-flow {:node                node
                                         :attribute           attribute
                                         :side-effect         side-effect
                                         :callback-trigger-fn callback-trigger-fn}))

;graph side-effects

(defn init-node [node] {} #_{:self (prep-attribute-value nil default-version (gen-ref node :self {}))
                             :id   (prep-attribute-value nil default-version node)
                             :type (prep-attribute-value nil default-version (->node-type node))})

(def query! (fn [query]
              (let [;todo rip mock query/response out when the time comes
                    mock-url (str "http://www.google.com?node=" query)
                    mock-response (init-node query)
                    cb (constantly mock-response)
                    http-get (with-callback http/get :callback cb :on-caller? true)]
                (if async?
                  (http-get mock-url :callback)
                  (deref (http-get mock-url cb))))))

(defn get!
  ([graph {:keys [node attribute]}] (get! graph node (->attribute-id attribute)))
  ([graph node attribute]
   (let [get-or-create #(if % % (init-node node))
         n (async-> (or (get-in graph [:nodes node]) (query! node))
                    (get-or-create))]
     (cond-> n attribute (get attribute)))))

(defn update! [graph {:keys [node attribute value]}]
  (let [attribute-id (->attribute-id attribute)
        curr-node (get! graph node nil)]
    (timbre/info [node attribute-id] "-mutate->" (clojure.core/type value))
    (async->> value
              (assoc curr-node attribute-id)
              (assoc-in graph [:nodes node]))))

(defn get-refs [{:keys [blueprints] :as graph} node attribute]
  (let [type (:type (get-attribute blueprints attribute))]
    (cond-async->> (get! graph node attribute)
                   :data (:data)
                   (= :refs type) (map (comp :id val))
                   (= :ref type) (:id))))

(defn resolve! [graph node bindings paths f]
  (letfn [(stepper! [x bindings path]
            (when x
              (if (coll? x)
                (map-async #(getter! % bindings path) x)
                (getter! x bindings path))))
          (getter! [n bindings [step & steps :as path]]
            (if (contains? bindings path)
              (get bindings path)
              (if steps
                (async-> (get-refs graph n step)
                         (stepper! bindings steps))
                (f n step))))]
    (map-async (partial getter! node bindings) paths)))

(defn get-listeners! [graph {node :node {:keys [listeners]} :attribute}]
  (async->> (resolve! graph node {} listeners #(do [%1 %2]))
            (flatten)
            (filter some?)
            (partition-all 2)))

(defn get-sources-data! [graph {node :node {:keys [sources]} :attribute bindings :external-binding :as args}]
  (let [data (async->> (get! graph args) (:data))]
    (resolve! graph
              node
              (assoc bindings [:this] data)
              sources
              #(async->> (get! graph %1 %2) (:data)))))

(defn get-event-listeners [{{:keys [events-listeners]} :blueprints :as graph} {:keys [node attribute event]}]
  (when-let [node-type (attribute->node-type (->attribute-id attribute))]
    (map (partial vector node) (get-in events-listeners [node-type event]))))

(defn ->notify-events [triggers {:keys [node attribute] :as args} curr-value events]
  (let [attribute-id (->attribute-id attribute)
        event-data {:node node :attribute attribute-id}]
    (reduce #(->notify-event %1 node attribute-id %2 event-data) triggers events)))

;core flows
(def core-flows
  {:update-flow       {:side-effect-fn get!
                       :process-fn     (fn update-fn
                                         [{:keys [node attribute new-value] :as args}
                                          {:keys [data] :as curr-value}]
                                         (let [{:keys [diff-fn set-fn version notify-boot? notify-events on-events]} attribute]
                                           (when (diff-fn data new-value)
                                             (let [attribute-id (->attribute-id attribute)
                                                   triggers '()]
                                               (timbre/info [node attribute-id] "-update->" (clojure.core/type new-value))
                                               (cond-> triggers
                                                       ;(and (nil? curr-value) notify-boot?) (->notify-event node attribute-id :boot {[:id] node})
                                                       (and (nil? curr-value) (not (some #{:boot} on-events))) (->notify-event node attribute-id :boot {[:id] node})
                                                       :mutate (->mutate node attribute-id set-fn curr-value new-value version)
                                                       :notify (->notify node attribute-id)
                                                       :notify-events (->notify-events args curr-value notify-events))))))}
   :link-flow         {:side-effect-fn get!
                       :process-fn     (fn link-fn
                                         [{:keys [node attribute target-node target-attribute meta] :as args}
                                          {:keys [modified data] :as curr-value}]
                                         (let [{:keys [diff-fn set-fn meta-fn type version notify-boot? notify-events on-events]} attribute
                                               ref (gen-ref target-node target-attribute meta)
                                               curr-ref (lookup-ref data type ref)
                                               same-ref? (= (->ref-id ref) (->ref-id curr-ref))
                                               new-ref (cond-> ref same-ref? (assoc :created (:created curr-ref)))
                                               triggers (->notify-event '() target-node target-attribute :visit)]
                                           (if (diff-fn new-ref curr-ref)
                                             (let [attribute-id (->attribute-id attribute)
                                                   unlink-prev-ref? (and curr-ref (= type :ref) (not same-ref?))]
                                               (timbre/info [node attribute-id] "-link->" [target-node target-attribute])
                                               (cond-> triggers
                                                       ;unset prev ref
                                                       unlink-prev-ref? (->unlink (:id curr-ref)
                                                                                  (:symmetric-attribute curr-ref)
                                                                                  node
                                                                                  attribute-id)
                                                       ;(and (nil? curr-value) notify-boot?) (->notify-event node attribute-id :boot {[:id] node})
                                                       (and (nil? curr-value) (not (some #{:boot} on-events))) (->notify-event node attribute-id :boot {[:id] node})
                                                       ;set new ref
                                                       :link-this-side (->mutate node attribute-id set-fn curr-value new-ref version)
                                                       :link-other-side (->link target-node target-attribute node attribute-id (meta-fn node))
                                                       :notify (->notify node attribute-id)
                                                       :notify-discovery (->notify-event target-node target-attribute :discovery)
                                                       :notify-events (->notify-events args curr-value notify-events)))
                                             triggers)))}
   :unlink-flow       {:side-effect-fn get!
                       :process-fn     (fn unlink-fn
                                         [{:keys [node attribute target-node target-attribute] :as args}
                                          {:keys [data] :as curr-value}]
                                         (let [{:keys [unset-fn type version notify-events]} attribute]
                                           (when-let [ref (lookup-ref data type target-node target-attribute)]
                                             (let [attribute-id (->attribute-id attribute)
                                                   triggers '()]
                                               (timbre/info [node attribute-id] "-unlink->" [target-node target-attribute])
                                               (-> triggers
                                                   (->mutate node attribute-id unset-fn curr-value (->ref-id ref) version)
                                                   (->unlink target-node target-attribute node attribute-id)
                                                   (->notify node attribute-id)
                                                   (->notify-events args curr-value notify-events))))))}
   :mutate-flow       {:side-effect-fn update!}
   :notify-flow       {:side-effect-fn get-listeners!
                       :process-fn     (fn notify-flow-fn
                                         [{:keys [node attribute]} listeners]
                                         (let [triggers '()]
                                           (reduce #(do (timbre/info [node (->attribute-id attribute)] "-notify->" (vec %2))
                                                        (apply ->eval %1 %2)) triggers listeners)))}
   :notify-event-flow {:side-effect-fn get-event-listeners
                       :process-fn     (fn notify-event-flow-fn
                                         [{:keys [node attribute event event-data]} listeners]
                                         (let [triggers '()]
                                           (reduce (fn [triggers [n att]]
                                                     (timbre/info [node (->attribute-id attribute)] "-notify-" event "->" [n att])
                                                     (->eval triggers n att event-data))
                                                   triggers
                                                   listeners)))}
   :eval-flow         {:side-effect-fn get-sources-data!
                       :process-fn     (fn eval-flow-fn
                                         [{:keys [node attribute external-binding]} sources-data]
                                         (let [{:keys [eval-fn meta-fn symmetric-attribute type async? derefable?]} attribute
                                               attribute-id (->attribute-id attribute)
                                               side-effect-outcome? (contains? external-binding :side-effect-outcome)
                                               ref? (->ref-attribute? type)
                                               triggers '()]
                                           (if (and (or async? derefable?) (not side-effect-outcome?))
                                             (let [side-effect (apply eval-fn sources-data)]
                                               (->side-effect triggers node attribute-id side-effect
                                                              #(->eval triggers
                                                                       node
                                                                       attribute-id
                                                                       {:side-effect-outcome %})))
                                             (let [outcome (if side-effect-outcome?
                                                             (:side-effect-outcome external-binding)
                                                             (apply eval-fn sources-data))]
                                               (timbre/info [node attribute-id] "-eval->" (clojure.core/type outcome))
                                               (if ref?
                                                 (let [metadata (meta-fn node)]
                                                   (reduce (fn [triggers target-node]
                                                             (->link triggers
                                                                     target-node
                                                                     symmetric-attribute
                                                                     node
                                                                     attribute-id
                                                                     metadata))
                                                           triggers
                                                           (collify outcome)))
                                                 (->update triggers node attribute-id outcome))))))}
   :side-effect-flow  {:side-effect-fn (fn [_ {:keys [attribute side-effect]}]
                                         (let [{:keys [async? derefable? timeout-ms timeout-val]} attribute]
                                           (if derefable?
                                             (if (and timeout-ms timeout-val)
                                               (deref side-effect timeout-ms timeout-val)
                                               (deref side-effect))
                                             side-effect)))
                       :process-fn     (fn side-effect-flow-fn
                                         [{:keys [node attribute callback-trigger-fn]} side-effect-outcome]
                                         (timbre/info [node (->attribute-id attribute)] "-side-effect->" (type side-effect-outcome))
                                         (callback-trigger-fn side-effect-outcome))}})

(defn deduce-listeners [blueprints]
  (letfn [(->symmetric-attribute [attribute]
            (:symmetric-attribute (get-attribute blueprints attribute)))
          (deduce-propagation-paths [blueprints]
            (for [[target {:keys [notifiers]}] blueprints
                  path notifiers]
              (when (every? (partial get-attribute blueprints) path)
                (loop [propagation-paths []
                       [source & steps] (reverse path)]
                  (let [propagation-path (conj (mapv ->symmetric-attribute steps) target)]
                    (cond-> propagation-paths
                            (every? (comp not nil?) propagation-path) (conj [source propagation-path])
                            steps (recur steps)))))))
          (apply-listeners [blueprints [source-attribute propagation-path]]
            (update-in blueprints [source-attribute :listeners] (comp distinct conj) propagation-path))]
    (->> (deduce-propagation-paths blueprints)
         (apply concat)
         (reduce apply-listeners blueprints))))

(defn gen-boot-attributes [blueprints]
  (-> blueprints
      (assoc :id (->boot (->dynamic identity [[:id]])))
      (assoc :type (->dynamic ->node-type [[:id]]))
      (assoc :version (->boot (->dynamic get-version [])))
      (assoc :created (->boot (->dynamic get-time-ms [])))
      (assoc :modified (->on-event (->dynamic get-time-ms []) :modified))
      #_(assoc :self (->boot (->ref :self)))))

(defn get-node-types [blueprints] (filter some? (distinct (map attribute->node-type (keys blueprints)))))

(defn prep-events-listeners [blueprints]
  (let [node-types (get-node-types blueprints)
        events-listeners
        (reduce #(apply update-in %1 %2)
                {}
                (for [{:keys [id on-events] :as attribute} (sort-by (comp namespace :id) (vals blueprints))
                      node-type (or (collify (attribute->node-type id)) node-types)
                      event on-events]
                  [[node-type event] conj id]))]
    (assoc blueprints
      :events-listeners events-listeners
      :node-types node-types)))

(defn fix-events [blueprints]
  (->> blueprints
       (map (fn [[id attribute]]
              (let [on-events (set (:on-events attribute))]
                [id (update attribute :notify-events (partial remove on-events))])))
       (into {})))

(defn fix-ids [blueprints]
  (->> blueprints
       (map (fn [[id attribute]] [id (assoc attribute :id id)]))
       (into {})))

(defn prep-blueprints [blueprints]
  (-> blueprints
      (gen-boot-attributes)
      (fix-ids)
      (fix-events)
      (deduce-listeners)
      (prep-events-listeners)))

(defn init-state [blueprints]
  {:blueprints (prep-blueprints blueprints)
   :nodes      {}})

(defn build-graph [blueprints triggers]
  (let [{:keys [blueprints] :as graph} (init-state blueprints)]
    ;(clojure.pprint/pprint blueprints)
    (letfn [(->ser-args [{:keys [attribute] :as args}]
              (cond-> args
                      (map? attribute) (update :attribute ->attribute-id)))
            (->deser-args [{:keys [attribute] :as args}]
              (update args :attribute (partial get-attribute blueprints)))]
      (sepl core-flows triggers graph
            :->ser-args ->ser-args
            :->deser-args ->deser-args))))

(defn ->steps [blueprints triggers]
  (let [{:keys [blueprints] :as graph} (init-state blueprints)]
    ;(clojure.pprint/pprint blueprints)
    (letfn [(->ser-args [{:keys [attribute] :as args}]
              (cond-> args
                      (map? attribute) (update :attribute ->attribute-id)))
            (->deser-args [{:keys [attribute] :as args}]
              (update args :attribute (partial get-attribute blueprints)))]
      (lazy-sepl core-flows triggers graph
            :->ser-args ->ser-args
            :->deser-args ->deser-args))))

(defn ->async-steps [blueprints triggers]
  (let [{:keys [blueprints] :as graph} (init-state blueprints)]
    ;(clojure.pprint/pprint blueprints)
    (letfn [(->ser-args [{:keys [attribute] :as args}]
              (cond-> args
                      (map? attribute) (update :attribute ->attribute-id)))
            (->deser-args [{:keys [attribute] :as args}]
              (update args :attribute (partial get-attribute blueprints)))]
      (async-sepl core-flows triggers graph
            :->ser-args ->ser-args
            :->deser-args ->deser-args))))

(defn sync-build-graph [blueprints triggers]
  (set-async false)
  (build-graph blueprints triggers))

(defn async-build-graph [blueprints triggers]
  (set-async true)
  (build-graph blueprints triggers))

;insert attribute id into attributes declaration?
;simplify symmetric attribute declaration?
;support non-symmetric edges?
;as-mutations-stream
;conflict resolution
;tts/cache?
