(ns tidb.monotonic
  "Establishes a collection of integers identified by keys. Monotonically
  increments individual keys via read-write transactions, and reads keys in
  small groups. We verify that the order of transactions implied by each key
  are mutually consistent; e.g. no transaction can observe key x increase, but
  key y decrease."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle :as cycle]
            [tidb.sql :as c :refer :all]))

(defn read-key
  "Read a specific key's value from the table. Missing values are represented
  as -1."
  [c test k]
  (-> (c/query c [(str "select (val) from cycle where "
                       (if (:use-index test) "pk" "sk") " = ?")
                  k])
      first
      (:val -1)))

(defn read-keys
  "Read several keys values from the table, returning a map of keys to values."
  [c test ks]
  (->> (map (partial read-key c test) ks)
       (zipmap ks)
       (into (sorted-map))))
  ;(zipmap ks (map (partial read-key c test) ks)))

(defrecord IncrementClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists cycle
                        (pk  int not null primary key,
                         sk  int not null,
                         val int)"])
      (when (:use-index test)
        (c/create-index! conn ["create index cycle_sk_val on cycle (sk, val)"]))))

  (invoke! [this test op]
    (c/with-txn op [c conn]
    ;(let [c conn]
      (case (:f op)
        :read (let [v (read-keys c test (shuffle (keys (:value op))))]
                (assoc op :type :ok, :value v))
        :inc (let [k (:value op)]
               (if (:update-in-place test)
                 ; Update directly
                 (do (when (= [0] (c/execute!
                                    c [(str "update cycle set val = val + 1"
                                            " where pk = ?") k]))
                       ; That failed; insert
                       (c/insert! c "cycle" {:pk k, :sk k, :val 0}))
                     ; We can't place any constraints on the values since we
                     ; didn't read anything
                     (assoc op :type :ok, :value {}))

                 ; Update via separate r/w
                 (let [v (read-key c test k)]
                   (if (= -1 v)
                     (c/insert! c "cycle" {:pk k, :sk k, :val 0})
                     (c/update! c "cycle" {:val (inc v)},
                                [(str (if (:use-index test) "sk" "pk") " = ?")
                                 k]))
                   ; The monotonic value constraint isn't actually enough to
                   ; capture all the ordering dependencies here: an increment
                   ; from x->y must fall after every read of x, and before
                   ; every read of y, but the monotonic order relation can only
                   ; enforce one of those. We'll return the written value here.
                   ; Still better than nothing.
                   (assoc op :type :ok :value {k (inc v)})))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn reads [key-count]
  (fn [] {:type  :invoke
          :f     :read
          :value (-> (range key-count)
                     ;util/random-nonempty-subset
                     (zipmap (repeat nil)))}))

(defn incs [key-count]
  (fn [] {:type :invoke,
          :f :inc
          :value (rand-int key-count)}))

(defn workload
  [opts]
  (let [key-count 8]
    {:client (IncrementClient. nil)
     :checker (checker/compose
                {:cycle (cycle/checker
                          ;cycle/monotonic-key-orders)
                          (cycle/combine cycle/monotonic-key-orders
                                         cycle/process-orders))
                 :timeline (timeline/html)})
     :generator (->> (gen/mix [(incs key-count)
                               (reads key-count)]))}))
