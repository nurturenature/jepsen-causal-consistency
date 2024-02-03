### Jepsen Tests for Causal Consistency

Designed for testing local first systems that use CRDTs and distributed syncing.

[Jepsen](https://github.com/jepsen-io/jepsen) has an established [history](https://jepsen.io/analyses) of testing databases.

These tests have often focused on stronger levels of [consistency](https://jepsen.io/consistency), e.g. snapshot-isolation, linearizability, and serializability.

This project explores using Jepsen to test for [Causal Consistency](https://jepsen.io/consistency/models/causal) with Strong Convergence and atomic transactions ([Monotonic Atomic View](https://jepsen.io/consistency/models/monotonic-atomic-view)).

The tests will use [ElectricSQL](https://electric-sql.com/):
  - transactional causal+ consistency
  - local first
  - active/active SQLite3/PostgreSQL CRDT based sync
  - strong research team

([Current status](doc/electricsql.md) of *early* testing.)

----

### Uses Elle, Jepsen's Checker

#### Adya's Consistent View(PL-2+) as the base consistency model
> Level PL-2+ ensures that a transaction is placed after all transactions that causally affect it, i.e., it provides a notion of “causal consistency”.
> 
>   -- Adya

#### Modifies Elle's consistency model [graph](https://github.com/jepsen-io/elle/blob/main/images/models.png)

Adds strong-session-consistent-view:
  - adds process graph and process variants of anomalies
  - fills in gap between stronger and weaker forms of strong-session consistency models 

----

### Adya Anomalies Expressed

#### Read Your Writes
  - `[:G-single-item-process :cyclic-versions]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]],   :index 1, :time -1}
     {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 3, :time -1}]
    ```

#### Monotonic Writes
  - `[:G-single-item-process :cyclic-versions]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1, :time -1}
     {:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 3, :time -1}
     {:process 1, :type :ok, :f :txn, :value [[:r :x 1]], :index 5, :time -1}
     {:process 1, :type :ok, :f :txn, :value [[:r :x 0]], :index 7, :time -1}]
    ```

#### Writes Follow Reads
  - `[:G-single-item :G-single-item-process]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1, :time -1}
     {:process 1, :type :ok, :f :txn, :value [[:r :x 0] [:w :y 1]], :index 3, :time -1}
     {:process 2, :type :ok, :f :txn, :value [[:r :y 1] [:r :x nil]], :index 5, :time -1}]
    ```

#### Causal
  - `[:G-single-item]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1, :time -1}
     {:process 1, :type :ok, :f :txn, :value [[:r :x 0] [:w :y 1]], :index 3, :time -1}
     {:process 2, :type :ok, :f :txn, :value [[:r :y 1] [:r :x nil]], :index 5, :time -1}]
    ```

#### Last Write Wins
  - `[:cyclic-versions]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 2, :time -1}
     {:process 1, :type :ok, :f :txn, :value [[:w :x 1]], :index 3, :time -1}
     {:process 0, :type :ok, :f :txn, :value [[:r :x 1]], :index 5, :time -1}
     {:process 1, :type :ok, :f :txn, :value [[:r :x 0]], :index 7, :time -1}]
    ```

----

### Issues, Impedance, and Friction with Adya

  - language has evolved over time
  - not a 1-to-1 mapping between the definition of Causal Consistency, its components and Adya's models

#### Lost Update Anomaly

Lost update is a violation of Consistent View yet is a valid Causal history.

The update isn't lost, it's eventually and consistently merged. 

```clj
; Hlost: r1 (x0, 10) r2(x0 , 10) w2(x2 , 15) c2 w1(x1 , 14) c1
;   [x0 << x2 << x1 ]
[{:process 1 :type :invoke :value [[:r :x nil] [:w :x 14]] :f :txn}
 {:process 2 :type :invoke :value [[:r :x nil] [:w :x 15]] :f :txn}
 {:process 2 :type :ok     :value [[:r :x 10]  [:w :x 15]] :f :txn}
 {:process 1 :type :ok     :value [[:r :x 10]  [:w :x 14]] :f :txn}]
```

#### G-single-item Anomaly 

G-single-item is a violation of Consistent View yet ***can*** be a valid Causal history.

It is not a ww|wr inferred rw cycle,
it's two transactions in different process observing the effects of other processes at different points in time that always move forward in time and are eventually and consistently merged.

```clj
[{:process 0, :type :ok, :f :txn, :value [[:w :x 0]]}
 {:process 1, :type :ok, :f :txn, :value [[:r :x 0] [:w :x 1]]}
 {:process 2, :type :ok, :f :txn, :value [[:r :x 0] [:w :x 2]]}
 {:process 3, :type :ok, :f :txn, :value [[:r :x 0]]}
 {:process 3, :type :ok, :f :txn, :value [[:r :x 1]]}
 {:process 3, :type :ok, :f :txn, :value [[:r :x 2]]}]
 ```

 ##### Yet G-single-iem ***can*** also indicate a true Causal violation
 
 Writes follow reads violation of Causal that is identified as a G-single-item:
 
 ```clj
[{:process 0, :type :ok, :f :txn, :value [[:w :x 0]]}
 {:process 1, :type :ok, :f :txn, :value [[:r :x 0] [:w :y 1]]}
 {:process 2, :type :ok, :f :txn, :value [[:r :y 1] [:r :x nil]]}]
 ```

----

### Elle Consistency Model Graph Changes

Look for `strong-session-PL-2+`:

![New Elle Model Graph](doc/models.png)

----

### Opts to Configure Elle for Causal Consistency

```clj
(def causal-opts
  ; rw_register provides:
  ;   - initial nil -> all versions for all keys
  ;   - w->r
  ;   - ww and rw dependencies, as derived from a version order
  {:consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's formalism for causal consistency
   :anomalies-ignored [:lost-update]                     ; `lost-update`s are causally Ok, they are PL-2+, Adya 4.1.3
   :sequential-keys? true                                ; infer version order from elle/process-graph
   :wfr-keys? true                                       ; wfr-version-graph when <rw within txns
   })
```
