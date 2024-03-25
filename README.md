### Jepsen Tests for Causal Consistency

Designed for testing local first systems, CRDTs, and distributed syncing.

[Jepsen](https://github.com/jepsen-io/jepsen) has an established [history](https://jepsen.io/analyses) of testing databases.

These tests have focused on stronger levels of [consistency](https://jepsen.io/consistency), e.g. snapshot-isolation, linearizability, and serializability.

This project explores using Jepsen to test for [Causal Consistency](https://jepsen.io/consistency/models/causal) with Strong Convergence and atomic transactions ([Monotonic Atomic View](https://jepsen.io/consistency/models/monotonic-atomic-view)).

----

### Uses Elle, Jepsen's Transactional Consistency Checker

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
  - `[:G-single-item-process]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]],   :index 1}
     {:process 0, :type :ok, :f :txn, :value [[:r :x nil]], :index 3}]
    ```
    ```txt
    G-single-item-process
    Let:
      T1 = {:index 3, :time -1, :type :ok, :process 0, :f :txn, :value [[:r :x nil]]}
      T2 = {:index 1, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 0]]}
    
    Then:
      - T1 < T2, because T1's read of [:x nil] did not observe T2's write of [:x 0] (r->w).
      - However, T2 < T1, because process 0 executed T2 before T1: a contradiction!
    ```
    ![read your writes G-single-item-process](doc/ryw-G-single-item-process.svg)

----

#### Monotonic Writes
  - `[:G-single-item-process]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
     {:process 0, :type :ok, :f :txn, :value [[:w :x 1]], :index 3}
     {:process 1, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 5}
     {:process 1, :type :ok, :f :txn, :value [[:r :x #{0 1}]], :index 7}]
    ```
    ```txt
    G-single-item-process
    Let:
      T1 = {:index 5, :time -1, :type :ok, :process 1, :f :txn, :value [[:r :x #{1}]]}
      T2 = {:index 1, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 0]]}
      T3 = {:index 3, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 1]]}
    
    Then:
      - T1 < T2, because T1's read of [:x #{1}] did not observe T2's write of [:x 0] (r->w).
      - T2 < T3, because process 0 executed T2 before T3.
      - However, T3 < T1, because T3's write of [:x 1] was read by T1 (w->r): a contradiction!
    ```
    ![monotonic writes G-single-item-process](doc/monotonic-writes-G-single-item-process.svg)

----

#### Monotonic Reads
  - `[:G-single-item-process]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
     {:process 1, :type :ok, :f :txn, :value [[:w :x 1]], :index 3}
     {:process 2, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 5}
     {:process 2, :type :ok, :f :txn, :value [[:r :x #{0 1}]], :index 7}
     {:process 2, :type :ok, :f :txn, :value [[:r :x #{1}]], :index 9}]
    ```
    ```txt
    G-single-item-process
    Let:
      T1 = {:index 9, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :x #{1}]]}
      T2 = {:index 1, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 0]]}
      T3 = {:index 7, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :x #{0 1}]]}
    
    Then:
      - T1 < T2, because T1's read of [:x #{1}] did not observe T2's write of [:x 0] (r->w).
      - T2 < T3, because T2's write of [:x 0] was read by T3 (w->r).
      - However, T3 < T1, because process 2 executed T3 before T1: a contradiction!
    ```
    ![monotonic reads G-single-item-process](doc/monotonic-reads-G-single-item-process.svg)

----

#### Writes Follow Reads
  - `[:G-single-item-process :G-single-item ]`
    ```clj
    [{:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 1}
     {:process 1, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 3}
     {:process 1, :type :ok, :f :txn, :value [[:w :y 0]], :index 5}
     {:process 2, :type :ok, :f :txn, :value [[:r :y #{0}]], :index 7}
     {:process 2, :type :ok, :f :txn, :value [[:r :x nil]], :index 9}]
    ```
    ```txt
    G-single-item-process
    Let:
      T1 = {:index 9, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :x nil]]}
      T2 = {:index 1, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 0]]}
      T3 = {:index 5, :time -1, :type :ok, :process 1, :f :txn, :value [[:w :y 0]]}
      T4 = {:index 7, :time -1, :type :ok, :process 2, :f :txn, :value [[:r :y #{0}]]}
    
    Then:
      - T1 < T2, because T1's read of [:x nil] did not observe T2's write of [:x 0] (r->w).
      - T2 < T3, because T2's write of [:x 0] was observed by process 1 before it executed T3 (wfr).
      - T3 < T4, because T3's write of [:y 0] was read by T4 (w->r).
      - However, T4 < T1, because process 2 executed T4 before T1: a contradiction!
      ```
      ![writes follow reads G-single-item-process](doc/writes-follow-reads-G-single-item-process.svg)

----

#### write -> read
- `[:G1c-process, :G0]`
  ```clj
  [{:process 0, :type :ok, :f :txn, :value [[:r :x #{0}]], :index 1, :time -1}
   {:process 0, :type :ok, :f :txn, :value [[:w :x 0]], :index 3, :time -1}]
  ```
  ```txt
  G1c-process
  Let:
    T1 = {:index 1, :time -1, :type :ok, :process 0, :f :txn, :value [[:r :x #{0}]]}
    T2 = {:index 3, :time -1, :type :ok, :process 0, :f :txn, :value [[:w :x 0]]}
  
  Then:
    - T1 < T2, because process 0 executed T1 before T2.
    - However, T2 < T1, because T2's write of [:x 0] was read by T1 (w->r): a contradiction!
  ```
  ![w->r G1c-process](doc/wr-G1c-process.svg)

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

The tests will use [ElectricSQL](https://electric-sql.com/):
  - transactional causal+ consistency
  - local first
  - active/active SQLite3/PostgreSQL CRDT based sync
  - strong research team

----

### Elle Consistency Model Graph Changes

Look for `strong-session-PL-2+`:

![New Elle Model Graph](doc/models.png)

----

### Opts to Configure Elle for Causal Consistency

```clj
(def causal-opts
  {:consistency-models [:strong-session-consistent-view] ; Elle's strong-session with Adya's formalism for causal consistency
   :anomalies-ignored  [:lost-update]                    ; `lost-update`s are causally Ok, but they are PL-2+, Adya 4.1.3 ?!?
   })
```
