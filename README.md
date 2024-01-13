# jepsen-causal-consistency

### Jepsen tests for causal consistency.

Designed for testing CRDTs.

### Uses Elle, Jepsen's checker:

  - Adya's PL-2+, Consistent View, as the consistency model
    - minus the Lost Update anomaly, the update isn't lost, it's eventually and consistently merged 
  - extends Elle's consistency model graph to include strong-session-consistent-view
    - fills in the gap between strong-session PL-2, Read Committed, and strong-session PL-SI, Snapshot Isolation
    - Causal Consistency needs process ordering
  - extends the rw_register test with version graphs for
    - writes follow reads
    - monotonic writes

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
  - `[:G-single-item-process :G-single-item]`
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

### Current Status

Working on a LWW Register test using [ElectricSQL](https://electric-sql.com/).

----

### Elle Consistency Model Graph Changes

Look for `strong-session-consistent-view`:

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
   :anomalies [:internal]                                ; basic hygiene
   :anomalies-ignored [:lost-update]                     ; `lost-update`s are causally Ok, they are PL-2+, Adya 4.1.3
   :sequential-keys? true                                ; infer version order from elle/process-graph
   :wfr-keys? true                                       ; wfr-version-graph when <rw within txns
   :wfr-process? true                                    ; wfr-process-graph used to infer version order
   })
```
