# jepsen-causal-consistency

### Jepsen tests for causal consistency.

Designed for testing CRDTs.

### Uses Elle, Jepsen's checker:

  - Adya's PL-2+, Consistent View, as the consistency model
  - extend Elle's consistency model graph to include strong-session-consistent-view
    - fills in the gap between strong-session PL-2, Read Committed, and PL-SI, Snapshot Isolation
    - Causal Consistency needs process ordering
  - extend the rw_register test with graphs for
    - writes follow reads
    - monotonic writes

----

### Adya Anomalies Expressed

Read Your Writes
  - `[:G-single-item-process :cyclic-versions]`

Monotonic Writes
  - `[:G-single-item-process :cyclic-versions]`

Writes Follow Reads
  - `[:G-single-item-process :G-single-item]`

Causal
  - `[:G-single-item]`

Last Write Wins
  - `[:cyclic-versions]`

----

### Current Status

Working on a LWW Register test using [ElectricSQL](https://electric-sql.com/).

----

### Elle Consistency Model Graph Changes

Look for `strong-session-consistent-view`:

![New Elle Model Graph](doc/models.png)

```clj
; changes to consistency-models

{:strong-session-monotonic-view  [:monotonic-view  :strong-session-PL-2]
 :strong-session-consistent-view [:consistent-view :strong-session-monotonic-view]

 :strong-session-snapshot-isolation [:snapshot-isolation
                                     :strong-session-PL-2
                                     :strong-session-consistent-view]}

{:strong-session-monotonic-view  [:G1-process]
 :strong-session-consistent-view [:G1-process
                                  :G-single-process]}
```
