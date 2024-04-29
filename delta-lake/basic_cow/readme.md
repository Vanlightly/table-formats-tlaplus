# TLA+ specification for Delta Lake COW tables

This specification models a single copy-on-write table with two columns. The workload treats column1 as a non-unique identifier, all updates and deletes use a predicate based on column1 values.

The accompanying blog post uses a table with a color and and a count column as the workload. 

## Notes

- Only models COW tables.
- Only models Add/Remove action types.
- Only models regular writers (no compaction/cleaning)
- Supports multiple writers.
- Does not include checkpoints.

## Model parameters

The following parameters (TLA+ constants), affect the size of the model and its behavior.

- `Writers`, a set of model values, e.g {w1, w2}
- `Column1Values`, a set of values that can be written to column1, e.g. {Red, Green, Blue} or {1, 2, 3}. Any set of either model or non-model values is ok.
- `Column2Values`, a set of values for column2, e.g. {A, B} or {1, 2}. Again, any set of either model or non-model values is ok.
- `OpCount`, integer. Limits the number of operations (state space) e.g. 4
- `PutIfAbsentSupported`, TRUE/FALSE. TRUE will implement optimistic concurrency control via PutIfAbsent storage.
- `UseCoordination`, TRUE/FALSE. TRUE will use writer coordination to implement optimistic concurrency control.
- `PartitionByColumn1`, TRUE/FALSE. TRUE will partition the table by the first column, FALSE will implement no partitioning.

## Correctness:

The following invariants exist:
1. Consistent reads at a given version (Snapshot Isolation).
2. No delta log entry can add and remove the same data file.
  
## Writer state-machine
    
The writer holds internal state outside of the scope of the Delta Lake protocol, but which is necessary to carry out all the work required to complete a transaction.

The write goes through the following phases:

1. Ready
2. ReadPhase
3. WritePhase
4. VersionCheckPhase (if UseCoordination=TRUE)
5. CommitPhase
                               
## Optimistic concurrency control (OCC)

OCC is implemented with either PutIfAbsent storage or via writer coordination. See the model parameters for how to set one or the other or neither.

### PutIfAbsent example

Two writer scenario, using the color and count example from the blog post.

```
CONSTANTS w1 = w1
          w2 = w2
          Writers = {w1, w2}
          Red = Red
          Green = Green
          Blue = Blue
          Column1Values = {Red, Green, Blue}
          Column2Values = {1, 2}
          PutIfAbsentSupported = TRUE
          UseCoordination = FALSE
          PartitionByColumn1 = TRUE
          OpLimit = 4
```

### Writer coordination example

Two writer scenario, using the color and count example from the blog post.

```
CONSTANTS w1 = w1
          w2 = w2
          Writers = {w1, w2}
          Red = Red
          Green = Green
          Blue = Blue
          Column1Values = {Red, Green, Blue}
          Column2Values = {1, 2}
          PutIfAbsentSupported = FALSE
          UseCoordination = TRUE
          PartitionByColumn1 = TRUE
          OpLimit = 4
```

## Running the model checker

Be sure to include the `-deadlock` argument when running TLC as the table versioning creates an infinite state space that we limit via the `OpLimit` parameter. This causes TLC to run out of states to explore, which it treats as a deadlock.