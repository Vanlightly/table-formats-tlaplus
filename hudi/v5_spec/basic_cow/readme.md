# TLA+ specification for COW tables and Commit action types (v5 spec)

This specification is based on v5 of the Hudi spec (https://hudi.apache.org/tech-specs/)

## Notes

- Only models COW tables.
- Only models Commit action types.
- Only models regular writers (no table services)
- Support multiple writers.
- Configurable monotonic timestamps, locking and PutIfAbsent storage.
- Writers only perform single key upsert/delete operations.
- Uses a fixed file group pool with lazy assignment.

## Model parameters

Parameters that affect model size:

- `Writers`, a set of model values, e.g {w1, w2}
- `Keys`, a set of model values, e.g. {k1, k2}
- `Values`, a set of model values, e.g. {A, B}
- `OpCount`, integer. Limits the number of operations (state space) e.g. 4
- `FileGroupCount`, integer. The number of possible file groups.

Parameters that affect correctness:

- `MonotonicTs`, TRUE/FALSE. TRUE is Hudi spec conformant. FALSE is not.
- `ConcurrencyControl`, 0=None, 1=Optimistic, 2=Pessimistic.
- `PrimaryKeyConflictCheck`, TRUE/FALSE. FALSE will lead to duplication of primary keys across file groups.
- `PutIfAbsentSupported`, TRUE/FALSE. TRUE will prevent files from being overwritten, and therefore timestamp collisions will be controlled.
- `UseSalt`, e.g TRUE or FALSE. TRUE will avoid timestamp collisions when MonotonicTs=FALSE

## Correctness:

The following invariants exist:
1. Consistent reads at a given timestamp (Snapshot Isolation).
2. No key written to multiple file groups.
  
## Writer state-machine
    
The writer holds internal state outside of the scope of the Hudi spec, but which is necessary to carry out all the work required to complete a transaction.

The write goes through the following phases:

1. Ready
2. RequestPhase
3. LookUpPhase
4. ReadPhase
5. WritePhase
6. UpdateIndexPhase
7. OCC_Phase (only if OCC configured)
8. CommitPhase
                               
Phase descriptions:

1. `Ready`. The write has no operation in progress.
2. `RequestPhase`: The writer writes the Requested instant to the timeline.
3. `LookupPhase`: The writer looks up the key in the indexes to see if it exists and get a file group. If its an insert, the writer non-deterministically assigns a file group to that key (locally).
4. `ReadPhase`: The writer scans the timeline for the file slice to merge. If it finds one, it reads the file slice it into memory. It also writes the Inflight instant to the timeline. If using pessimistic concurrency control, the writer acquires a lock per file group it will modify.
5. `WritePhase`: The writer merges the changes, and writes a new file slice.
6. `UpdateIndexPhase`: The writer updates any indexes as a result of things like new primary keys with new file group mappings. If using OCC, it first acquires the table lock.
7. `OCC_Phase`: The writer performs a concurrency check if optimistic concurrency control is enabled.
8. `CommitPhase`: The writer writes the Completed instant to the timeline. It also frees the any locks if it held any.

## Concurrency control

Three types:
- `0` -> None
- `1` -> Optmimistic. The writer acquires a table lock before updating the indexes and releases it after writing the completed instant. It also performs file group conflict check.
- `2` -> Pessimistic. The writer acquires per-file-group locks for the file groups it intends to modify. These locks are released once the completed instant is written.

### Optimistic concurrency control check

1. The writer loads the timeline. 
2. The writer determines the timestamp to use to detect a conflict. If the writer performed a merge on a file slice, then the timestamp is that of the merge file slice. If no merge file slice existed, then it used a timestamp of 0.
3. The writer scans the loaded timeline for any completed instant that touched the same file id, and has a timestamp > the selected timestamp. 
4. If such a completed instant is found, the check fails, else it passes.

On failure, the writer aborts, releasing any locks.

## Hudi spec conformant configurations

The v5 spec mandates the use of monotonic timestamps. It also recommends optimistic locking in multi writer scenarios. Primary key conflict detection is optional.

Optimistic concurrency control with primary key conflict detection.

```
MonotonicTs = TRUE
ConcurrencyControl = 1
PrimaryKeyConflictCheck = TRUE
PutIfAbsentSupported = FALSE
UseSalt = FALSE
```

Optimistic concurrency control without primary key conflict detection.

```
MonotonicTs = TRUE
ConcurrencyControl = 1
PrimaryKeyConflictCheck = FALSE
PutIfAbsentSupported = FALSE
UseSalt = FALSE
```

Pessimistic concurrency control with primary key conflict detection.

```
MonotonicTs = TRUE
ConcurrencyControl = 2
PrimaryKeyConflictCheck = TRUE
PutIfAbsentSupported = FALSE
UseSalt = FALSE
```

Optimistic concurrency control without primary key conflict detection.

```
MonotonicTs = TRUE
ConcurrencyControl = 2
PrimaryKeyConflictCheck = FALSE
PutIfAbsentSupported = FALSE
UseSalt = FALSE
```

## Notable RFCs to check

The following RFCs may be relevant to this work:
- RFC-56: Early Conflict Detection For Multi-writer
- RFC-69: Hudi 1.0

Also:
- https://hudi.apache.org/docs/concurrency_control/