# TLA+ specification for COW tables and Commit action types (v5 spec)

This specification is based on v5 of the Hudi spec (https://hudi.apache.org/tech-specs/)

## Notes

- Only models COW tables.
- Only models Commit action types.
- Only models regular writers (not table services)
- Support multiple writers.
- Configurable monotonic timestamps and locking.
- Writers only perform single key upsert/delete operations.
- Uses a fixed file group pool.

## Correctness:

The following invariants exist:
1. Consistent reads at a given timestamp (Snapshot Isolation).
2. Pessimistic lock usage leads to only one active transaction at a time.

Correctness in multi-writer scenarios is verified for the following configs:
1. MonotonicTsProvider=TRUE, LockType=1 (optimistic)
2. MonotonicTsProvider=FALSE, LockType=2 (pessimistic)
    
## Writer state-machine
    
The writer holds internal state outside of the scope of the Hudi spec, but which is necessary to carry out all the work required to complete a transaction.

The write goes through the following phases:

```
RequestPhase -> ReadPhase -> WritePhase -> CheckPhase -> CommitPhase
              -> Abort
```                                          
Phase descriptions:

1. RequestPhase: The writer has obtained a timestamp.
2. ReadPhase: The writer has written a Requested instant to the timeline and must now read the relevant file slice (if it exists).
3. WritePhase: The writer has read any relevant file slice to merge, and written the Inflight instant to the timeline.
4. CheckPhase: The writer has written the base file.
5. CommitPhase: The concurrency check passed and the writer can now write the Completed instant to the timeline.
6. Abort: The writer detected a conflict in the check phase.

## Locking

Three lock types:
- 0 -> None
- 1 -> Optmimistic locking. The writer acquires the lock before writing the completed instant.
- 2 -> Pessimistic locking. The writer acquires the lock before doing anything.

Locks are released after writing the completed instant of the transaction.