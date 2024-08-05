# Apache Iceberg formal specification (using Fizzbee)

This spec models Iceberg v2.

The spec is modeled as three roles:

- One or more `Writer` instances. A writers can perform both writes and compactions.
- One `Catalog` instance. 
- One `ObjectStore` instance. Stores objects with simple KV API.

The spec models a single Iceberg table with three columns where the 1st column as acts an id column. Given that Iceberg does not include primary keys, the spec ensures that no duplicate rows (rows of the same id) can be inserted. Correctness is verified based on unique ids.

## What is and is not modeled

The following is modeled:

1. Copy-on-write (COW).
2. Merge-on-read (MOR), with positional deletes.
3. Data file compaction.
4. Serializable and Snapshot Isolation.

The following is not modeled:

1. Partitions.
2. Equality deletes
3. Filter pushdown into Iceberg, such as delete expressions.
4. Delete file compaction.
5. Snapshot expiry and associated file clean-up.

## State machine

Iceberg is represented as a state machine with the following states:

- `READY`: The writer is ready to perform an operation.
- `READ_DATA_FILES`: The writer has started an operation and must perform a scan phase.
- `WRITE_DATA_FILES`: The writer has performed a scan (if needed), and is ready to write the new data (and delete) files.
- `WRITE_METADATA_FILES`: The writer has written the data files and is ready to start writing the metadata files.
- `COMMIT`: The writer has written the metadata files and is ready to commit.
- `COMMITTED`: The writer successfully committed.
- `ABORTED`: The writer had to abort after a conflict validation error.

## Consistency checking

A linearized history of per column for each row (per id) (along with the table versions) is recorded and reads against the table are evaluated against this history. 

For example, the following operations would create the following history:

1. INSERT INTO Tab1(Name, FavColor) VALUES('jack', 'red')
2. UPDATE Tab1 SET FavColor = 'blue' WHERE Name = 'jack'
3. DELETE FROM Tab1 WHERE Name = 'jack'

History of FavColor column for row with id 'jack':
1. value='red', version=1
2. value='blue', version=2
3. value=None, version=3

### Possible violation

Model parameters:

```
TableMode = enum('COPY_ON_WRITE', 'MERGE_ON_READ')
UPDATE_MODE = TableMode.MERGE_ON_READ
DELETE_MODE = TableMode.MERGE_ON_READ

Isolation = enum('SERIALIZABLE', 'SNAPSHOT_ISOLATION')
CONFIGURED_ISOLATION = Isolation.SNAPSHOT_ISOLATION
NUM_WRITERS = 2

# State space limiting constants
ALLOW_UPDATES = True               # Include update queries
ALLOW_DELETES = True               # Include delete queries
MAX_WRITE_OPS = 3                  # The max number of write operations
MAX_COMPACTION_OPS = 0             # The max number of compactions

# Column values, each col1 value represents a possible row in the table.
# Col 1 values are not duplicated, so if there are 2 values then there are 2 possible rows.
Col1Values = ['jack']
Col2Values = ['red', 'blue']
Col3Values = ['A']
```

Steps:

1. Writer-0. A single row `['jack', 'red', 'A']` is added to `data-1` in an append op. Run through to commit of `metadata-1` with `snapshot-1`.
2. Writer-0. Step=StartUpdateOperation. Predicate `['jack', None, None]`, set values `[None, 'blue', None]`. Translates to set color='blue' where id='jack'. Starting snapshot id = `snapshot-1`.
3. Writer-1: Step=StartDeleteOperation. Predicate `['jack', None, None]`. Translates to delete from where id='jack'. Starting snapshot id = `snapshot-1`.
4. Writer-0. Step=ReadDataFiles. Performs a scan, finds row `['jack', 'red', 'A']` to update and determines that `data-1` pos 0 must be deleted.
5. Writer-1. Step=ReadDataFiles. Performs a scan, finds row `['jack', 'red', 'A']` to delete in `data-1`.
6. Writer-0. Step=WriteDataFiles. Writes row `['jack', 'blue', 'A']` to `data-2` and delete file `delete-1` pointing to pos 0 of `data-1`.
7. Writer-1. Step=WriteDataFiles. Writes delete file `delete-2` pointing to pos 0 of `data-1`. This means `data-1` has two delete files referencing it, though none is committed yet at this point.
8. Writer-0. Step=WriteMetadataFiles.
    1. Refresh metadata. Remains at `metadata-1`.
    2. Perform validation.
    3. Write a new data manifest `manifest-2` with
        1. `data-1` existing.
        2. `data-2` added.
    4. Write a new delete manifest `manifest-3` with:
        1. `delete-1` added.
    5. Write a new manifest list `mmanifest-list-2` with `manifest-2` and `manifest-3`.
    6. Write a new metadata file `metadata-2` with `snapshot-2` containing `manifest-list-2`.
9. Writer-0. Commit. Swaps `metadata-1` for `metadata-2`.
8. Writer-1. Step=WriteMetadataFiles.
    1. Refresh metadata to `metadata-2`.
    2. Perform validation. Checks that the data file `data-1` has not been deleted since `snapshot-1` was written. It has not. Only has a row-level delete file associated, which is not included in this validation.
    3. Write a new data manifest `manifest-4` with
        1. `data-1` existing.
        2. `data-2` existing.
    4. Write a new delete manifest `manifest-5` with:
        1. `delete-1` existing.
        2. `delete-2` added.
    5. Write a new manifest list `mmanifest-list-3` with `manifest-4` and `manifest-5`.
    6. Write a new metadata file `metadata-3` with `snapshot-3` containing `manifest-list-3`.
10. Writer-1. Commit. Swaps `metadata-2` for `metadata-3`.

At this point a table scan will return row `['jack', 'blue', 'A']` even though it should have been deleted. Whether we use a total ordering of UPDATE then DELETE or DELETE then UPDATE, the 'jack' row should not exist in table version 3.

The issue is that in the Spark Iceberg code thr `SparkPositionDeltaWrite` only enables the validation checks necessary to avoid this when the command is an UPDATE or MERGE, not a DELETE. https://github.com/apache/iceberg/blob/e02b5c90ef305b4d1ca5c19f0b9b2e99f9392e44/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkPositionDeltaWrite.java#L219

A simple fix would be to include DELETE commands.

NEEDS TO BE VERIFIED BY AN ICEBERG COMMITTER.