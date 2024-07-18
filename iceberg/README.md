# Apache Iceberg formal specification (using Fizzbee)

This spec models Iceberg v2.

The spec is modeled as three roles:

- One or more `Writer` instances. A writers can perform both writes and compactions.
- One `Catalog` instance. 
- One `ObjectStore` instance. Stores objects with simple KV API.

The spec models a single Iceberg table with three columns where the 1st column as acts an id column. Given that Iceberg does not include primary keys, the spec ensures that no duplicate rows (rows of the same id) can be inserted. Correctness is verified based on unique ids.

Both copy-on-write (COW) and merge-on-read (MOR) are modeled, as is compaction. Either Serializable or Snapshot Isolation can be configured.

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

### Multiple writers and Snapshot isolation

Snapshot isolation should in theory not cause a consistency violation, but Iceberg's definition of SI is not standard as Iceberg SI allows concurrent conflicting operations to commit causing lost updates. This is documented, but perhaps this isolation level name should be changed.

### Serializable + Concurrent delete/compaction + COW

The Fizzbee spec has detected a consistency violation with the Serializable isolation level when used in conjunction with COW and compaction.

1. Writer 0: Step=StartInsert. Insert row `['jack', 'red', 'A']`.
2. Writer 0: Step=WriteDataFiles. Write `data-0` with `rows=[['jack', 'red', 'A']]`.
3. Writer 0: Step=WriteMetadataFiles:
    a. Manifest: `manifest-0` with `data-0` added.
    b. Manifest-list: `manifest-list-0` with `manifest-0`.
    c. Metadata: `metadata-0-uuid0` with snapshot 1 containing `manifest-list-0`.
4. Writer 0: Step=Commit. Current location=`metadata-0-uuid0`.
5. Writer 0: Step=StartDelete. Delete of row with id column = 'jack', table version 1. Starting snapshot=current snapshot of `metadata-0-uuid0`.
6. Writer 0: Step=ReadDataFiles, identifying `data-0` to be deleted.
7. Writer 1: Step=StartCompaction, at table version 1. Starting snapshot=current snapshot of `metadata-0-uuid0`.
7. Writer 0: Step=WriteDataFiles. Writes `data-1` with the contents of `data-0` with the 'jack' row removed, `rows=[]`.
8. Writer 0. Step=WriteMetadataFiles. Loads metadata `metadata-0-uuid0`.
    a. Manifest: `manifest-1` with `data-0` removed and `data-1` added.
    b. Manifest-list: `manifest-list-1` with `manifest-1`.
    c. Metadata: `metadata-1-uuid1` with snapshot 1 containing `manifest-list-1`.
10. Writer 1: Step=ReadDataFiles. Perform a table scan, returning one row `['jack', 'red', 'A']`.
11. Writer 1: Step=WriteDataFiles. Write table scan results to a new data file, `data-2` with `rows=[['jack', 'red', 'A']]`.
12. Writer 0: Step=Commit. Current location=`metadata-1-uuid1`.
13. Writer 1: Step=WriteMetadataFiles. Loads metadata `metadata-1-uuid1`.
    a. Manifest: `manifest-2` with `data-1` existing and `data-2` added. `data-0` was listed as deleted already, so gets filtered out of the manifest.
    b. Manifest-list: `manifest-list-2` with `manifest-2`.
    c. Metadata: `metadata-2-uuid2` with current snapshot containing `manifest-list-2`.
14. Writer 1: Step=Commit. Current location=`metadata-2-uuid2`.

At this point a table scan will return `['jack', 'red', 'A']`, despite the successful delete.

Thus, an insert of `['jack', 'red', 'A']`, followed by a concurrent delete of row `['jack', 'red', 'A']` and a compaction, results in the undeleting of row `['jack', 'red', 'A']`.

The compaction currently only checks for conflicting deletes using MOR mode (i.e. it looks for conflicting delete files). With COW, no such delete files are added and so the compaction is allowed to commit.

THIS SHOULD BE CHECKED BY AN ICEBERG COMMITTER.