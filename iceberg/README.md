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