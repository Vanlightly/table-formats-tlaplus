# A Fizzbee specification of Apache Paimon

This spec models Primary Key tables in Apache Paimon, as of v0.8.

The spec is modeled as three roles:

- `Writer` instance. Each instance does its own commits. A regular writer and a compactor are co-located in the same instance, just like the real-world.
- `Lock` instance. A lock service for when not using PutIfAbsent storage.
- `ObjectStore` instance. Stores objects with simple KV API.

The spec models a table with three columns, the first being the primary, and the other two being string columns. It supports pmultiple partitions and buckets (fixed). It does not model dynamic bucket mode.

Primary keys are mapped to partitions and buckets using a round-robin approach in order to make the mapping predictable ahead of time. The same goes for the mapping of writers to buckets.

Note this spec does not do caching, therefore writers often do object store reads where the implementation might use a cache. It is not trying to emulate the implementation details, only the logical design relevant to consistency.

## Deletion Vectors

You can enable or disable deletion vectors with the `DV_ENABLED` constant. When `True`, compactors will maintain a single DV file per bucket. Note, that reads will include level 0, which does not match the implementation, however this makes the spec simpler. Reading from level 0 with DV enabled does not affect correctness.

When `DV_ENABLED=False`, reads only use the sequence number merge process.


## PutIfAbsent and Lock

Paimon requires either:

```
PUT_IF_ABSENT = True
USE_LOCK = False
```

or:

```
PUT_IF_ABSENT = False
USE_LOCK = True
```

Using the following will result in a safety property violation:

```
PUT_IF_ABSENT = False
USE_LOCK = False
```

## Supported topologies

This spec does not support the Flink style (concurrent writer, serial committer) topology. In this spec, each writer and each compactor does its own commits, meaning that commits can happen concurrently.

The following constants are relevant:
- `NUM_WRITERS` and `NUM_COMPACTORS`. Writers and compactors are co-located in the same Writer instances when there is the same count of each. When the counts are different, some Writer instances may only have one of the two.
- `NUM_PARTITIONS` and `NUM_BUCKETS`. 
- When `ONE_WRITER_PER_BUCKET`=True, each writer instance is responsible for a disjoint set of the buckets. Thus each bucket can only have one writer instance (one instance having one writer and one compactor). Assignment to buckets is done round-robin fashion so that the assignment is predictable. When False, each writer can write to any bucket. 
- `STREAMING_SINK`. When True, updates set the value of all columns, treating the table as not the source of truth. When False, an update performs a read/modify/write operation on one column only, treating the table as a source of truth.

### Multiple buckets, one writer/compactor per bucket

For example, two writers/compactors, one partition with two buckets:

```
NUM_WRITERS = 2
NUM_COMPACTORS = 2
NUM_PARTITIONS = 1
NUM_BUCKETS = 2
```

### One bucket, multiple writers, one compactor

Note, this is an unsafe configuration for Paimon. It will cause a consistency violation due to sequence number reordering duing merges.

```
NUM_WRITERS = 2
NUM_COMPACTORS = 1
NUM_PARTITIONS = 1
NUM_BUCKETS = 1
```

### One bucket, one writer, multiple compactors

Note, this is an unsafe configuration for Paimon, when using deletion vectors. It will cause issues such as dangling deletion vectors, or lost updates to deletion vectors. Note that to hit such a violation requires at a minimum 2 writes and 3 compactions and a very large amount of memory (Fizzbee currently is memory-only). 

```
NUM_WRITERS = 1
NUM_COMPACTORS = 2
NUM_PARTITIONS = 1
NUM_BUCKETS = 1
```

Constants to reproduce dangling deletion vectors with smallest state space:

```
NUM_WRITERS = 1
NUM_COMPACTORS = 2
NUM_PARTITIONS = 1
NUM_BUCKETS = 1
MAX_LEVEL = 3
PUT_IF_ABSENT = True
USE_LOCK = False
DV_ENABLED = True
ONE_WRITER_PER_BUCKET = False
STREAMING_SINK = FALSE
ALLOW_UPDATES = True
ALLOW_DELETES = False
MAX_WRITE_OPS = 2
MAX_WRITE_OPS_PER_KEY = 2
MAX_WRITE_OPS_PER_WRITER = 2
MAX_COMPACTIONS = 3
MAX_COMPACTIONS_PER_COMPACTOR = 2
PkCol1Values = ['jack'  ]
Col2Values = ['red', 'blue']
Col3Values = ['A']
```

## Auxilliary variables and functions

Any variable or function name with "aux" in it refers to auxilliary state and logic used for checking properties, they are nothing to do with Paimon itself.

## Reducing the state space.

Fizzbee at the time of writing does not spill to disk and is therefore limited in the size of the state space. The following constants can be used to limit the state space:

- `ALLOW_UPDATES` True/False. Allows or disallows streaming sink and read/modify/write updates.
- `ALLOW_DELETES` True/False. Allows or disallows deletes.
- `MAX_WRITE_OPS` Integer. The maximum number of write ops.
- `MAX_WRITE_OPS_PER_KEY` Integer. The maximum number of write ops per primary key.
- `MAX_WRITE_OPS_PER_WRITER` Integer. The maximum number of write ops per writer.
- `MAX_COMPACTIONS` Integer. The maximum number of compactions.
- `MAX_COMPACTIONS_PER_COMPACTOR` Integer. The maximum number of compactions per compactor.

Additionally, the number of primary key values and non-primary key values affects the state space:

Very small number of values.

```
PkCol1Values = ['jack']
Col2Values = ['red', 'blue']
Col3Values = ['A']
```

Large (for Fizzbee) number of values

```
PkCol1Values = ['jack', 'sarah', 'john']
Col2Values = ['red', 'blue']
Col3Values = ['A', 'B']
```