----------------------------- MODULE delta_lake -----------------------------

EXTENDS Integers, Naturals, FiniteSets, FiniteSetsExt, Sequences, TLC

\* Model parameters
CONSTANTS Writers,              \* The set of writers.
          OpLimit,              \* The number of operations possible (limit state space).
          PutIfAbsentSupported, \* TRUE/FALSE whether storage supports put-if-absent
          UseCoordination,      \* TRUE/FALSE whether to use coordination for delta log writes
          PartitionByColumn1,   \* TRUE=partition by col1, FALSE=no partitioning
          Column1Values,        \* The set of col1 values
          Column2Values         \* The set of col2 values
          
\* Internal states of the writer, not part of the Delta Lake spec.
\* A writer goes through a series of steps to carry out
\* a transaction, modeled as a simple state machine.
CONSTANTS Ready,        \* The writer is available to start a transaction.
          ReadPhase,    \* The writer notes the current table version and
                        \* reads the merge target data files into memory.
          WritePhase,   \* The writer writes any new data files.
          VersionCheckPhase, \* (Optional) The writer checks the delta log version in a lock.
          CommitPhase,  \* The writer writes the log entry to the delta log
          Failed        \* The writer failed. Once failed it never leaves this state.

\* operation types
CONSTANTS Insert,
          Update, 
          Delete
          
CONSTANT Nil

ASSUME PutIfAbsentSupported \in BOOLEAN
ASSUME UseCoordination \in BOOLEAN
ASSUME PartitionByColumn1 \in BOOLEAN
ASSUME OpLimit \in Nat

VARIABLES writer,       \* writer state, as a map (writer id -> state).
          table_lock,   \* a table lock (when UseCoordination=TRUE)
          delta_log,    \* the write-ahead-log as a map (log entry id -> log entry file)
          data_files    \* the (Parquet) files that contain the data.
                        \* As a map (file id -> file contents)

\* auxilliary variables (not part of the Delta Lake spec)
VARIABLES aux_kv_log,     \* serialized transaction history of each col1 value, used in invariants.
          aux_file_ctr,   \* used for creating unique file names
          aux_txn_count   \* the number of transactions that have started.

aux_vars == << aux_kv_log, aux_file_ctr, aux_txn_count >>
vars == << writer, table_lock, delta_log, data_files, aux_vars >>

\* -------------------------------------------
\* Types and helpers
\* -------------------------------------------

DataFileId ==
    [file_no: Nat, \* an integer identifier, used as a file name
     partition: Column1Values \union {Nil}]

DeltaLogRecord ==
    [version: Nat,
     added_files: SUBSET DataFileId,
     removed_files: SUBSET DataFileId]

InsertOp == [col1: Column1Values,
             col2: Column2Values]
DeleteOp == [col1: Column1Values]             
UpdateOp == [col1: Column1Values,
             set_col2: Column2Values]            

OptimisticTxn ==
    [read_version: Nat,
     op_type: Insert,
     partition: Column1Values \union {Nil},
     op: InsertOp]
     
     \union
     
    [read_version: Nat,
     op_type: Update,
     partition: Column1Values \union {Nil},
     op: UpdateOp]
     
     \union
     
    [read_version: Nat,
     op_type: Delete,
     partition: Column1Values \union {Nil},
     op: DeleteOp]
     
\* Writer helpers ---------------------
     
WriterActive(w) ==
    writer[w] \notin {Ready, Failed}

WriterInState(w, phase) ==
    /\ WriterActive(w)
    /\ writer[w].phase = phase

EmptyWriterState ==
    [phase          |-> Nil,
     delta_log      |-> Nil,
     txn            |-> Nil,
     merge_targets  |-> Nil,
     commit_version |-> 0,
     added_files    |-> Nil,
     removed_files  |-> Nil]

\* Delta log helpers ---------------------------

CurrentVersion(dlog) ==
    IF Cardinality(DOMAIN dlog) = 0
    THEN 0
    ELSE Max(DOMAIN dlog)
    
AbsentOrNotSupported(log_entry_id) ==
    \/ ~PutIfAbsentSupported
    \/ log_entry_id \notin DOMAIN delta_log

DeltaLogPut(log_entry_id, contents) ==
    (* If the log entry id exists in the delta log, then
       it gets overwritten. This cannot happen when
       PutIfAbsentSupported=TRUE.*)
    IF log_entry_id \in DOMAIN delta_log
    THEN \* overwrite
         delta_log' = [delta_log EXCEPT ![log_entry_id] = contents]
    ELSE \* add
         delta_log' = delta_log @@ (log_entry_id :> contents)    

\* File helpers ------------------------------

DataFiles ==
    (* All the possible data file names ever created so far *) 
    IF PartitionByColumn1
    THEN [file_no: 1..aux_file_ctr, partition: Column1Values]
    ELSE [file_no: 1..aux_file_ctr, partition: {Nil}]

CurrentOrNewDataFiles ==
    (* All the possible data file names ever created so far, 
       and a file name of a new file. Used to non-deterministically
       use an existing data file, or create a new one. *)
    IF PartitionByColumn1
    THEN [file_no: 0..aux_file_ctr, partition: Column1Values]
    ELSE [file_no: 0..aux_file_ctr, partition: {Nil}]

IsLiveDataFile(f, partition, dlog) ==
    (* Determines if this data file exists in the current
       table version (the delta log loaded into memory of 
       the writer). *)
    \E id \in DOMAIN dlog :
        /\ f \in delta_log[id].added_files
        /\ f.partition = partition
        /\ ~\E id1 \in DOMAIN dlog :
            /\ f \in delta_log[id1].removed_files
            /\ f.partition = partition
            /\ id1 > id

\* -------------------------------------------
\* Actions
\* -------------------------------------------

(* 
    ACTION: StartOperation ------------------
    
    The writer decides to perform an insert, update
    or delete.
*)

InitWriterState(txn, loaded_dl) ==
    (* Sets the initial state of the writer with the optimistic
       transaction. It caches the delta log in memory, to be used
       during the read phase. The field commit_version is recorded
       as the table version (based on the last log entry) + 1. *)
    [EmptyWriterState EXCEPT !.phase          = ReadPhase,
                             !.txn            = txn,
                             !.delta_log      = loaded_dl,
                             !.commit_version = txn.read_version + 1]

Partition(col1) ==
    IF PartitionByColumn1
    THEN col1 ELSE Nil

StartInsertTxn(w, loaded_dl) ==
    \E col1 \in Column1Values, col2 \in Column2Values :
        LET txn == [read_version |-> CurrentVersion(loaded_dl),
                    op_type      |-> Insert,
                    partition    |-> Partition(col1),
                    op           |-> [col1 |-> col1,
                                      col2 |-> col2]]
        IN writer' = [writer EXCEPT ![w] = InitWriterState(txn, loaded_dl)]

StartUpdateTxn(w, loaded_dl) ==
    \E col1 \in Column1Values, new_col2 \in Column2Values :
        LET txn == [read_version |-> CurrentVersion(loaded_dl),
                    op_type      |-> Update,
                    partition    |-> Partition(col1),
                    op           |-> [col1     |-> col1,
                                      set_col2 |-> new_col2]]
        IN writer' = [writer EXCEPT ![w] = InitWriterState(txn, loaded_dl)]

StartDeleteTxn(w, loaded_dl) ==
    \E col1 \in Column1Values :
        LET txn == [read_version |-> CurrentVersion(loaded_dl),
                    op_type      |-> Delete,
                    partition    |-> Partition(col1),
                    op           |-> [col1 |-> col1]]
        IN writer' = [writer EXCEPT ![w] = InitWriterState(txn, loaded_dl)]

StartOperation(w) ==
    \* enabling conditions
    /\ aux_txn_count < OpLimit
    /\ writer[w] = Ready
    \* state mutations
    \* non-deterministically choose a combination of 
    \* col1 value, col2 value and op_type
    /\ \E op_type \in {Insert, Update, Delete} :
        CASE op_type = Insert ->
                StartInsertTxn(w, delta_log)
          [] op_type = Update ->
                StartUpdateTxn(w, delta_log)
          [] OTHER ->
                StartDeleteTxn(w, delta_log)
    /\ aux_txn_count' = aux_txn_count + 1
    /\ UNCHANGED << delta_log, data_files, table_lock, 
                    aux_file_ctr, aux_kv_log >>

(* 
    ACTION: ReadDataFiles ------------------
    
    The writer is in the ReadPhase.
    
    The writer scans the loaded delta log to determine
    which data files, are the live files. Then it determines
    which of those live files contain rows relevant to the
    transaction. The implementation would prune based on 
    partitioning and column statistics.
    
    The writer then loads the contents of the identified data
    files into memory.
*)

NewFileToCreate(f, partition) ==
    /\ f \notin DOMAIN data_files
    /\ f.partition = partition

GetMergeTargetFileNames(w, partition, col1) ==
    { f \in DataFiles :
        /\ IsLiveDataFile(f, partition, writer[w].delta_log)
        /\ \E row \in data_files[f].rows :
            /\ row[1] = col1 }

ReadDataFiles(w) ==
    \* enabling conditions
    /\ WriterInState(w, ReadPhase)
    \* state mutations
    /\ IF writer[w].txn.op_type \in {Update, Delete}
       THEN 
            \* UPDATES and DELETES
            \* Find the files with matching rows for the update/delete
            \* and load them into memory
            LET target_filenames  == GetMergeTargetFileNames(w,
                                        writer[w].txn.partition,
                                        writer[w].txn.op.col1)
                merge_targets     == { data_files[f] : f \in target_filenames } 
            IN IF merge_targets = {}
               THEN writer' = [writer EXCEPT ![w] = Ready] \* abort, nothing to do
               ELSE writer' = [writer EXCEPT ![w].merge_targets = merge_targets,
                                             ![w].phase = WritePhase]
       ELSE 
            \* INSERTS
            \* Non-deterministically choose a file to insert the
            \* new row into, and load it into memory. The writer 
            \* is free to choose to create a new file, without 
            \* merging any existing one.
            \E f \in CurrentOrNewDataFiles :
               /\ \* either the file is a live one of the partition of the txn (this spec does single row operations)
                  \/ IsLiveDataFile(f, writer[w].txn.partition, writer[w].delta_log)
                  \* or this is going to be a new file
                  \/ NewFileToCreate(f, writer[w].txn.partition)
               /\ LET is_new_file  == f \notin DOMAIN data_files
                      merge_target == IF is_new_file
                                      THEN [file_id |-> f,
                                            rows    |-> {}]
                                      ELSE data_files[f]
                  IN writer' = [writer EXCEPT ![w].merge_targets = {merge_target},
                                              ![w].phase = WritePhase] 
    /\ UNCHANGED << delta_log, data_files, table_lock, aux_vars >>  

(* 
    ACTION: WriteDataFiles ------------------
    
    The writer is in the WritePhase.
    
    The writer merges the current transaction with
    the loaded data files and writes them back
    as new data files.
*)

MergeRows(rows, txn) ==
    CASE txn.op_type = Delete ->
            rows \ { row \in rows : row[1] = txn.op.col1 }
      [] txn.op_type = Update ->
            { IF row[1] = txn.op.col1
              THEN << txn.op.col1, txn.op.set_col2 >>
              ELSE row : row \in rows }
      [] txn.op_type = Insert ->
            rows \union { << txn.op.col1, txn.op.col2 >> } 

AddedFiles(w, files) ==
    (* Set difference between new set of files and the original set *)
    DOMAIN files \ DOMAIN data_files

RemovedFiles(w) ==
    (* The preexisting set of merge target data files *)
    { f \in DataFiles :
        /\ f \in DOMAIN data_files
        /\ \E target \in writer[w].merge_targets :
            f = target.file_id }     

RECURSIVE WriteFiles(_,_,_,_)
WriteFiles(w, file_id, merge_targets, files) ==
    (* Recursive function that in each iteration, chooses one
       of the merge target data files, to merge and then
       writes result as a new data file. Once there are no files
       left to merge, the operation completes. *)
    IF Cardinality(merge_targets) = 0
    THEN \* all copy-on-write merging is done
         /\ data_files' = files
         /\ writer' = [writer EXCEPT ![w].phase  = IF UseCoordination
                                                   THEN VersionCheckPhase
                                                   ELSE CommitPhase,
                                     ![w].added_files = AddedFiles(w, files),
                                     ![w].removed_files = RemovedFiles(w)]
         /\ aux_file_ctr' = Cardinality(DOMAIN files)
    ELSE \* still more copy-on-write merging to do
         LET target   == CHOOSE t \in merge_targets : TRUE
             rows     == MergeRows(target.rows, writer[w].txn)
             new_file == [file_id |-> file_id, rows |-> rows]
             new_files == files @@ (file_id :> new_file)
             new_merge_targets == merge_targets \ {target}
             next_file_id == [file_no   |-> file_id.file_no + 1,
                              partition |-> file_id.partition]
         IN WriteFiles(w, next_file_id, new_merge_targets, new_files)

NextFileId(w) ==
    (* Picks a unique number to be used as a file number for
       the next data file. This is not Delta Lake protocol, just
       a way of creating unique file names in TLA+. *)
    [file_no   |-> Cardinality(DOMAIN data_files)+1,
     partition |-> IF PartitionByColumn1
                   THEN writer[w].txn.op.col1
                   ELSE Nil] 

WriteDataFiles(w) ==
    \* enabling conditions
    /\ WriterInState(w, WritePhase)
    \* state mutations
    /\ WriteFiles(w,
                  NextFileId(w),
                  writer[w].merge_targets,
                  data_files)
    /\ UNCHANGED << delta_log, table_lock, aux_kv_log, aux_txn_count >>

(* 
    ACTION: CoordinatePrepareWrite ------------------
    
    The writer is in the VersionCheckPhase because 
    UseCoordination=TRUE and the read phase has completed.
    The writer acquires a table lock and loads the delta 
    log. If it finds a log entry with a higher entry id 
    than its own commit version (read version + 1 of its
    transaction), then it performs a conflict detection 
    check. If a conflict is detected then the writer aborts,
    else it transitions to the commit phase.
    
    Conflict check:
    - If no partitioning is used, then there is a conflict.
    - If there is partitioning, then there is a conflict if
      the higher log entry/entries impacted data files within
      the partitions of this writers transaction. If the higher
      log entries do not touch the partitions of this writer's
      transaction, then there is no conflict. 
*)

ConflictDetected(w) ==
    \E id \in DOMAIN delta_log :
        /\ id >= writer[w].txn.read_version
        /\ IF PartitionByColumn1
           THEN \/ \E f \in delta_log[id].added_files : f.partition = writer[w].txn.partition
                \/ \E f \in delta_log[id].removed_files : f.partition = writer[w].txn.partition
           ELSE TRUE    
    
CoordinatePrepareWrite(w) ==
    /\ WriterInState(w, VersionCheckPhase)
    /\ table_lock = Nil
    /\ IF writer[w].commit_version <= CurrentVersion(delta_log)
       THEN \* Another writer won. If there is a conflict then abort, else
            \* reset the commit version of the writer to repeat.
            /\ IF ConflictDetected(w)
               THEN writer' = [writer EXCEPT ![w] = Ready] \* aborts
               ELSE writer' = [writer EXCEPT ![w].commit_version = CurrentVersion(delta_log) + 1]
            /\ UNCHANGED table_lock \* implicit lock release by not setting table_lock variable.
       ELSE /\ writer' = [writer EXCEPT ![w].phase = CommitPhase]
            /\ table_lock' = w \* lock acquired
    /\ UNCHANGED <<delta_log, data_files, aux_vars>>    

(* 
    ACTION: TryCommitTxn --------------------------------
    
    The writer is in the CommitPhase and attempts to write
    the log entry, using the commit version (read version + 1),
    as the filename. The implementation uses a zero-left-padded
    filename such as 00000000000000000003.json, but this TLA+
    spec uses an integer.
    
    If PutIfAbsentSupported=TRUE, then the log entry write will
    only succeed if there is no existing log entry in the delta
    log with the same id. If it fails to write the file, then
    the writer loads the delta log and performs a conflict check
    (see CoordinatePrepareWrite for a description of the check).
    If the check is ok then it sets its commit version to the
    current table version + 1, in order to try again (but does
    not retry atomically, TryCommitTxn will be executed again).
    
    If PutIfAbsentSupported=FALSE, then the log entry write
    will succeed whether there is an existing log entry in 
    the delta log with the same id or not. For safety, 
    UseCoordination=TRUE should be used in this case, which
    avoids overwriting delta log entries.
*)

RecordCommit(w) ==
    (* Records a linearized history of transactions for this col1
       value. *)
    aux_kv_log' = [aux_kv_log EXCEPT ![writer[w].txn.op.col1] = 
                         Append(@, [version |-> writer[w].commit_version,
                                    txn     |-> writer[w].txn])]

DoCommit(w) ==
    /\ DeltaLogPut(writer[w].commit_version, 
            [version       |-> writer[w].commit_version,
             added_files   |-> writer[w].added_files,
             removed_files |-> writer[w].removed_files])
    /\ RecordCommit(w)

TryCommitTxn(w) ==
    /\ WriterInState(w, CommitPhase)
    /\ CASE  
         \* CASE: PutIfAbsent write succeeds, or not supported
            AbsentOrNotSupported(writer[w].commit_version) ->
                /\ DoCommit(w)
                /\ writer' = [writer EXCEPT ![w] = Ready]
         \* CASE: PutIfAbsent failed, and no conflict, update the commit version for a repeat.
         [] ConflictDetected(w) = FALSE ->
                /\ writer' = [writer EXCEPT ![w].commit_version = CurrentVersion(delta_log) + 1]
                /\ UNCHANGED << delta_log, aux_kv_log >>
         \* CASE: PutIfAbsent failed, and conflict detected, aborts.
         [] OTHER ->
                /\ writer' = [writer EXCEPT ![w] = Ready]
                /\ UNCHANGED << delta_log, aux_kv_log >>
    /\ IF UseCoordination
       THEN table_lock' = Nil \* release the table lock
       ELSE UNCHANGED table_lock 
    /\ UNCHANGED <<data_files, aux_file_ctr, aux_txn_count>>            

(*
    ACTION: WriterFail ------------------------
    
    The writer fails and if it held a lock, the 
    lock is released.
    
    NOTE! Currently only fails between writing data
    files and writing the log entry to the delta log
    (which is more interesting).
*)

WriterFail(w) ==
    \* enabling conditions
    /\ WriterActive(w)
    /\ WriterInState(w, CommitPhase) 
    \* state mutations
    /\ writer' = [writer EXCEPT ![w] = Failed]
    /\ IF UseCoordination /\ table_lock = w
       THEN table_lock' = Nil \* release the table lock
       ELSE UNCHANGED table_lock
    /\ UNCHANGED << delta_log, data_files, aux_vars >>

\* -------------------------------------------
\* Invariants and liveness
\* -------------------------------------------

\* INV: ValidDeltaLog -------------------------------------
\* The delta log has some invariants including:
\*   - A log entry cannot have the same file in added and removed
\*     if it is has the data change flag set to true. This TLA+ spec
\*     only performs data changes, no rearrangement which is equivalent
\*     to data_change=true.
ValidDeltaLog ==
    \A id \in DOMAIN delta_log :
        ~\E f \in delta_log[id].added_files :
            f \in delta_log[id].removed_files

\* INV: ConsistentRead -------------------------------------
\* This spec uses col1 and col2 as a kind of KV pair. All updates
\* and deletes use col1 as the target.
\* Once a KV pair has been written at a given version, subsequent
\* reads at that version should return the same value. Reads at
\* later version should read this value unless it gets overwritten 
\* by a different value (or deleted) with a higher version.
\* Duplicates are possible as there are no primary keys, so it is
\* possible to insert two rows with identical values. Duplicates
\* do not violate consistency, and if two inserts of the same row
\* occur, consistency rules mean that both rows should be readable.

IsRelevantDataFile(f, partition, version) ==
    (* Is the data file readable at this version *)
    \* 1. Partition match
    /\ f.partition = partition
    \* 2. There is a log entry that references it in its added_files. 
    /\ \E id \in DOMAIN delta_log :
        /\ id <= version
        /\ f \in delta_log[id].added_files
        \* 3. There is no later log entry that references it in its removed_files.
        /\ ~\E id1 \in DOMAIN delta_log :
            /\ id1 <= version
            /\ f \in delta_log[id1].removed_files
            /\ id1 > id

ValidValueReadable(col1_value, version, col2_values) ==
    (* A delta log entry references a relevant data file 
       (a data file that is relevant for the given version) 
       that contains a row with matching col1 and col2 values. *)
    \A col2_value \in col2_values :
        \E id \in DOMAIN delta_log :
            /\ id <= version
            /\ \E f \in delta_log[id].added_files :
                /\ IsRelevantDataFile(f, Partition(col1_value), version)
                /\ \E row \in data_files[f].rows :
                    /\ row[1] = col1_value
                    /\ row[2] = col2_value

NoInvalidValueReadable(col1_value, version, col2_values) ==
    (* There is no delta log entry that references a relevant data file 
       (a data file that is relevant for the given version) that contains 
       this col1 value but has a different col2 value. *)
    ~\E id \in DOMAIN delta_log :
        /\ id <= version
        /\ \E f \in delta_log[id].added_files :
            /\ IsRelevantDataFile(f, Partition(col1_value), version)
            /\ \E row \in data_files[f].rows :
                /\ row[1] = col1_value
                /\ row[2] \notin col2_values
              
ReadIsNil(col1_value, version) ==
    (* There is no delta log entry that references a relevant data file 
       (a data file that is relevant for the given version) that has 
       a row with this col1 value.*)
    ~\E id \in DOMAIN delta_log :
        /\ id <= version
        /\ \E f \in delta_log[id].added_files :
            /\ IsRelevantDataFile(f, Partition(col1_value), version)
            /\ \E row \in data_files[f].rows : row[1] = col1_value

RECURSIVE ModelReadRow(_,_,_,_)
ModelReadRow(i, seq, version, values) ==
    (* Read the serialized history of transactions of this key (col1),
       in order, and determine the final rows which a read should return. *)
    CASE i > Len(seq) -> values
      [] seq[i].version <= version ->
            CASE seq[i].txn.op_type = Delete ->
                    ModelReadRow(i+1, seq, version, {})
              [] seq[i].txn.op_type = Insert ->
                    ModelReadRow(i+1, seq, version, values \union {seq[i].txn.op.col2})
              [] seq[i].txn.op_type = Update ->
                    ModelReadRow(i+1, seq, version, {seq[i].txn.op.set_col2 : v \in values})
              [] OTHER -> 
                    ModelReadRow(i+1, seq, version, values)
      [] OTHER -> ModelReadRow(i+1, seq, version, values)

ModelRead(col1_value, version) ==
    ModelReadRow(1, aux_kv_log[col1_value], version, {})

ConsistentRead ==
    \A col1_value \in Column1Values, version \in 1..CurrentVersion(delta_log) :
        LET model_col2_values == ModelRead(col1_value, version)
        IN IF model_col2_values = {}
           THEN \* No rows should be readable
                ReadIsNil(col1_value, version)
           ELSE \* All the rows of the model read should be readable
                /\ ValidValueReadable(col1_value, version, model_col2_values)
                \* No rows other rows should be readable
                /\ NoInvalidValueReadable(col1_value, version, model_col2_values)

\* INV: TestInv -------------------------------------
\* For debugging
TestInv ==
    TRUE

\* LIVENESS: EventuallyCompletes --------------------
\* Once a writer begins a transaction, it either
\* completes it successfully, aborts or fails.
\* Writer failure is not weakly fair so cannot interfere
\* with this liveness property. 
EventuallyCompletes ==
    \A w \in Writers :
        WriterActive(w) ~> ~WriterActive(w)

\* -------------------------------------------
\* Init and Next
\* -------------------------------------------

EmptyMap == [e \in {} |-> Nil]

Init ==
    /\ writer = [w \in Writers |-> Ready]
    /\ table_lock = Nil
    /\ delta_log = EmptyMap
    /\ data_files = EmptyMap
    /\ aux_kv_log = [k \in Column1Values |-> <<>>]
    /\ aux_txn_count = 0
    /\ aux_file_ctr = 1

Next ==
    \E w \in Writers :
        \/ StartOperation(w)
        \/ ReadDataFiles(w)
        \/ WriteDataFiles(w)
        \/ CoordinatePrepareWrite(w) \* when no put-if-absent
        \/ TryCommitTxn(w)
        \/ WriterFail(w)

Fairness ==
    (* Note that WriterFail is not included here on purpose.*)
    \A w \in Writers :
        /\ WF_vars(StartOperation(w))
        /\ WF_vars(ReadDataFiles(w))
        /\ WF_vars(WriteDataFiles(w))
        /\ WF_vars(CoordinatePrepareWrite(w))
        /\ WF_vars(TryCommitTxn(w))
        
Spec == Init /\ [][Next]_vars
LivenessSpec == Init /\ [][Next]_vars /\ Fairness

=============================================================================
