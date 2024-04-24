---------------------- MODULE hudi ----------------------
(*
Author: Jack Vanlightly

This specification is based on v5 of the Hudi spec (https://hudi.apache.org/tech-specs/).

The specification has limited functionality, only modeling Commit action types
on COW tables. See the readme.md for notes.

Modules:
 - hudi_common.tla:     Has some common constants.
 - hudi_timeline.tla:   All timeline state in storage layer,
                        plus all timeline logic in the writers.
 - hudi_file_group.tla: All file group state in storage layer,
                        plus all file group logic in the writers.
 - hudi_timestamp.tla:  External timestamp provider state plus logic
                        in the writers.
                        
All logic is in the writers. Storage is simply a file storage layer.
*)

EXTENDS Integers, Naturals, FiniteSets, Sequences, 
        hudi_common, hudi_timeline, hudi_file_group, hudi_timestamp

CONSTANTS Writers, \* The set of writers.
          OpLimit  \* The number of operations possible (limit state space).
                            
\* Internal states of the writer, not part of the Hudi spec.
\* A writer goes through a series of steps to carry out
\* a transaction, modeled as a simple state machine.
CONSTANTS Ready,        \* The writer is available to start an op.
          RequestPhase, \* The writer writes a Requested instant to the timeline.
          LookupPhase,  \* The writer looks up if the key exists or not and gets the
                        \* file group of the key.
          ReadPhase,    \* The writer reads the merge target file slice into memory
                        \* and also writes the Inflight instant.
          WritePhase,   \* The writer writes a new file slice.
          UpdateIndexPhase, \* The writer updates the indexes.
          OCC_Phase,        \* The writer performs an optimistic concurrency control check.
          CommitPhase,      \* The writer writes the Completed instant to the timeline.
          Failed            \* The writer failed. Once failed it never leaves this state.

VARIABLES writer,        \* writer state.
          table_lock,    \* a table lock (using an external lock provider)
          fg_lock,       \* per-file-group locks (using an external lock provider)
          key_index      \* the set of committed keys

\* auxilliary variables (not part of the Hudi spec)
VARIABLES aux_kv_log,    \* a log of the expected values for each key, used in invariants.
          aux_op_count   \* the number of operations that have started.


lock_vars == << table_lock, fg_lock >>
aux_vars == << aux_kv_log, aux_op_count >>
vars == << writer, key_index, lock_vars, timeline_vars, fg_vars, ts_vars,
           aux_kv_log, aux_op_count >>

\* ---------------------------------------------
\* Writer state helpers
\* ---------------------------------------------

WriterOp ==
    [type: {Upsert,Delete},
     key: Keys,
     ts: Nat,
     value: Values]

WriterActive(w) ==
    writer[w] \notin {Ready, Failed}

WriterInState(w, phase) ==
    /\ WriterActive(w)
    /\ writer[w].phase = phase

\* ---------------------------------------------
\* Locking
\* ---------------------------------------------

LockAvailable(file_id) ==
    fg_lock[file_id] = Nil

HoldsLock(w, file_id) ==
    fg_lock[file_id] = w

AcquireFileGroupLocks(w, file_id) ==
    /\ fg_lock[file_id] = Nil
    /\ fg_lock' = [fg_lock EXCEPT ![file_id] = w]
    /\ UNCHANGED table_lock

FreeAnyFileGroupLocks(w) ==
    fg_lock' = [fid \in 1..FileGroupCount |->
                    IF fg_lock[fid] = w
                    THEN Nil
                    ELSE fg_lock[fid]]

FreeAnyLocks(w) ==
    CASE \* CASE No Lock Held. No locks ever held in RequestPhase or LookupPhase
         writer[w].phase \in {RequestPhase, LookupPhase} ->
            UNCHANGED << table_lock, fg_lock >>
         \* CASE OCC. Release table_lock if held
      [] /\ ConcurrencyControl = Optimistic
         /\ table_lock = w ->
            /\ table_lock' = Nil
            /\ UNCHANGED fg_lock
         \* CASE PCC. Release file group locks if held
      [] ConcurrencyControl = Pessimistic ->
            /\ FreeAnyFileGroupLocks(w)
            /\ UNCHANGED table_lock
      [] OTHER ->
            UNCHANGED << table_lock, fg_lock >>

Abort(w) ==
    \* A writer that aborts goes back to the Ready state.
    /\ writer' = [writer EXCEPT ![w] = Ready]
    /\ FreeAnyLocks(w)

\* ---------------------------------------------
\* ACTIONS
\* ---------------------------------------------

(* 
    ACTION: ObtainTimestamp ------------------
    
    The writer decides to perform an upsert or delete.
    
    The writer obtains an action timestamp. When
    MonotonicTs=TRUE the timestamp is obtained from an
    external monotonic timestamp provider. Else a
    a non-deterministic timestamp between 1 and a monotonic
    value is used.
    
    See the Timestamps function in hudi_timestamp.tla. 
*)

ObtainTimestamp(w) ==
    \* enabling conditions
    /\ aux_op_count < OpLimit
    /\ writer[w] = Ready
    \* state mutations
    \* non-deterministically choose a combination of key, value and op_type
    /\ \E key \in Keys, value \in Values, op_type \in {Upsert,Delete} :
        \* non-deterministically choose a ts (when monotonic configured
        \* it will be monotonic)
        \E ts \in Timestamps :
            /\ writer' = [writer EXCEPT ![w] = 
                            [phase            |-> RequestPhase,
                             action_ts        |-> ts,
                             op               |-> [type  |-> op_type,
                                                   key   |-> key,
                                                   value |-> value,
                                                   ts    |-> ts],
                             salt             |-> IF UseSalt
                                                  THEN aux_op_count \* serves as a unique source
                                                  ELSE Nil,
                             instant          |-> Nil,
                             file_id          |-> Nil,
                             merge_file_slice |-> Nil,
                             new_file_slice   |-> Nil]]
            /\ RecordTs(ts)
    /\ aux_op_count' = aux_op_count + 1
    /\ UNCHANGED << key_index, lock_vars, timeline_vars, fg_vars, aux_kv_log >>
    
(* 
    ACTION: AppendRequestedToTimeline ------------------
    
    The writer wants to either:
        * Write a key-value pair 
        * Delete a key 
    
    Using the obtained action timestamp, the writer writes
    the "requested" instant to the timeline. The instant id
    corresponds to its file name, and the instant
    object as a whole corresponds to the file. Note that
    when UseSalt=TRUE, a unique salt is included which
    avoids file name collisions when using non-monotonic
    timestamps,
    
    The writer sets the writer phase to LookupPhase.  
*)

AppendRequestedToTimeline(w) ==
    \* enabling conditions
    /\ WriterInState(w, RequestPhase)
    \* state mutations
    /\ LET id        == [action_ts    |-> writer[w].action_ts,
                         action_type  |-> Commit,
                         action_state |-> Requested,
                         salt         |-> writer[w].salt]
           instant   == [id      |-> id,
                         payload |-> Nil]
       IN IF AbsentOrNotSupported(instant)
          THEN /\ AppendToTimeline(instant)
               /\ writer' = [writer EXCEPT ![w].phase   = LookupPhase,
                                           ![w].instant = instant]
               /\ UNCHANGED lock_vars 
          ELSE /\ Abort(w)
               /\ UNCHANGED timeline_vars
    /\ UNCHANGED << key_index, ts_vars, fg_vars, aux_vars >>

(* 
    ACTION: KeyLookup ---------------------------------
    
    The writer must determine if the key of this operation
    exists. For Upserts, it can then classify it as either
    an insert or an update.
    
    If the key does not exist, the writer non-deterministically
    assigns a file group to it.
*)

KeyLookup(w) ==
    \* enabling conditions
    /\ WriterInState(w, LookupPhase)
    \* state mutations
    /\ LET key        == writer[w].op.key
           key_exists == key \in key_index
           op_type == CASE /\ writer[w].op.type = Upsert
                           /\ key_exists -> Update
                        [] /\ /\ writer[w].op.type = Upsert
                           /\ ~key_exists -> Insert
                        [] OTHER -> Delete
       IN \E file_id \in 1..FileGroupCount :
            /\ IF key_exists
               THEN file_id = FileGroupLookup(key)
               ELSE TRUE \* non-deterministic choice
            /\ IF ~key_exists /\ op_type = Delete
               THEN Abort(w) \* nothing to delete
               ELSE /\ writer' = [writer EXCEPT ![w].op.type = op_type,
                                                ![w].phase   = ReadPhase,
                                                ![w].file_id = file_id]
                    /\ UNCHANGED lock_vars                         
    /\ UNCHANGED << key_index, timeline_vars, ts_vars, fg_vars, aux_vars >>

(* 
    ACTION: ReadMergeTargetFileSlice ------------------
    
    The writer is in the ReadPhase, and just wrote an
    instant to the timeline with:
        * action type = Commit
        * action state = Requested
    
    If the writer uses ConcurrencyControl=2 (pessimistic), then before
    reading any files to merge, it obtains a lock on each file
    group (in this TLA+ specification, it's just one file group).
    
    The writer acquires the lock only if it is free (set to Nil).
    Acquiring the lock consists of setting it to the identity
    of the writer.

    The writer loads the timeline and scans it to determine
    which file slice it must merge. It does this by:
    1. Identifying all completed instants that touched the file
       group of the key of the operation.
    2. Filter down those instants to ones whose action ts
       <= the merge commit ts.
    3. Take the instant with the highest action ts. The file
       slice it points to is the merge target. 
    
    The writer then loads the contents of the file slice into memory.
*)

LoadFSliceToMerge(tl, file_id, merge_ts) ==
    LET merge_id == FindMergeTargetId(tl, file_id, merge_ts)
    IN IF merge_id = Nil THEN Nil
       ELSE LoadFileSlice(file_id, merge_id.action_ts, merge_id.salt)

ValidMergeTargetTs(w, fslice) ==
    \* Due to concurrent writers, it is possible that since
    \* obtaining a monotonic ts, another writer with a higher
    \* ts already wrote to the same file group as this writer
    \* intends to. The scan of the timeline in this step 
    \* should catch this and allow the writer to abort early.
    \/ fslice = Nil
    \/ fslice.base_file_id.commit_ts < writer[w].action_ts

ReadMergeTargetFileSlice(w) ==
    \* enabling conditions
    /\ WriterInState(w, ReadPhase)
    /\ IF ConcurrencyControl = Pessimistic
       THEN LockAvailable(writer[w].file_id)
       ELSE TRUE
    \* state mutations
    /\ LET loaded_timeline == LoadTimeline \* first timeline load
           merge_commit_ts == MaxCommitTs(loaded_timeline)
           fslice  == LoadFSliceToMerge(loaded_timeline,
                                        writer[w].file_id, 
                                        merge_commit_ts)
           id      == [action_ts    |-> writer[w].action_ts,
                       action_type  |-> Commit,
                       action_state |-> Inflight,
                       salt         |-> writer[w].salt]
           instant == [id      |-> id,
                       payload |-> Nil]
       IN IF /\ ValidMergeTargetTs(w, fslice)
             /\ AbsentOrNotSupported(instant)
          THEN /\ AppendToTimeline(instant)
               /\ writer' = [writer EXCEPT ![w].instant = instant,
                                           ![w].merge_file_slice = fslice,
                                           ![w].phase = WritePhase]
               /\ IF ConcurrencyControl = Pessimistic
                  THEN AcquireFileGroupLocks(w, writer[w].file_id)
                  ELSE UNCHANGED lock_vars
          ELSE /\ Abort(w)
               /\ UNCHANGED timeline_vars
    /\ UNCHANGED << key_index, ts_vars, fg_vars, aux_vars >>  

(* 
    ACTION: WriteFileSlices ------------------
    
    The writer is in the WritePhase and just wrote an
    instant to the timeline with:
        * action type = Commit
        * action state = Inflight
    
    The writer merges the upsert/delete operation
    with the stored in-memory file slice and writes a new
    file slice with the id of:
        * file_id (the file group)
        * write_token (always 1 in this spec as write failures
          are not modeled)
        * commit_ts (the action timestamp of this writer op)
        * optionally, a salt if using salts.
        
    The id corresponds to the file slice filename. 
*)

WriteFileSlices(w) ==
    \* enabling conditions
    /\ WriterInState(w, WritePhase)
    \* state mutations
    /\ LET file_slice_id == [file_id     |-> writer[w].file_id,
                             write_token |-> 1,
                             commit_ts   |-> writer[w].action_ts,
                             salt        |-> writer[w].salt] 
           file_slice    == [base_file_id |-> file_slice_id,
                             entries      |-> MergeFileEntries(
                                                writer[w].merge_file_slice,
                                                writer[w].op)]
       IN IF FileSliceAbsentOrNotSupported(writer[w].file_id, 
                                           file_slice_id)
          THEN /\ PutFileSlice(writer[w].file_id, file_slice_id, file_slice)
               /\ writer' = [writer EXCEPT ![w].new_file_slice = [file_id   |-> writer[w].file_id,
                                                                  commit_ts |-> writer[w].action_ts,
                                                                  salt      |-> writer[w].salt],
                                           ![w].phase          = UpdateIndexPhase]
               /\ UNCHANGED lock_vars
          ELSE /\ Abort(w)
               /\ UNCHANGED fg_vars
    /\ UNCHANGED << key_index, table_lock, timeline_vars, ts_vars, aux_vars >>  

(*
    ACTION: UpdateIndexes ------------------------
    
    The writer has written the new file slice and
    now updates the indexes to reflect any changes
    such as a new key and file group mapping.
    
    If the writer uses ConcurrencyControl=1 (optimistic),
    then it acquires the table lock before beginning.
    
    The writer then transitions to the OCC_Check phase
    if using OCC, else it transitions to the Commit phase.
*)

UpdateIndexes(w) ==
    \* enabling conditions
    /\ WriterInState(w, UpdateIndexPhase)
    /\ IF ConcurrencyControl = Optimistic
       THEN table_lock = Nil
       ELSE TRUE
    \* state mutations
    /\ IF /\ writer[w].op.type = Insert
          /\ PrimaryKeyConflict(writer[w].op.key,
                                writer[w].file_id)
       THEN /\ Abort(w)
            /\ UNCHANGED << key_index, fg_vars >>
       ELSE /\ table_lock' = w \* acquire the lock
            /\ key_index' = key_index \union {writer[w].op.key}
            /\ CommitKeyFileGroupMapping(writer[w].op.key, writer[w].file_id)
            /\ writer' = [writer EXCEPT ![w].phase = IF ConcurrencyControl = Optimistic
                                                     THEN OCC_Phase
                                                     ELSE CommitPhase]
            /\ UNCHANGED fg_lock
    /\ UNCHANGED << timeline_vars, ts_vars, aux_vars >>

(* 
    ACTION: OptimisticConcurrencyCheck ------------------
        
    The writer uses ConcurrencyControl=1 (optimistic), and has just
    acquired the table lock and updated the indexes. Still holding
    the table lock the writer does the CC check.
    
    The writer loads the timeline again. If the writer 
    finds a completed instant that contains a file slice with
    an action timestamp that is greater than the commit ts of the
    merge target file slice, then the writer aborts.
    
    If the check finds no conflicting instants in the timeline,
    the writer transitions to the Commit writer phase.
*)

OptimisticConcurrencyCheck(w) ==
    \* enabling conditions
    /\ WriterInState(w, OCC_Phase)
    \* state mutations
    /\ LET orig_ts   == IF writer[w].merge_file_slice = Nil
                        THEN 0 
                        ELSE writer[w].merge_file_slice.base_file_id.commit_ts
           loaded_timeline == LoadTimeline \* second timeline load
       IN IF ConflictExists(loaded_timeline,
                            writer[w].file_id,
                            orig_ts)
          THEN Abort(w)
          ELSE /\ writer' = [writer EXCEPT ![w].phase = CommitPhase]
               /\ UNCHANGED lock_vars
    /\ UNCHANGED << key_index, timeline_vars, ts_vars, fg_vars, aux_vars >>

(* 
    ACTION: AppendCommitToTimeline ------------------------
    
    The writer is in the Commit phase.

    The writer still holds a lock if either optimistic or 
    pessimistic concurrency control is enabled.
    
    The writer writes the completed instant to the timeline
    to commit the operation.
    
    For invariant checking, the key and value is logging in a
    data structure that will be used to compare reads vs
    expected reads (aux_lv_log).
    
    Finally, the writer wipes all state of the operation 
    and releases the lock (if it held it). 
*)

AppendCommitToTimeline(w) ==
    \* enabling conditions
    /\ WriterInState(w, CommitPhase)
    \* state mutations
    /\ LET ts      == writer[w].action_ts
           id      == [action_ts    |-> ts,
                       action_type  |-> Commit,
                       action_state |-> Completed,
                       salt         |-> writer[w].salt]
           instant == [id      |-> id,
                       payload |-> writer[w].new_file_slice]
       IN /\ IF AbsentOrNotSupported(instant)
             THEN /\ AppendToTimeline(instant)
                  /\ aux_kv_log' = [aux_kv_log EXCEPT ![writer[w].op.key] = Append(@, writer[w].op)]
             ELSE UNCHANGED <<timeline_vars, aux_kv_log>>
          /\ writer' = [writer EXCEPT ![w] = Ready]
          /\ FreeAnyLocks(w)
    /\ UNCHANGED << key_index, ts_vars, fg_vars, aux_op_count >>

(*
    ACTION: WriterFail ------------------------
    
    The writer fails and if it held a lock, the 
    lock is released.
    
    NOTE! Currently only fails between updating
    indexes and the timeline as that is where it
    is interesting for a writer to fail.
*)

WriterFail(w) ==
    \* enabling conditions
    /\ WriterActive(w)
    /\ WriterInState(w, CommitPhase) 
    \* state mutations
    /\ writer' = [writer EXCEPT ![w] = Failed]
    /\ FreeAnyLocks(w)
    /\ UNCHANGED << key_index, timeline_vars, fg_vars, 
                    ts_vars, aux_vars >>

\* -------------------------------------------
\* Invariants 
\* -------------------------------------------

\* INV: ConsistentRead -------------------------------------
\* Once a KV pair has been written at a given ts, subsequent
\* reads at that ts should return the same value. Reads at
\* later ts should read this value unless it gets overwritten 
\* by a different value (or deleted) with a higher ts.
\* Also, reads should not return multiple rows for a given key
\* with conflicting values.

IsMostRecentOfFileGroup(id, ts) ==
    /\ ~\E id1 \in DOMAIN timeline :
        /\ id1.action_ts <= ts
        /\ id1.action_state = Completed
        /\ timeline[id1].payload.file_id = timeline[id].payload.file_id
        /\ id1.action_ts > id.action_ts

ValidValueReadable(key, ts, value) ==
    \* A completed instant references a file slice
    \* that contains this KV pair.
    \E id \in DOMAIN timeline :
        /\ id.action_ts <= ts
        /\ id.action_state = Completed
        /\ IsMostRecentOfFileGroup(id, ts)
        /\ LET instant == timeline[id]
               fs == LoadFileSlice(instant.payload.file_id, 
                                   instant.payload.commit_ts,
                                   instant.id.salt)
           IN /\ key \in DOMAIN fs.entries
              /\ fs.entries[key] = value

NoInvalidValueReadable(key, ts, value) ==
    \* There isn't an orphaned row of a duplicate
    \* key that is still readable because its file slice is
    \* still readable.
    ~\E id \in DOMAIN timeline :
        /\ id.action_ts <= ts
        /\ id.action_state = Completed
        /\ IsMostRecentOfFileGroup(id, ts)
        /\ LET instant == timeline[id]
               fs == LoadFileSlice(instant.payload.file_id, 
                                   instant.payload.commit_ts,
                                   instant.id.salt)
           IN /\ key \in DOMAIN fs.entries
              /\ fs.entries[key] # value
              
ReadIsNil(key, ts) ==
    ~\E id \in DOMAIN timeline :
        /\ id.action_ts <= ts
        /\ id.action_state = Completed
        /\ IsMostRecentOfFileGroup(id, ts)
        /\ LET instant == timeline[id]
               fs == LoadFileSlice(instant.payload.file_id, 
                                   instant.payload.commit_ts,
                                   instant.id.salt)
           IN key \in DOMAIN fs.entries

RECURSIVE ModelReadKey(_,_,_,_)
ModelReadKey(i, seq, ts, value) ==
    CASE i > Len(seq) -> value
      [] seq[i].ts <= ts ->
            IF seq[i].type = Delete
            THEN ModelReadKey(i+1, seq, ts, Nil)
            ELSE ModelReadKey(i+1, seq, ts, seq[i].value)
      [] OTHER -> ModelReadKey(i+1, seq, ts, value)

ModelRead(key, ts) ==
    ModelReadKey(1, aux_kv_log[key], ts, Nil)

ConsistentRead ==
    \A key \in Keys, ts \in 1..MaxActionTs(timeline) :
        LET model_value == ModelRead(key, ts)
        IN IF model_value = Nil
           THEN ReadIsNil(key, ts)
           ELSE /\ ValidValueReadable(key, ts, model_value)
                /\ NoInvalidValueReadable(key, ts, model_value)

\* INV: NoDuplicateKeysAcrossFiles --------------------
\* A key is not duplicated across file slices of different
\* file groups. This invariant is not so important as
\* ConsistentRead will also detect this when the value of
\* the duplicated key is different in each file group.
NoDuplicateKeysAcrossFiles ==
    \A key \in Keys :
        ~\E fg1, fg2 \in 1..FileGroupCount :
            /\ fg1 # fg2
            /\ \E fs_id \in FileSliceIds(fg1) :
                \* the key is in the file slice
                /\ key \in DOMAIN FileSlice(fg1, fs_id).entries
                \* there is a completed instant pointing to this file slice
                /\ \E id \in DOMAIN timeline :
                    /\ id.action_state = Completed
                    /\ timeline[id].payload.file_id = fg1
                    /\ timeline[id].payload.commit_ts = fs_id.commit_ts
                    /\ timeline[id].payload.salt = fs_id.salt
            /\ \E fs_id \in FileSliceIds(fg2) :
                \* the key is in the file slice
                /\ key \in DOMAIN FileSlice(fg2, fs_id).entries
                \* there is a completed instant pointing to this file slice
                /\ \E id \in DOMAIN timeline :
                    /\ id.action_state = Completed
                    /\ timeline[id].payload.file_id = fg2
                    /\ timeline[id].payload.commit_ts = fs_id.commit_ts
                    /\ timeline[id].payload.salt = fs_id.salt

TestInv ==
    TRUE

\* LIVENESS: EventuallyWriterTerminates
\* A writer that begins an op will eventually either
\* return to the ready state after completing or
\* aborting the op, or will fail. Failing is not
\* weakly fair, so a liveness violation cannot
\* be hidden by a writer failure. 
    
EventuallyWriterTerminates ==
    \A w \in Writers :
        WriterActive(w) ~> (\/ writer[w] = Ready
                            \/ writer[w] = Failed)

\* -------------------------------------------
\* Init and Next
\* -------------------------------------------

Init ==
    /\ writer = [w \in Writers |-> Ready]
    /\ table_lock = Nil
    /\ fg_lock = [fg \in 1..FileGroupCount |-> Nil]
    /\ key_index = {}
    /\ TimelineInit
    /\ FgInit(Keys)
    /\ TsInit
    /\ aux_kv_log = [k \in Keys |-> <<>>]
    /\ aux_op_count = 0

Next ==
    \E w \in Writers :
        \/ ObtainTimestamp(w)
        \/ AppendRequestedToTimeline(w)
        \/ KeyLookup(w)
        \/ ReadMergeTargetFileSlice(w)
        \/ WriteFileSlices(w)
        \/ UpdateIndexes(w)
        \/ OptimisticConcurrencyCheck(w)
        \/ AppendCommitToTimeline(w)
        \/ WriterFail(w)

Fairness ==
    \A w \in Writers :
        /\ WF_vars(ObtainTimestamp(w))
        /\ WF_vars(AppendRequestedToTimeline(w))
        /\ WF_vars(KeyLookup(w))
        /\ WF_vars(ReadMergeTargetFileSlice(w))
        /\ WF_vars(WriteFileSlices(w))
        /\ WF_vars(OptimisticConcurrencyCheck(w))
        /\ WF_vars(UpdateIndexes(w))
        /\ WF_vars(AppendCommitToTimeline(w))
        
Spec == Init /\ [][Next]_vars
LivenessSpec == Init /\ [][Next]_vars /\ Fairness

===================================================