---------------------- MODULE hudi ----------------------
(*
Author: Jack Vanlightly

This specification is based on v5 of the Hudi spec (https://hudi.apache.org/tech-specs/).

The specification has limited functionality, only modeling Commit action types
on COW tables. See the hudi_cow_spec_v5_commits_readme.md for notes.

Modules:
 - hudi_common.tla:     Has some common constants.
 - hudi_timeline.tla:   All timeline state in storage layer,
                        plus all timeline logic in the writers.
 - hudi_file_group.tla: All file group state in storage layer,
                        plus all file group logic in the writers.
 - hudi_timestamp.tla:  External timestamp provider state plus logic
                        in the writers.
                        
All logic is in the writers. Storage is simply a file storage layer
without any conditional logic (mimicking S3).
*)

EXTENDS Integers, Naturals, FiniteSets, Sequences, 
        hudi_common, hudi_timeline, hudi_file_group, hudi_timestamp

CONSTANTS Writers, \* The set of writers.
          OpLimit,
          TestWriterFailures
                            
\* Internal states of the writer, not part of the Hudi spec.
\* A writer goes through a series of steps to carry out
\* a transaction, modeled as a simple state machine.
CONSTANTS Ready,
          TsPhase,
          RequestPhase, \* The writer has obtained a timestamp.
          LookupPhase,
          ReadPhase,    \* The writer has written a Requested instant to the timeline.
          WritePhase,   \* The writer has read any relevant file slice to merge, and
                        \* written the Inflight instant to the timeline.
          CheckPhase,   \* The writer has written the base files.
          CommitIndexPhase,   \* The concurrency check passed and the writer can now
                        \* write the Completed instant to the timeline.
          CommitTimelinePhase,
          Failed

ASSUME TestWriterFailures \in BOOLEAN

VARIABLES writer,        \* writer state.
          external_lock, \* an external lock provider.
          key_index

\* auxilliary variables (not part of the Hudi spec)
VARIABLES aux_kv_log,    \* a log of the expected values for each key, used in invariants.
          aux_op_count
          
aux_vars == << aux_kv_log, aux_op_count >>
vars == << writer, external_lock, key_index, timeline_vars, fg_vars, ts_vars,
           aux_kv_log, aux_op_count >>

\* ------------------------------------------------------------------
\* Types, definitions 
\* ------------------------------------------------------------------

WriterOp ==
    [type: {Upsert,Delete},
     key: Keys,
     value: Values]

\* ---------------------------------------------
\* Helpers
\* ---------------------------------------------

WriterActive(w) ==
    writer[w] \notin {Ready, Failed}

WriterInState(w, action_type, action_state, phase) ==
    /\ WriterActive(w)
    /\ writer[w].instant # Nil
    /\ writer[w].instant.id.action_type = action_type
    /\ writer[w].instant.id.action_state = action_state
    /\ writer[w].phase = phase

LockCheck(w) ==
    IF LockType = PessimisticLock
    THEN external_lock = w
    ELSE TRUE
    
\* ---------------------------------------------
\* ACTIONS
\* ---------------------------------------------

(* 
    ACTION: WriterStartsOp ------------------
    
    The writer decides to perform an upsert or delete. 
    When LockType=2, the writer will acquire the lock now.
    The writer caches the timeline.
*)

WriterStartsOp(w) ==
    /\ aux_op_count < OpLimit
    /\ writer[w] = Ready
    /\ IF LockType = PessimisticLock
       THEN /\ external_lock = Nil
            /\ external_lock' = w
       ELSE UNCHANGED external_lock
    /\ \E key \in Keys, value \in Values, op_type \in {Upsert,Delete} :
         writer' = [writer EXCEPT ![w] = 
                        [phase            |-> TsPhase,
                         op               |-> [type  |-> op_type,
                                               key   |-> key,
                                               value |-> value,
                                               ts    |-> Nil],
                         cached_timeline  |-> LoadTimeline,
                         action_ts        |-> Nil,
                         merge_commit_ts  |-> Nil,
                         instant          |-> Nil,
                         file_id          |-> Nil,
                         merge_file_slice |-> Nil,
                         new_file_slice   |-> Nil]]
    /\ aux_op_count' = aux_op_count + 1
    /\ UNCHANGED << key_index, timeline_vars, fg_vars, ts_vars, aux_kv_log >>
    
(* 
    ACTION: ObtainTimestamp ------------------
    
    The writer obtains an action timestamp. There are three 
    configurable ways of obtaining the timestamp in this spec:
        1 = Using an external monotonic timestamp provider.
        2 = Using the highest ts in the cached timeline + 1.
        3 = A non-deterministic ts between 1 and a monotonic
            value, which does not conflict with the cached timeline.
    
    See the Timestamps function in hudi_timestamp.tla.
    
    It also determines the merge commit timestamp, from the
    cached lineline, which will be used later to find the
    data to merge (if any).
*)

ObtainTimestamp(w) ==
    /\ LockCheck(w)
    /\ WriterActive(w)
    /\ writer[w].phase = TsPhase
    /\ \E ts \in Timestamps(writer[w].cached_timeline) :
        /\ ~\E id \in DOMAIN writer[w].cached_timeline : id.action_ts = ts 
        /\ writer' = [writer EXCEPT ![w].phase = RequestPhase,
                                    ![w].action_ts = ts,
                                    ![w].op.ts = ts,
                                    ![w].merge_commit_ts = MaxCommitTs(writer[w].cached_timeline)]
        /\ RecordTs(ts)
        /\ UNCHANGED << external_lock, key_index, timeline_vars, fg_vars,
                        aux_vars >>

(* 
    ACTION: AppendRequestedToTimeline ------------------
    
    The writer wants to either:
        * Write a key-value pair 
        * Delete a key 
    
    Using the obtained action timestamp, the writer performs a
    timeline writes the "requested" instant to the timeline. 
    The instant id corresponds to its file name, and the instant
    object as a whole corresponds to the file.
    
    The writer sets the writer phase to ReadPhase.  
*)

AppendRequestedToTimeline(w) ==
    /\ LockCheck(w)
    /\ WriterActive(w)
    /\ writer[w].phase = RequestPhase
    /\ LET id        == [action_ts    |-> writer[w].action_ts,
                         action_type  |-> Commit,
                         action_state |-> Requested]
           instant   == [id      |-> id,
                         payload |-> Nil]
       IN /\ AppendToTimeline(instant)
          /\ writer' = [writer EXCEPT ![w].phase   = LookupPhase,
                                      ![w].instant = instant]
    /\ UNCHANGED << external_lock, key_index, fg_vars, ts_vars, 
                    aux_vars >>

(* 
    ACTION: KeyLookup ---------------------------------
    
    The writer must determine if the key of this operation
    exists. For Upserts, it can then classify it as either
    an insert or an update.
*)

KeyLookup(w) ==
    /\ LockCheck(w)
    /\ WriterActive(w)
    /\ writer[w].phase = LookupPhase
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
               ELSE TRUE
            /\ IF ~key_exists /\ op_type = Delete
               THEN writer' = [writer EXCEPT ![w] = Ready] \* abort, go back to Ready
               ELSE writer' = [writer EXCEPT ![w].op.type = op_type,
                                             ![w].phase   = ReadPhase,
                                             ![w].file_id = file_id]
    /\ UNCHANGED << external_lock, key_index, timeline_vars, fg_vars, ts_vars, 
                    aux_vars >>

(* 
    ACTION: WriteFilesReadPhase ------------------
    
    The writer is in the ReadPhase, and just wrote an
    instant to the timeline with:
        * action type = Commit
        * action state = Requested
    
    The writer scans the cached timeline determines which
    file slice it must merge. It does this by:
    1. Identify all completed instants that touched the file
       group of the key of the operation.
    2. Filter down those instants to ones whose action ts
       <= the merge commit ts.
    3. Take the instant with the highest action ts. The file
       slice it points to is the merge target. 
    
    The writer then loads the contents of the file slice into memory.
*)

LoadFSliceToMerge(cached_timeline, file_id, merge_ts) ==
    LET merge_id == FindMergeTargetId(cached_timeline,
                                      file_id, merge_ts)
    IN IF merge_id = Nil THEN Nil
       ELSE LoadFileSlice(file_id, merge_id.action_ts)

WriteFilesReadPhase(w) ==
    /\ LockCheck(w)
    /\ WriterInState(w, Commit, Requested, ReadPhase)
    /\ LET fslice  == LoadFSliceToMerge(writer[w].cached_timeline,
                                        writer[w].file_id, 
                                        writer[w].merge_commit_ts)
           id      == [action_ts    |-> writer[w].action_ts,
                       action_type  |-> Commit,
                       action_state |-> Inflight]
           instant == [id      |-> id,
                       payload |-> Nil]
       IN /\ AppendToTimeline(instant)
          /\ writer' = [writer EXCEPT ![w].instant = instant,
                                      ![w].merge_file_slice = fslice,
                                      ![w].phase = WritePhase]
    /\ UNCHANGED << external_lock, key_index, fg_vars, ts_vars, 
                    aux_vars >>  

(* 
    ACTION: WriteFilesWritePhase ------------------
    
    The writer is in the WritePhase and just wrote an
    instant to the timeline with:
        * action type = Commit
        * action state = Inflight
    
    The writer merges the upsert/delete operation
    with the stored file slice and writes a new
    file slice with the id of:
        * file_id (the file group)
        * write_token (always 1 in this spec as write failures
          are not modeled)
        * commit_ts (the action timestamp of this writer op)
        
    The id corresponds to the file slice filename. 
*)

WriteFilesWritePhase(w) ==
    /\ LockCheck(w)
    /\ WriterInState(w, Commit, Inflight, WritePhase)
    /\ LET file_slice_id == [file_id     |-> writer[w].file_id,
                             write_token |-> 1,
                             commit_ts   |-> writer[w].action_ts] 
           file_slice    == [base_file_id |-> file_slice_id,
                             entries      |-> MergeFileEntries(
                                                writer[w].merge_file_slice,
                                                writer[w].op)]
       IN /\ PutFileSlice(writer[w].file_id, file_slice_id, file_slice)
          /\ writer' = [writer EXCEPT ![w].new_file_slice = [file_id   |-> writer[w].file_id,
                                                             commit_ts |-> writer[w].action_ts],
                                      ![w].phase          = CheckPhase]
    /\ UNCHANGED << external_lock, key_index, timeline_vars, ts_vars, 
                    aux_vars >>  


(* 
    ACTION: AcquireOptimisticLock ------------------
    
    The writer uses LockType=1 (optimistic), and has just
    written a file slice.
    
    The writer acquires the lock only if it is free (set to Nil).
    Acquiring the lock consists of setting it to the identity
    of the writer.
*)

AcquireOptimisticLock(w) ==
    /\ LockType = OptimisticLock
    /\ external_lock = Nil
    /\ WriterInState(w, Commit, Inflight, CheckPhase)
    /\ external_lock' = w
    /\ UNCHANGED << writer, key_index, timeline_vars, fg_vars, ts_vars,
                    aux_vars >>

(* 
    ACTION: ConcurrencyCheck ------------------
        
    The writer has written all the file group files. At this
    point, if the writer is holding a lock, it is either with
    optimistic or pessimistic locking enabled.
    
    The writer refreshes its cached timeline. If the writer 
    finds a completed instant that contains a file slice with
    an action timestamp that is greater than the commit ts of the
    merge target file slice, then the writer aborts.
    
    If the file group mapping index enables conflict detection,
    then on trying to commit a key insertion, it checks that the
    file group mapping index does not have a conflicting mapping.
    This is not discussed in the Hudi spec, but seems like an
    easy check to do at this point.
    
    In this spec, the writer does not clean up any files.
    
    If the check finds no conflicting instants in the timeline,
    the writer transitions to the Commit writer phase.
*)

ConcurrencyCheck(w) ==
    /\ \/ LockType = NoLock
       \/ external_lock = w
    /\ WriterInState(w, Commit, Inflight, CheckPhase)
    /\ LET orig_ts   == IF writer[w].merge_file_slice = Nil
                        THEN 0 
                        ELSE writer[w].merge_file_slice.base_file_id.commit_ts
           cached_tl == LoadTimeline
           file_overlap_conflict == ConflictExists(cached_tl,
                                        writer[w].file_id,
                                        orig_ts,
                                        writer[w].new_file_slice.commit_ts)
           fg_mapping_conflict == IF writer[w].op.type = Insert
                                  THEN FgMappingConflict(writer[w].op.key,
                                                         writer[w].file_id)
                                  ELSE FALSE
       IN IF file_overlap_conflict \/ fg_mapping_conflict
          THEN /\ writer' = [writer EXCEPT ![w] = Ready] \* writer aborts due to a conflict
               /\ external_lock' = Nil
          ELSE /\ writer' = [writer EXCEPT ![w].phase  = CommitIndexPhase]
               /\ UNCHANGED external_lock
    /\ UNCHANGED << key_index, timeline_vars, fg_vars, ts_vars,
                    aux_vars >>

(*
    ACTION: UpdateIndexes ------------------------
    
    The writer has passed the concurrency check and
    now begins to the commit phase, which is a two
    step process. In this step, the writer updates
    the indexes to reflect the new state.
    
    The writer then transitions to the CommitTimeline phase.
*)

UpdateIndexes(w) ==
    /\ \/ LockType = NoLock
       \/ external_lock = w
    /\ WriterInState(w, Commit, Inflight, CommitIndexPhase)
    /\ key_index' = key_index \union {writer[w].op.key}
    /\ CommitKeyFileGroupMapping(writer[w].op.key, writer[w].file_id)
    /\ writer' = [writer EXCEPT ![w].phase = CommitTimelinePhase] 
    /\ UNCHANGED << external_lock, file_group, timeline_vars,
                    ts_vars, aux_vars >>

(* 
    ACTION: AppendCommitToTimeline ------------------------
    
    The writer is in the CommitTimeline phase.

    The writer still holds a lock if either optimistic or 
    pessimistic locking is enabled.
    
    The writer writes the completed instant to the timeline
    to commit the operation.
    
    For invariant checking, the key and value is logging in a
    data structure that will be used to compare reads vs
    expected reads.
    
    Finally, the writer wipes all state of the operation 
    and releases the lock (if it held it). 
*)

AppendCommitToTimeline(w) ==
    /\ \/ LockType = NoLock
       \/ external_lock = w
    /\ WriterInState(w, Commit, Inflight, CommitTimelinePhase)
    /\ LET ts      == writer[w].action_ts
           id      == [action_ts    |-> ts,
                       action_type  |-> Commit,
                       action_state |-> Completed]
           instant == [id      |-> id,
                       payload |-> writer[w].new_file_slice]
       IN /\ AppendToTimeline(instant)
          /\ aux_kv_log' = [aux_kv_log EXCEPT ![writer[w].op.key] = Append(@, writer[w].op)]
          /\ writer' = [writer EXCEPT ![w] = Ready]
          /\ external_lock' = Nil
    /\ UNCHANGED << key_index, fg_vars, ts_vars, aux_op_count >>

(*
    ACTION: WriterFail ------------------------
    
    The writer fails and if it held a lock, the 
    lock is released.
    
    Only enabled when TestWriterFailures = TRUE. This is
    done to avoid failures from interfering with the
    liveness checks. When checking liveness, set
    TestWriterFailures = FALSE.
*)

WriterFail(w) ==
    /\ TestWriterFailures = TRUE
    /\ WriterActive(w)
    \* Only fail between updating indexes and the timeline (optionally comment out)
    /\ WriterInState(w, Commit, Inflight, CommitTimelinePhase) 
    /\ writer' = [writer EXCEPT ![w] = Failed]
    /\ IF external_lock = w
       THEN external_lock' = Nil
       ELSE UNCHANGED external_lock
    /\ UNCHANGED << key_index, timeline_vars, fg_vars, 
                    ts_vars, aux_vars >>

\* -------------------------------------------
\* Invariants 
\* -------------------------------------------

\* INV: ConsistentRead --------------------
\* Once a KV pair has been written at a given ts, subsequent
\* reads at that ts should return the same value. Reads at
\* later ts should read this value unless it gets overwritten 
\* by a different value (or deleted) with a higher ts.

Read(key, ts) ==
    LET last_commit == FindLastCommitOfKey(key, ts)
        fs          == LoadFileSlice(last_commit.payload.file_id, 
                                     last_commit.payload.commit_ts)
    IN CASE FileGroupLookup(key) = Nil -> Nil
         [] last_commit = Nil -> Nil 
         [] key \notin DOMAIN fs.entries -> Nil
         [] OTHER -> fs.entries[key]

RECURSIVE ModelReadKey(_,_,_,_)
ModelReadKey(i, seq, ts, value) ==
    CASE i > Len(seq) -> value
      [] seq[i].ts <= ts ->
            IF seq[i].type = Delete
            THEN ModelReadKey(i+1, seq, ts, Nil)
            ELSE ModelReadKey(i+1, seq, ts, seq[i].value)
      [] OTHER -> ModelReadKey(i+1, seq, ts, value)

ModelRead(key, ts) ==
    LET value == ModelReadKey(1, aux_kv_log[key], ts, Nil)
    IN IF value = Nil
       THEN Nil ELSE <<value>> \* make it a sequence like file group storage

ConsistentRead ==
    \A key \in Keys, ts \in 1..MaxActionTs(timeline) :
        LET value == Read(key, ts)
            model_value == ModelRead(key, ts)
        IN value = model_value    
    
\*    \A key \in Keys, ts \in 1..MaxActionTs(timeline) :
\*        LET value == Read(key, ts)
\*            model_value == ModelRead(key, ts)
\*        IN IF value = model_value
\*           THEN TRUE
\*           ELSE /\ PrintT(<<key, ts, value, model_value>>)
\*                /\ FALSE

\* INV: NoDuplicatesInSingleFile ----------------
\* A key is not duplicated in a single file slice.
NoDuplicatesInSingleFile ==
    \A key \in Keys, ts \in 1..MaxActionTs(timeline) :
        ~\E fg1 \in 1..FileGroupCount :
            /\ \E fs_id \in FileSliceIds(fg1) : 
                /\ fs_id.commit_ts = ts
                /\ key \in DOMAIN FileSlice(fg1, fs_id).entries
                /\ Len(FileSlice(fg1, fs_id).entries[key]) > 1

\* INV: NoDuplicatesAcrossFiles --------------------
\* A key is not duplicated across file slices of different
\* file groups.
NoDuplicatesAcrossFiles ==
    \A key \in Keys :
        ~\E fg1, fg2 \in 1..FileGroupCount :
            /\ fg1 # fg2
            /\ \E fs_id \in FileSliceIds(fg1) :
                key \in DOMAIN FileSlice(fg1, fs_id).entries        
            /\ \E fs_id \in FileSliceIds(fg2) :
                key \in DOMAIN FileSlice(fg2, fs_id).entries

\* INV: OneActiveWriterWithPessimisticLock ----------
\* When using pessimistic locking, only one writer
\* should have an active transaction at a time.
OneActiveWriterWithPessimisticLock ==
    IF LockType = PessimisticLock
    THEN ~\E w1, w2 \in Writers :
        /\ w1 # w2
        /\ WriterActive(w1)
        /\ WriterActive(w2)
    ELSE TRUE

TestInv ==
    ~\E w \in Writers : writer[w] = Nil
\*    ~\E a, b \in Writers, ts1, ts2 \in 1..MaxActionTs(timeline) :
\*        /\ a # b
\*        /\ writer[a] = Failed
\*        /\ WriterActive(b)
\*        /\ ts1 # ts2
\*        /\ \E id \in DOMAIN timeline :
\*            /\ id.action_state = Inflight
\*            /\ id.action_ts = ts1
\*        /\ ~\E id \in DOMAIN timeline :
\*            /\ id.action_state = Completed
\*            /\ id.action_ts = ts1
\*        /\ \E id \in DOMAIN timeline :
\*            /\ id.action_state = Completed
\*            /\ id.action_ts = ts2
        
        
        
\*    ~\E a, b \in Writers, x, y \in Keys :
\*        /\ a # b
\*        /\ x # y
\*        /\ writer[a] # Nil
\*        /\ writer[b] = Nil
\*        /\ writer[a].op.key = x
\*        /\ writer[a].op.type # Delete
\*        /\ Len(aux_kv_log[x]) = 0
\*        /\ Len(aux_kv_log[y]) = 1
\*        /\ \E i \in DOMAIN aux_kv_log[y] :
\*            /\ aux_kv_log[y][i].ts = writer[a].action_ts
\*            /\ aux_kv_log[y][i].type # Delete
\*        /\ writer[a].phase = CheckPhase
    
    
\*    IF \E a, b \in Writers :
\*        /\ a # b
\*        /\ writer[a] # Nil
\*        /\ writer[b] # Nil
\*        /\ writer[a].action_ts = writer[b].action_ts 
\*        /\ writer[a].op.key # writer[b].op.key
\*        /\ writer[a].op.type = Insert
\*        /\ writer[b].op.type = Insert
\*        /\ writer[a].phase = CommitPhase
\*        /\ writer[b].phase = CheckPhase
\*    THEN \A key \in Keys, ts \in 1..MaxActionTs(timeline) :
\*            /\ LET value == Read(key, ts)
\*                   model_value == ModelRead(key, ts)
\*               IN IF value # model_value
\*                  THEN PrintT(<<key, ts, value, model_value>>)
\*                  ELSE TRUE
\*            /\ FALSE
\*    ELSE TRUE
            
\*    ~(/\ aux_op_count = 2
\*      /\ \A w \in Writers : writer[w] = Nil
\*      /\ Cardinality(DOMAIN timeline) = 3)
      
      
\*    IF (/\ aux_op_count = 2
\*        /\ \A w \in Writers : writer[w] = Nil
\*        /\ Cardinality(DOMAIN timeline) = 3)
\*    THEN /\ \A key \in Keys, ts \in 1..MaxActionTs :
\*                LET value == Read(key, ts)
\*                    model_value == ModelRead(key, ts)
\*                IN PrintT(<<key, ts, value, model_value, aux_kv_log, timeline, file_group>>)
\*         /\ FALSE      
\*    ELSE TRUE
    
    
\*    ~\E w \in Writers :
\*        \E id \in DOMAIN timeline :
\*            /\ writer[w] # Nil
\*            /\ writer[w].phase = LookupPhase
\*            /\ id.action_ts = writer[w].action_ts
\*            /\ id.action_state = Completed
              
    
EventuallyWriterTerminates ==
    \A w \in Writers :
        WriterActive(w) ~> (\/ writer[w] = Ready
                            \/ writer[w] = Failed)

\* -------------------------------------------
\* Init and Next
\* -------------------------------------------

Init ==
    /\ writer = [w \in Writers |-> Ready]
    /\ external_lock = Nil
    /\ key_index = {}
    /\ TimelineInit
    /\ FgInit(Keys)
    /\ TsInit
    /\ aux_kv_log = [k \in Keys |-> <<>>]
    /\ aux_op_count = 0

Next ==
    \E w \in Writers :
        \/ WriterStartsOp(w)
        \/ ObtainTimestamp(w)
        \/ AppendRequestedToTimeline(w)
        \/ KeyLookup(w)
        \/ WriteFilesReadPhase(w)
        \/ WriteFilesWritePhase(w)
        \/ AcquireOptimisticLock(w)
        \/ ConcurrencyCheck(w)
        \/ UpdateIndexes(w)
        \/ AppendCommitToTimeline(w)
        \/ WriterFail(w)

Fairness ==
    \A w \in Writers :
        /\ WF_vars(WriterStartsOp(w))
        /\ WF_vars(ObtainTimestamp(w))
        /\ WF_vars(AppendRequestedToTimeline(w))
        /\ WF_vars(KeyLookup(w))
        /\ WF_vars(WriteFilesReadPhase(w))
        /\ WF_vars(WriteFilesWritePhase(w))
        /\ WF_vars(AcquireOptimisticLock(w))
        /\ WF_vars(ConcurrencyCheck(w))
        /\ WF_vars(UpdateIndexes(w))
        /\ WF_vars(AppendCommitToTimeline(w))
        
Spec == Init /\ [][Next]_vars
LivenessSpec == Init /\ [][Next]_vars /\ Fairness

===================================================