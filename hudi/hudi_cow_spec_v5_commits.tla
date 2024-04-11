---------------------- MODULE hudi_cow_spec_v5_commits ----------------------
(*
Author: Jack Vanlightly

This specification is based on v5 of the Hudi spec (https://hudi.apache.org/tech-specs/).

The specification has limited functionality. See the hudi_cow_spec_v5_commits_readme.md 
for notes.
*)


EXTENDS Integers, Naturals, FiniteSets, Sequences, SequencesExt, TLC

CONSTANTS Writers,              \* The set of writers.
          Keys,                 \* The set of keys that can be written.
          Values,               \* The set of values that can be written.
          FileGroupCount,       \* The fixed number of file groups.
          MonotonicTsProvider,  \* TRUE/FALSE whether to obtain monotonic timestamps from an external provider
          LockType              \* 0=No Lock, 1=Optimistic, 2=Pessimistic
                            

\* Action states
CONSTANTS Requested,
          Inflight,
          Completed

\* Action types (only commit modeled)
CONSTANTS Commit

\* Operations
CONSTANTS Upsert,
          Delete

\* Internal states of the writer, not part of the Hudi spec
\* A writer goes through a series of steps to carry out
\* a transaction, modeled as a simple state machine.
CONSTANTS RequestPhase, \* The writer has obtained a timestamp
          ReadPhase,    \* The writer has written a Requested instant to the timeline
          WritePhase,   \* The writer has read any relevant file slice to merge, and
                        \* written the Inflight instant to the timeline.
          CheckPhase,   \* The writer has written the base files.
          CommitPhase   \* The concurrency check passed and the writer can now
                        \* write the Completed instant to the timeline.

\* Other constants
CONSTANTS Nil

ASSUME FileGroupCount \in Nat
ASSUME MonotonicTsProvider \in BOOLEAN
ASSUME LockType \in Nat

VARIABLES writer,           \* writer state
          timeline,         \* the timeline, a bag of files with ordering precedence
          timeline_seq,     \* a projection of the timeline instant file names, in order
          file_group,       \* a map of file_id to one or more file slices
          external_lock,    \* an external lock provider
          ts_source         \* a timestamp source

\* auxilliary variables (not part of the Hudi spec)
VARIABLES aux_kv_log,       \* a log of the expected values for each key, used in invariants
          aux_fg_key,       \* the mapping of key -> file_id
          aux_written,      \* the set of KV pairs that commits associated with them
          aux_deleted       \* the keys which have delete ops associated with them

vars == << writer, timeline, timeline_seq, file_group, external_lock, ts_source,
           aux_kv_log, aux_fg_key, aux_written, aux_deleted >>

\* ---------------------------------------------
\* Types, definitions 
\* ---------------------------------------------

NoLock == 0
OptimisticLock == 1
PessimisticLock == 2

States == { Requested, Inflight, Completed }
Actions == { Commit }

InstantIdentifier ==
    [action_ts: Nat,
     action_type: Actions,
     action_state: States]

CommitPayload ==
    [file_id: Nat,
     commit_ts: Nat]
     
Instant ==
    [id: InstantIdentifier,
     payload: CommitPayload \union {Nil}]
     
KvEntry ==
    [key: Keys,
     value: Values]

WriterOp ==
    [type: {Upsert,Delete},
     ts: Nat,
     key: Keys,
     value: Values]

BaseFileIdentifier ==
    [file_id: Nat,
     file_write_token: Nat,
     commit_ts: Nat]

BaseFile ==
    [base_file_id: BaseFileIdentifier,
     entries: SeqOf(KvEntry, Cardinality(Keys))]


\* ---------------------------------------------
\* HELPER: Sorting the bag of files in the timeline
\* ---------------------------------------------

CompareState(s1, s2) ==
    \* Requested > Inflight > Completed
    CASE s1 = s2 -> FALSE
      [] /\ s1 = Requested
         /\ s2 # Requested -> TRUE
      [] /\ s2 = Requested
         /\ s1 # Requested -> FALSE
      [] /\ s1 = Inflight
         /\ s2 # Inflight -> TRUE
      [] /\ s2 = Inflight
         /\ s1 # Inflight -> FALSE
      [] OTHER -> FALSE

CompareInstant(a, b) ==
    CASE a.action_ts < b.action_ts -> TRUE
      [] a.action_ts > b.action_ts -> FALSE
      [] OTHER -> CompareState(a.action_state, 
                               b.action_state) 

RECURSIVE Sort(_,_,_)
Sort(new_timeline, done, seq) ==
    IF Cardinality(done) = Cardinality(DOMAIN new_timeline)
    THEN seq
    ELSE LET next == CHOOSE id \in DOMAIN new_timeline :
                        /\ id \notin done
                        /\ ~\E id1 \in DOMAIN new_timeline :
                            /\ id1 \notin done
                            /\ CompareInstant(id1, id)
             next_done == done \union {next}
             next_seq  == Append(seq, next)
         IN Sort(new_timeline, next_done, next_seq) 

\* ---------------------------------------------
\* HELPER: File PUT (file slice and timeline instants)
\* ---------------------------------------------

FileGroupPut(file_id, file_slice_id, file_slice) ==
    IF file_slice_id \in DOMAIN file_group[file_id]
    THEN [file_group EXCEPT ![file_id][file_slice_id] = file_slice]
    ELSE [file_group EXCEPT ![file_id] =
            @ @@ (file_slice_id :> file_slice)]

TimelinePut(instant) ==
    IF instant.id \in DOMAIN timeline
    THEN [timeline EXCEPT ![instant.id] = instant]
    ELSE timeline @@ (instant.id :> instant)
  
AppendToTimeline(instant) ==
    LET new_timeline == TimelinePut(instant)
    IN /\ timeline' = new_timeline 
       /\ timeline_seq' = Sort(new_timeline, {}, <<>>)  

WriterInState(w, action_type, action_state, phase) ==
    /\ writer[w] # Nil
    /\ writer[w].instant # Nil
    /\ writer[w].instant.id.action_type = action_type
    /\ writer[w].instant.id.action_state = action_state
    /\ writer[w].phase = phase

\* ---------------------------------------------
\* HELPER: Other
\* ---------------------------------------------

FileGroupOf(key) ==
    aux_fg_key[key]
    
LockCheck(w) ==
    IF LockType = PessimisticLock
    THEN external_lock = w
    ELSE TRUE

\* ---------------------------------------------
\* ACTIONS
\* ---------------------------------------------

(* 
    ACTION: AcquirePessimisticLock ------------------
    
    
*)

AcquirePessimisticLock(w) ==
    /\ LockType = PessimisticLock
    /\ writer[w] = Nil
    /\ external_lock = Nil
    /\ external_lock' = w
    /\ UNCHANGED << writer, timeline, timeline_seq, file_group, ts_source,
                    aux_kv_log, aux_fg_key, aux_written, aux_deleted >>
    

(* 
    ACTION: ObtainTimestamp ------------------
    
    The writer obtains two timestamps:
    1. An action timestamp, that it's commit will use.
    2. A merge commit timestamp, corresponding to the
       last action timestamp of a completed commit
       instant in the timeline.
    
    When MonotonicTs is true, the obtained action 
    timestamp is guaranteed to be monotonic. Otherwise, 
    the timestamp may or may not be monotonic, and may
    even be a duplicate (resulting in a timestamp
    collision).
    
    The merge commit timestamp, may or may not collide
    with another writer's merge commit timestamp. Whether
    or not this matters depends on whether there are 
    overlapping files between the transactions and whether
    an optimistic lock is used later on.
*)

GetMaxCommitTs ==
    (*If the timeline is empty or has no completed transactions
      then return 0. Else, choose the action timestamp of the
      most recent completed instant.*)
    IF \/ Cardinality(DOMAIN timeline) = 0
       \/ ~\E id \in DOMAIN timeline : 
            id.action_state = Completed
    THEN 0
    ELSE LET id == CHOOSE id \in DOMAIN timeline :
                        /\ id.action_state = Completed
                        /\ ~\E id1 \in DOMAIN timeline :
                            /\ id1.action_state = Completed
                            /\ id1.action_ts > id.action_ts
         IN id.action_ts

MaxTimelineTs ==
    IF Cardinality(DOMAIN timeline) = 0
    THEN 0
    ELSE LET id == CHOOSE id \in DOMAIN timeline :
                     \A id1 \in DOMAIN timeline :
                         id.action_ts >= id1.action_ts
         IN id.action_ts

Timestamps ==
    CASE \*If monotonic then use an external ts source to get a monotonic ts
         MonotonicTsProvider = TRUE -> {ts_source + 1}
         \* If this is a pessimistic lock scenario return a single ts
         \* that is equal to the highest in the timeline + 1
      [] LockType = PessimisticLock -> {MaxTimelineTs+1}
         \* Else return multiple ts between 1 and a monotonic value.
      [] OTHER -> 1..(ts_source + 1)
      
ObtainTimestamp(w) ==
    /\ LockCheck(w)
    /\ writer[w] = Nil \* The writer has no state for an ongoing transaction
    /\ \E ts \in Timestamps :
        /\ writer' = [writer EXCEPT ![w] = 
                        [action_ts        |-> ts,
                         merge_commit_ts  |-> GetMaxCommitTs,
                         phase            |-> RequestPhase,
                         instant          |-> Nil,
                         op               |-> Nil,
                         merge_file_id    |-> Nil,
                         merge_file_slice |-> Nil,
                         new_file_slice   |-> Nil]]
        /\ ts_source' = IF ts > ts_source THEN ts ELSE ts_source
        /\ UNCHANGED << timeline, timeline_seq, file_group, external_lock,
                        aux_kv_log, aux_fg_key, aux_written, aux_deleted >>

(* 
    ACTION: AppendUpsertRequestedToTimeline &
            AppendDeleteRequestedToTimeline ------------------
    
    The writer wants to either:
        * Write a key-value pair (that has not yet been written)
        * Delete a key (that has been committed)
    
    Using the obtained action timestamp, the writer performs a
    timeline put of an instant. The instant id corresponds to
    its file name, and the instant as a whole is the file.
    
    The writer stores the following internal state in-memory:
        * The instant
        * The operation (upsert/delete, the key/valye, the timestamp)
        * Sets the writer phase to ReadPhase.  
*)

AppendRequestedToTimeline(w, key, value, op_type) ==
    /\ LockCheck(w)
    /\ writer[w] # Nil
    /\ writer[w].phase = RequestPhase
    /\ LET id        == [action_ts    |-> writer[w].action_ts,
                         action_type  |-> Commit,
                         action_state |-> Requested]
           instant   == [id      |-> id,
                         payload |-> Nil]
       IN /\ AppendToTimeline(instant)
          /\ writer' = [writer EXCEPT ![w].phase   = ReadPhase,
                                      ![w].instant = instant,
                                      ![w].op      = [type  |-> op_type,
                                                      ts    |-> id.action_ts,
                                                      key   |-> key,
                                                      value |-> value],
                                      ![w].merge_file_id = FileGroupOf(key)]      

AppendUpsertRequestedToTimeline(w) ==
    /\ writer[w] # Nil
    /\ writer[w].phase = RequestPhase
    /\ \E key \in Keys, value \in Values :
        /\ ~\E written \in aux_written : 
            /\ written.key = key
            /\ written.value = value
        /\ AppendRequestedToTimeline(w, key, value, Upsert)
        /\ aux_written' = aux_written \union {[key   |-> key,
                                               value |-> value]}
    /\ UNCHANGED << file_group, external_lock, ts_source, 
                    aux_kv_log, aux_deleted, aux_fg_key >>

AppendDeleteRequestedToTimeline(w) ==
    /\ writer[w] # Nil
    /\ writer[w].phase = RequestPhase
    /\ \E written \in aux_written :
        /\ written.key \notin aux_deleted
        /\ AppendRequestedToTimeline(w, written.key, Nil, Delete)
        /\ aux_deleted' = aux_deleted \union {written.key}
    /\ UNCHANGED << file_group, external_lock, ts_source, 
                    aux_kv_log, aux_written, aux_fg_key >>    


(* 
    ACTION: WriteFilesReadPhase ------------------
    
    The writer is in the ReadPhase, who just wrote a
    commit instant in the Requested state.
    
    The writer scans the timeline and determines which
    file slice it must merge. The writer starts the merge
    process by reading the contents of the file slice
    with the highest timestamp <= the action timestamp
    of the writer operation. The writer stores the contents
    in memory.
*)

IsCompletedFileMatch(id, file_id) ==
    /\ id.action_state = Completed
    /\ timeline[id].payload # Nil
    /\ timeline[id].payload.file_id = file_id

FileSliceToMerge(file_id, merge_ts) ==
    IF ~\E id \in DOMAIN timeline : 
            /\ id.action_ts <= merge_ts
            /\ IsCompletedFileMatch(id, file_id)
    THEN Nil
    ELSE LET merge_id == CHOOSE id \in DOMAIN timeline :
                            /\ id.action_ts <= merge_ts
                            /\ IsCompletedFileMatch(id, file_id)
                            /\ ~\E id1 \in DOMAIN timeline :
                                /\ id1.action_ts <= merge_ts
                                /\ IsCompletedFileMatch(id1, file_id)
                                /\ id1.action_ts > id.action_ts
             fs_id    == CHOOSE fs_id \in DOMAIN file_group[file_id] :
                            fs_id.commit_ts = merge_id.action_ts
         IN file_group[file_id][fs_id]

WriteFilesReadPhase(w) ==
    /\ LockCheck(w)
    /\ WriterInState(w, Commit, Requested, ReadPhase)
    /\ LET ts      == writer[w].action_ts
           fslice  == FileSliceToMerge(writer[w].merge_file_id, 
                                       writer[w].merge_commit_ts)
           id      == [action_ts    |-> ts,
                       action_type  |-> Commit,
                       action_state |-> Inflight]
           instant == [id      |-> id,
                       payload |-> Nil]
       IN /\ AppendToTimeline(instant)
          /\ writer' = [writer EXCEPT ![w].instant = instant,
                                      ![w].merge_file_slice = fslice,
                                      ![w].phase = WritePhase]
    /\ UNCHANGED << file_group, external_lock, ts_source, 
                    aux_kv_log, aux_written, aux_deleted, aux_fg_key >>  

(* 
    ACTION: WriteFilesWritePhase ------------------
    
    The writer is in the WritePhase, who just wrote a
    commit instant in the Inflight state.
    
    The writer merges the upsert/delete operation
    with the stored file slice and writes a new
    file slice with the id of:
        * file_id (the file group)
        * write_token (always 1 in this spec as write failures are not modeled)
        * commit_ts (the action timestamp of this writer op)
        
    The id corresponds to the file slice filename. 
*)

ApplyUpsertToFileEntries(fslice, op) ==
    IF fslice = Nil
    THEN (op.key :> op.value)
    ELSE IF \E key \in DOMAIN fslice.entries : key = op.key
            THEN [fslice.entries EXCEPT ![op.key] = op.value]
            ELSE fslice.entries @@ (op.key :> op.value) 

ApplyDeleteToFileEntries(fslice, op) ==
    IF fslice = Nil
    THEN <<>>
    ELSE LET new_keys == DOMAIN fslice.entries \ {op.key}
         IN [key \in new_keys |-> fslice.entries[key]]

NewBaseFileEntries(fslice, op) ==
    IF op.type = Upsert
    THEN ApplyUpsertToFileEntries(fslice, op)
    ELSE ApplyDeleteToFileEntries(fslice, op)

WriteFilesWritePhase(w) ==
    /\ LockCheck(w)
    /\ WriterInState(w, Commit, Inflight, WritePhase)
    /\ LET commit_ts  == writer[w].action_ts
           file_id    == writer[w].merge_file_id
           file_slice_id == [file_id     |-> file_id,
                             write_token |-> 1,
                             commit_ts   |-> commit_ts] 
           file_slice == [base_file_id |-> file_slice_id,
                          entries      |-> NewBaseFileEntries(
                                                writer[w].merge_file_slice,
                                                writer[w].op)]
       IN /\ file_group' = FileGroupPut(file_id, file_slice_id, file_slice)
          /\ writer' = [writer EXCEPT ![w].new_file_slice = [file_id   |-> file_id,
                                                             commit_ts |-> commit_ts],
                                      ![w].phase          = CheckPhase]
    /\ UNCHANGED << timeline, timeline_seq, external_lock, ts_source, 
                    aux_kv_log, aux_written, aux_deleted, aux_fg_key >>  


(* 
    ACTION: AcquireLock ------------------
    
    This action can only be enabled when UseOptimisticLock = TRUE.
    
    The writer acquires the lock only if it is free (set to Nil).
    Acquiring the lock consists of setting it to the identity
    of the writer.
*)

AcquireOptimisticLock(w) ==
    /\ LockType = OptimisticLock
    /\ external_lock = Nil
    /\ WriterInState(w, Commit, Inflight, CheckPhase)
    /\ external_lock' = w
    /\ UNCHANGED << writer, timeline, timeline_seq, file_group, ts_source,
                    aux_kv_log, aux_fg_key, aux_written, aux_deleted >>

(* 
    ACTION: ConcurrencyCheck ------------------
        
    While still holding the lock (or UseOptimisticLock = FALSE),
    the writer scans the timeline. If the writer finds a completed
    instant that contains a file slice with a commit timestamp
    that is greater than the merge commit timestamp of the writer
    op, then the writer aborts. In this spec, the writer does not
    clean up any files.
    
    If the check finds no conflicting instants in the timeline,
    the writer transitions to the Commit writer phase.
*)

ConcurrencyCheck(w) ==
    /\ \/ LockType = NoLock
       \/ external_lock = w
    /\ WriterInState(w, Commit, Inflight, CheckPhase)
    /\ IF \E id \in DOMAIN timeline :
            /\ id.action_state = Completed
            /\ timeline[id].payload.file_id = writer[w].new_file_slice.file_id
            /\ timeline[id].payload.commit_ts > writer[w].merge_commit_ts
       THEN writer' = [writer EXCEPT ![w] = Nil] \* writer aborts
       ELSE writer' = [writer EXCEPT ![w].phase  = CommitPhase]
    /\ UNCHANGED << timeline, timeline_seq, file_group, external_lock, ts_source,
                    aux_kv_log, aux_fg_key, aux_written, aux_deleted >>

(* 
    ACTION: AppendCommitToTimeline ------------------
    
    The writer still holds the lock (or UseOptimisticLock = FALSE),
    and is in the Commit writer phase.
    
    The writer performs a PUT of the completed instant 
    to the timeline to commit the operation.
    
    For invariant checking, the key and value is logging in a
    data structure that will be used to compare reads vs
    expected reads.
    
    The writer wipes all state of the operation and releases
    the lock.  
*)

AppendCommitToTimeline(w) ==
    /\ \/ LockType = NoLock
       \/ external_lock = w
    /\ WriterInState(w, Commit, Inflight, CommitPhase)
    /\ LET ts      == writer[w].instant.id.action_ts
           id      == [action_ts    |-> ts,
                       action_type  |-> Commit,
                       action_state |-> Completed]
           instant == [id      |-> id,
                       payload |-> writer[w].new_file_slice]
       IN /\ AppendToTimeline(instant)
          /\ aux_kv_log' = [aux_kv_log EXCEPT ![writer[w].op.key] = Append(@, writer[w].op)]
          /\ writer' = [writer EXCEPT ![w] = Nil]
          /\ external_lock' = Nil
    /\ UNCHANGED << file_group, ts_source,
                    aux_written, aux_deleted, aux_fg_key >>

\* -------------------------------------------
\* Invariants --------------------------------

IsMatch(id, key, ts, max_ts) ==
    /\ id.action_ts > max_ts
    /\ id.action_ts <= ts
    /\ id.action_type = Commit
    /\ id.action_state = Completed 
    /\ timeline[id].payload.file_id = FileGroupOf(key)

RECURSIVE FindLastCommitIdInTimeline(_,_,_,_,_)
FindLastCommitIdInTimeline(i, key, ts, max_ts, id) ==
    CASE i > Len(timeline_seq) -> id
      [] IsMatch(timeline_seq[i], key, ts, max_ts) -> 
                FindLastCommitIdInTimeline(
                    i+1, key, ts, 
                    timeline_seq[i].action_ts, 
                    timeline_seq[i])
      [] OTHER -> FindLastCommitIdInTimeline(i+1, key, ts, max_ts, id)

Read(key, ts) ==
    LET last_commit_id == FindLastCommitIdInTimeline(1, key, ts, 0, Nil)
        payload   == timeline[last_commit_id].payload
        fs_id     == CHOOSE fs_id \in DOMAIN file_group[payload.file_id] :
                        fs_id.commit_ts = payload.commit_ts
        fs        == file_group[payload.file_id][fs_id]
    IN CASE last_commit_id = Nil -> Nil 
         [] key \notin DOMAIN fs.entries -> Nil
         [] OTHER -> fs.entries[key]

RECURSIVE ModelReadKey(_,_,_,_)
ModelReadKey(i, seq, ts, value) ==
    CASE i > Len(seq) -> value
      [] seq[i].ts <= ts ->
            IF seq[i].type = Upsert
            THEN ModelReadKey(i+1, seq, ts, seq[i].value)
            ELSE ModelReadKey(i+1, seq, ts, Nil)
      [] OTHER -> ModelReadKey(i+1, seq, ts, value)

ModelRead(key, ts) ==
    ModelReadKey(1, aux_kv_log[key], ts, Nil)

AllCommitTs ==
    LET ids == {id \in DOMAIN timeline : id.action_state = Completed}
    IN { id.action_ts : id \in ids}    

\* TODO: What to read for ts=1, when ts=2 completed, but ts=1 is in-prog?
ConsistentRead ==
    \A key \in Keys, ts \in 1..ts_source :
        LET value == Read(key, ts)
            model_value == ModelRead(key, ts)
        IN value = model_value

OneActiveWriterWithPessimisticLock ==
    IF LockType = PessimisticLock
    THEN ~\E w1, w2 \in Writers :
        /\ w1 # w2
        /\ writer[w1] # Nil
        /\ writer[w2] # Nil
    ELSE TRUE

TestInv ==
    TRUE 

\* Init and Next --------------------

EmptyMap == [x \in {} |-> Nil]

Init ==
    LET keyseq == SetToSeq(Keys)
    IN 
        /\ writer = [w \in Writers |-> Nil]
        /\ timeline = EmptyMap
        /\ timeline_seq = <<>>
        /\ file_group = [fg \in 1..FileGroupCount |-> EmptyMap]
        /\ external_lock = Nil
        /\ ts_source = 0
        /\ aux_fg_key = [k \in Keys |-> 
                            LET index == IndexFirstSubSeq(<<k>>, keyseq)
                            IN (index % FileGroupCount) + 1]
        /\ aux_kv_log = [k \in Keys |-> <<>>]
        /\ aux_written = {}
        /\ aux_deleted = {}

Next ==
    \E w \in Writers :
        \/ AcquirePessimisticLock(w)
        \/ ObtainTimestamp(w)
        \/ AppendUpsertRequestedToTimeline(w)
        \/ AppendDeleteRequestedToTimeline(w)
        \/ WriteFilesReadPhase(w)
        \/ WriteFilesWritePhase(w)
        \/ AcquireOptimisticLock(w)
        \/ ConcurrencyCheck(w)
        \/ AppendCommitToTimeline(w)


===================================================