--------------------------- MODULE hudi_timeline ---------------------
EXTENDS Integers, Naturals, FiniteSets, Sequences,
        hudi_common, hudi_file_group

\* Action states
CONSTANTS Requested,
          Inflight,
          Completed

\* Action types (only commit modeled)
CONSTANT Commit

VARIABLES timeline \* the timeline, a bag of files with ordering precedence

timeline_vars == << timeline >>

TimelineInit ==
    timeline = [x \in {} |-> 0] \* an empty map

\* ------------------------------------------------------------------
\* Types and definitions
\* ------------------------------------------------------------------

States == { Requested, Inflight, Completed }
Actions == { Commit }

InstantIdentifier ==
    [action_ts: Nat,
     action_type: Actions,
     action_state: States,
     salt: Nat]

CommitPayload ==
    [file_id: Nat,
     commit_ts: Nat,
     write_token: Nat,
     salt: Nat]
     
Instant ==
    [id: InstantIdentifier,
     payload: CommitPayload \union {Nil}] 

LoadTimeline ==
    timeline

\* ------------------------------------------------------------------
\* Timeline timestamps
\* ------------------------------------------------------------------

MaxCommitTs(tl) ==
    (* If the timeline is empty or has no completed transactions
      then return 0. Else, choose the action timestamp of the
      most recent completed instant.*)
    IF \/ Cardinality(DOMAIN tl) = 0
       \/ ~\E id \in DOMAIN tl : 
            /\ id.action_state = Completed
    THEN 0
    ELSE LET id == CHOOSE id \in DOMAIN tl :
                        /\ id.action_state = Completed
                        /\ ~\E id1 \in DOMAIN tl :
                            /\ id1 # id
                            /\ id1.action_state = Completed
                            /\ \/ id1.action_ts > id.action_ts
                               \/ /\ id1.action_ts = id.action_ts
                                  /\ \/ /\ UseSalt = TRUE
                                        /\ id1.salt > id.salt
                                     \/ UseSalt = FALSE
        IN id.action_ts

MaxActionTs(tl) ==
    IF Cardinality(DOMAIN tl) = 0
    THEN 0
    ELSE LET id == CHOOSE id \in DOMAIN tl :
                     \A id1 \in DOMAIN tl :
                         id.action_ts >= id1.action_ts
         IN id.action_ts

\* ---------------------------------------------
\* Timeline read and write functions
\* ---------------------------------------------

AbsentOrNotSupported(instant) ==
    \/ ~PutIfAbsentSupported
    \/ instant.id \notin DOMAIN timeline

TimelinePut(instant) ==
    IF instant.id \in DOMAIN timeline
    THEN [timeline EXCEPT ![instant.id] = instant]
    ELSE timeline @@ (instant.id :> instant)
  
AppendToTimeline(instant) ==
    timeline' = TimelinePut(instant) 

IsCandidateMergeTarget(tl, id, file_id, merge_ts) ==
    /\ id.action_state = Completed
    /\ id.action_ts <= merge_ts
    /\ tl[id].payload # Nil
    /\ tl[id].payload.file_id = file_id
    
FindMergeTargetId(tl, file_id, merge_ts) ==
    IF ~\E id \in DOMAIN tl : 
        IsCandidateMergeTarget(tl, id, file_id, merge_ts)
    THEN Nil \* no existing file slice to merge
    ELSE CHOOSE id \in DOMAIN tl :
                /\ IsCandidateMergeTarget(tl, id, file_id, merge_ts)
                \* there is no other instant that is more recent
                \* which also touches the same file_id
                /\ ~\E id1 \in DOMAIN tl :
                    /\ id1 # id
                    /\ IsCandidateMergeTarget(tl, id1, file_id, merge_ts)
                    /\ IF UseSalt = TRUE
                       THEN \/ id1.action_ts > id.action_ts
                            \/ /\ id1.action_ts = id.action_ts
                               /\ id1.salt > id.salt
                       ELSE id1.action_ts > id.action_ts

\* ---------------------------------------------
\* Conflict detection
\* ---------------------------------------------

ConflictExists(tl, file_id, ts_orig_fslice) ==
    (* There is an instant with a higher timestamp than the original
       file slice (the merge target) which also touches the same file id *)
    \E id \in DOMAIN tl :
        /\ id.action_state = Completed
        /\ tl[id].payload.file_id = file_id
        /\ tl[id].payload.commit_ts > ts_orig_fslice
       
===================================================       