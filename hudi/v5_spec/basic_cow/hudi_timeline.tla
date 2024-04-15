--------------------------- MODULE hudi_timeline ---------------------
EXTENDS Integers, Naturals, FiniteSets, Sequences,
        hudi_common, hudi_file_group

\* Action states
CONSTANTS Requested,
          Inflight,
          Completed

\* Action types (only commit modeled)
CONSTANT Commit

VARIABLES timeline,    \* the timeline, a bag of files with ordering precedence
          timeline_seq \* a projection of the timeline instant file names, in order

timeline_vars == << timeline, timeline_seq >>

TimelineInit ==
    /\ timeline = [x \in {} |-> 0] \* an empty map
    /\ timeline_seq = <<>>

\* ------------------------------------------------------------------
\* Types and definitions
\* ------------------------------------------------------------------

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

LoadTimeline ==
    timeline

\* ------------------------------------------------------------------
\* Timeline timestamps
\* ------------------------------------------------------------------

MaxCommitTs(tl) ==
    (*If the timeline is empty or has no completed transactions
      then return 0. Else, choose the action timestamp of the
      most recent completed instant.*)
    IF \/ Cardinality(DOMAIN tl) = 0
       \/ ~\E id \in DOMAIN tl : 
            id.action_state = Completed
    THEN 0
    ELSE LET id == CHOOSE id \in DOMAIN tl :
                        /\ id.action_state = Completed
                        /\ ~\E id1 \in DOMAIN tl :
                            /\ id1.action_state = Completed
                            /\ id1.action_ts > id.action_ts
         IN id.action_ts

MaxActionTs(tl) ==
    IF Cardinality(DOMAIN tl) = 0
    THEN 0
    ELSE LET id == CHOOSE id \in DOMAIN tl :
                     \A id1 \in DOMAIN tl :
                         id.action_ts >= id1.action_ts
         IN id.action_ts

\* ------------------------------------------------------------------
\* Timeline sort
\* ------------------------------------------------------------------

Prededence(state) ==
    CASE state = Requested -> 1
      [] state = Inflight -> 2
      [] OTHER -> 3

CompareState(s1, s2) ==
    \* Requested > Inflight > Completed
    LET s1_pre == Prededence(s1)
        s2_pre == Prededence(s2)
    IN s1_pre < s2_pre

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
\* Timeline read and write functions
\* ---------------------------------------------

TimelinePut(instant) ==
    IF instant.id \in DOMAIN timeline
    THEN [timeline EXCEPT ![instant.id] = instant]
    ELSE timeline @@ (instant.id :> instant)
  
AppendToTimeline(instant) ==
    LET new_timeline == TimelinePut(instant)
    IN /\ timeline' = new_timeline 
       /\ timeline_seq' = Sort(new_timeline, {}, <<>>)

IsMergeTarget(tl, id, file_id, merge_ts) ==
    /\ id.action_state = Completed
    /\ id.action_ts <= merge_ts
    /\ tl[id].payload # Nil
    /\ tl[id].payload.file_id = file_id
    
FindMergeTargetId(tl, file_id, merge_ts) ==
    IF ~\E id \in DOMAIN tl : IsMergeTarget(tl, id, file_id, merge_ts)
    THEN Nil
    ELSE CHOOSE id \in DOMAIN tl :
                /\ IsMergeTarget(tl, id, file_id, merge_ts)
                /\ ~\E id1 \in DOMAIN tl :
                    /\ IsMergeTarget(tl, id1, file_id, merge_ts)
                    /\ id1.action_ts > id.action_ts

\* ---------------------------------------------
\* Conflict detection
\* ---------------------------------------------

ConflictExists(tl, file_id, ts_orig_fslice, ts_new_fslice) ==
    \E id \in DOMAIN tl :
        /\ id.action_state = Completed
        /\ tl[id].payload.file_id = file_id
        /\ tl[id].payload.commit_ts > ts_orig_fslice

\* ---------------------------------------------
\* Timeline scan for last commit of a given key
\* for read operations.
\* ---------------------------------------------

IsMatch(id, key, ts) ==
    /\ \/ ts = 0 \* when ts=0, we search for any instant of this key
       \/ id.action_ts <= ts
    /\ id.action_type = Commit
    /\ id.action_state = Completed 
    /\ timeline[id].payload.file_id = FileGroupLookup(key)

RECURSIVE FindLastCommitIdInTimeline(_,_,_,_)
FindLastCommitIdInTimeline(i, key, ts, id) ==
    CASE i > Len(timeline_seq) -> id
      [] IsMatch(timeline_seq[i], key, ts) -> 
                FindLastCommitIdInTimeline(
                    i+1, key, ts, 
                    timeline_seq[i])
      [] OTHER -> FindLastCommitIdInTimeline(i+1, key, ts, id)

FindLastCommitOfKey(key, ts) ==
    LET id == FindLastCommitIdInTimeline(1, key, ts,  Nil)
    IN IF id = Nil THEN Nil
       ELSE timeline[id]
       
===================================================       