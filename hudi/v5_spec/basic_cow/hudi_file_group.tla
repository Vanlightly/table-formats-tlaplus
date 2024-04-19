--------------------------- MODULE hudi_file_group ---------------------
EXTENDS Integers, Naturals, FiniteSets, Sequences, SequencesExt, TLC,
        hudi_common

\* The fixed number of file groups.
CONSTANT FileGroupCount

\* Primary key conflict detection
CONSTANT PrimaryKeyConflictCheck

VARIABLES file_group,     \* a map of file_id -> one or more file slices
          fg_key_mapping  \* a map of key -> file_id

ASSUME FileGroupCount \in Nat
ASSUME PrimaryKeyConflictCheck \in BOOLEAN

fg_vars == << file_group, fg_key_mapping >>

\* ------------------------------------------------------------------
\* Types and definitions
\* ------------------------------------------------------------------

BaseFileIdentifier ==
    [file_id: Nat,
     file_write_token: Nat,
     commit_ts: Nat,
     salt: Nat]

BaseFile ==
    [base_file_id: BaseFileIdentifier,
     entries: [Keys -> Values]]

\* ------------------------------------------------------------------
\* Init
\* ------------------------------------------------------------------

FgInit(keys) ==
    \* each file group starts with an empty map of file slices
    /\ file_group     = [fg \in 1..FileGroupCount |-> 
                            [x \in {} |-> Nil]]
    \* no key-fg mappings yet
    /\ fg_key_mapping = [k \in {} |-> 0]

\* ------------------------------------------------------------------
\* HELPER: File group CRUD
\* ------------------------------------------------------------------

FileSliceIds(fg) ==
    DOMAIN file_group[fg]
    
FileSlice(fg, id) ==
    file_group[fg][id]

FileGroupLookup(key) ==
    IF key \notin DOMAIN fg_key_mapping
    THEN Nil
    ELSE fg_key_mapping[key]

PrimaryKeyConflict(key, fg) ==
    /\ PrimaryKeyConflictCheck
    /\ key \in DOMAIN fg_key_mapping
    /\ fg_key_mapping[key] # fg

CommitKeyFileGroupMapping(key, fg) ==
    /\ IF key \in DOMAIN fg_key_mapping
       THEN \* if this happens then primary key conflict detection 
            \* must be disabled. This will orphan a previously written
            \* key by repointing the mapping to a different file group.
            fg_key_mapping' = [fg_key_mapping EXCEPT ![key] = fg]
       ELSE fg_key_mapping' = fg_key_mapping @@ (key :> fg)
    /\ UNCHANGED << file_group >>
    
LoadFileSlice(file_id, ts, salt) ==
    LET fs_id == CHOOSE fs_id \in DOMAIN file_group[file_id] :
                    /\ fs_id.commit_ts = ts
                    /\ fs_id.salt = salt
    IN file_group[file_id][fs_id]

FileSliceAbsentOrNotSupported(file_id, file_slice_id) ==
    \/ ~PutIfAbsentSupported
    \/ file_slice_id \notin DOMAIN file_group[file_id]

PutFileSlice(file_id, file_slice_id, file_slice) ==
    /\ IF file_slice_id \in DOMAIN file_group[file_id]
       THEN file_group' = [file_group EXCEPT ![file_id][file_slice_id] = file_slice]
       ELSE file_group' = [file_group EXCEPT ![file_id] =
                               @ @@ (file_slice_id :> file_slice)]
    /\ UNCHANGED << fg_key_mapping >>

\* ---------------------------------------------
\* HELPER: Merge file slice entries (in-memory)
\* ---------------------------------------------

EmptyEntries ==
    [k \in {} |-> Nil]

ApplyUpsertToFileEntries(entries, op) ==
    IF Cardinality(DOMAIN entries) = 0
    THEN (op.key :> op.value)
    ELSE IF \E key \in DOMAIN entries : key = op.key
            THEN [entries EXCEPT ![op.key] = op.value]
            ELSE entries @@ (op.key :> op.value) 

ApplyDeleteToFileEntries(entries, op) ==
    IF Cardinality(DOMAIN entries) = 0
    THEN EmptyEntries
    ELSE LET new_keys == DOMAIN entries \ {op.key}
         IN [key \in new_keys |-> entries[key]]

MergeFileEntries(fslice, op) ==
    LET entries == IF fslice = Nil THEN EmptyEntries ELSE fslice.entries
    IN
        CASE \/ op.type = Insert
             \/ op.type = Update -> ApplyUpsertToFileEntries(entries, op)
          [] OTHER -> ApplyDeleteToFileEntries(entries, op)
    
===================================================    