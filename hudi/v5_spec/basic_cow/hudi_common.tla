--------------------------- MODULE hudi_common ---------------------------
EXTENDS Naturals

\* model parameters
CONSTANTS Keys,    \* The set of keys that can be written.
          Values,  \* The set of values that can be written.
          ConcurrencyControl \* 0=None, 1=Optimistic, 2=Pessimistic

\* Writer operations
CONSTANTS Upsert,
          Insert,
          Update,
          Delete

\* File/object storage provides PutIfAbsent support
CONSTANT PutIfAbsentSupported

\* TRUE/FALSE whether to add random salt to the instant ids (file names)
CONSTANT UseSalt            

CONSTANT Nil

ASSUME ConcurrencyControl \in Nat

\* CC types
None == 0
Optimistic == 1
Pessimistic == 2

===================================================