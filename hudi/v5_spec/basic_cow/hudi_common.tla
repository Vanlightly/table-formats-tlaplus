--------------------------- MODULE hudi_common ---------------------------
EXTENDS Naturals

\* model parameters
CONSTANTS Keys,    \* The set of keys that can be written.
          Values,  \* The set of values that can be written.
          LockType \* 0=No Lock, 1=Optimistic, 2=Pessimistic

\* Writer operations
CONSTANTS Upsert,
          Insert,
          Update,
          Delete
          
CONSTANT Nil

ASSUME LockType \in Nat

NoLock == 0
OptimisticLock == 1
PessimisticLock == 2

===================================================