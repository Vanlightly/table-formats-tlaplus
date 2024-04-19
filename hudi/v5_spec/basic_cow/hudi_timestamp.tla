--------------------------- MODULE hudi_timestamp ---------------------
EXTENDS Integers, Naturals, FiniteSets, Sequences, SequencesExt, TLC,
        hudi_common, hudi_timeline

\* TRUE/FALSE whether to obtain monotonic timestamps from an external provider
CONSTANT MonotonicTs

ASSUME MonotonicTs \in BOOLEAN

\* a timestamp source
VARIABLE ts_prov_ctr

ts_vars == << ts_prov_ctr >>

TsInit == ts_prov_ctr = 0

Timestamps ==
    IF MonotonicTs
    THEN {ts_prov_ctr + 1}
    ELSE 1..(MaxActionTs(timeline)+1)

RecordTs(ts) ==
    IF MonotonicTs
    THEN ts_prov_ctr' = IF ts > ts_prov_ctr THEN ts ELSE ts_prov_ctr
    ELSE UNCHANGED ts_prov_ctr
    
===================================================    