---- MODULE SnapshotTransition ----
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
 Partitioned snapshot durability and the snapshot-to-stream handoff. Volatile
 rows may be replayed after a crash, but the transition cannot occur until
 every source row is covered by a durable partition checkpoint. Streaming
 starts at the exported snapshot boundary and never before it.
***************************************************************************)

CONSTANTS Row1, Row2, Row3, Partition1, Partition2, SnapshotLSN, MaxStreamLSN

ASSUME Cardinality({Row1, Row2, Row3}) = 3
ASSUME Partition1 # Partition2
ASSUME SnapshotLSN \in Nat
ASSUME MaxStreamLSN \in Nat
ASSUME SnapshotLSN <= MaxStreamLSN

Rows == {Row1, Row2, Row3}
Partitions == {Partition1, Partition2}
PartitionOf == [row \in Rows |-> IF row \in {Row1, Row2} THEN Partition1 ELSE Partition2]

Phases == {"Snapshot", "Transition", "Streaming"}
Processes == {"Up", "Crashed"}

VARIABLES phase, process, scanned, durable, streamPosition

vars == <<phase, process, scanned, durable, streamPosition>>

Init ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ scanned = [p \in Partitions |-> {}]
  /\ durable = [p \in Partitions |-> {}]
  /\ streamPosition = 0

ReadSnapshot ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ \E row \in Rows:
       LET partition == PartitionOf[row]
       IN /\ row \notin scanned[partition]
          /\ scanned' = [scanned EXCEPT ![partition] = @ \cup {row}]
  /\ UNCHANGED <<phase, process, durable, streamPosition>>

PersistPartition ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ \E partition \in Partitions:
       /\ durable[partition] # scanned[partition]
       /\ durable' = [durable EXCEPT ![partition] = scanned[partition]]
  /\ UNCHANGED <<phase, process, scanned, streamPosition>>

Crash ==
  /\ process = "Up"
  /\ phase = "Snapshot"
  /\ process' = "Crashed"
  /\ scanned' = durable
  /\ UNCHANGED <<phase, durable, streamPosition>>

Restart ==
  /\ process = "Crashed"
  /\ process' = "Up"
  /\ UNCHANGED <<phase, scanned, durable, streamPosition>>

CompleteSnapshot ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ UNION {durable[p] : p \in Partitions} = Rows
  /\ phase' = "Transition"
  /\ UNCHANGED <<process, scanned, durable, streamPosition>>

StartStreaming ==
  /\ phase = "Transition"
  /\ process = "Up"
  /\ phase' = "Streaming"
  /\ streamPosition' = SnapshotLSN
  /\ UNCHANGED <<process, scanned, durable>>

ReadStream ==
  /\ phase = "Streaming"
  /\ process = "Up"
  /\ streamPosition < MaxStreamLSN
  /\ streamPosition' = streamPosition + 1
  /\ UNCHANGED <<phase, process, scanned, durable>>

Idle ==
  /\ phase = "Streaming"
  /\ streamPosition = MaxStreamLSN
  /\ UNCHANGED vars

Next ==
  \/ ReadSnapshot
  \/ PersistPartition
  \/ Crash
  \/ Restart
  \/ CompleteSnapshot
  \/ StartStreaming
  \/ ReadStream
  \/ Idle

Spec == Init /\ [][Next]_vars

TypeInvariant ==
  /\ phase \in Phases
  /\ process \in Processes
  /\ scanned \in [Partitions -> SUBSET Rows]
  /\ durable \in [Partitions -> SUBSET Rows]
  /\ streamPosition \in 0..MaxStreamLSN

RowsStayInAssignedPartition ==
  \A partition \in Partitions:
    \A row \in scanned[partition]: PartitionOf[row] = partition
DurableRowsWereScanned ==
  \A partition \in Partitions: durable[partition] \subseteq scanned[partition]
TransitionRequiresCompleteSnapshot ==
  phase \in {"Transition", "Streaming"} => UNION {durable[p] : p \in Partitions} = Rows
StreamingStartsAtSnapshotBoundary ==
  phase = "Streaming" => streamPosition >= SnapshotLSN

====
