---- MODULE SnapshotTransition ----
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************
 Slot-anchored, partitioned snapshot durability and the atomic handoff to
 streaming. A task retry may resume while the same exporter process remains
 live. Process replacement loses that exporter, invalidates every cursor in
 the bootstrap generation, and forces a complete new generation. A published
 handoff uses the slot consistent point exactly.
***************************************************************************)

CONSTANTS Row1, Row2, Row3, Partition1, Partition2, SnapshotLSN,
          MaxStreamLSN, MaxBootstrapGeneration

ASSUME Cardinality({Row1, Row2, Row3}) = 3
ASSUME Partition1 # Partition2
ASSUME SnapshotLSN \in Nat
ASSUME MaxStreamLSN \in Nat
ASSUME SnapshotLSN <= MaxStreamLSN
ASSUME MaxBootstrapGeneration \in Nat \ {0}

Rows == {Row1, Row2, Row3}
Partitions == {Partition1, Partition2}
PartitionOf == [row \in Rows |-> IF row \in {Row1, Row2} THEN Partition1 ELSE Partition2]

Phases == {"Snapshot", "Transition", "Streaming"}
Processes == {"Up", "Crashed"}

VARIABLES phase, process, exporterAlive, bootstrapGeneration,
          scanned, durable, durableGeneration, publishedGeneration,
          streamPosition

vars == <<phase, process, exporterAlive, bootstrapGeneration,
          scanned, durable, durableGeneration, publishedGeneration,
          streamPosition>>

Init ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ exporterAlive = TRUE
  /\ bootstrapGeneration = 1
  /\ scanned = [p \in Partitions |-> {}]
  /\ durable = [p \in Partitions |-> {}]
  /\ durableGeneration = 1
  /\ publishedGeneration = 0
  /\ streamPosition = 0

ReadSnapshot ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ exporterAlive
  /\ \E row \in Rows:
       LET partition == PartitionOf[row]
       IN /\ row \notin scanned[partition]
          /\ scanned' = [scanned EXCEPT ![partition] = @ \cup {row}]
  /\ UNCHANGED <<phase, process, exporterAlive, bootstrapGeneration,
                  durable, durableGeneration, publishedGeneration,
                  streamPosition>>

PersistPartition ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ exporterAlive
  /\ \E partition \in Partitions:
       /\ durable[partition] # scanned[partition]
       /\ durable' = [durable EXCEPT ![partition] = scanned[partition]]
  /\ durableGeneration' = bootstrapGeneration
  /\ UNCHANGED <<phase, process, exporterAlive, bootstrapGeneration,
                  scanned, publishedGeneration, streamPosition>>

CrashTask ==
  /\ process = "Up"
  /\ phase = "Snapshot"
  /\ exporterAlive
  /\ process' = "Crashed"
  /\ scanned' = durable
  /\ UNCHANGED <<phase, exporterAlive, bootstrapGeneration, durable,
                  durableGeneration, publishedGeneration, streamPosition>>

RestartTaskSameExporter ==
  /\ process = "Crashed"
  /\ exporterAlive
  /\ process' = "Up"
  /\ UNCHANGED <<phase, exporterAlive, bootstrapGeneration, scanned, durable,
                  durableGeneration, publishedGeneration, streamPosition>>

LoseExporter ==
  /\ phase = "Snapshot"
  /\ exporterAlive
  /\ bootstrapGeneration < MaxBootstrapGeneration
  /\ exporterAlive' = FALSE
  /\ process' = "Crashed"
  /\ UNCHANGED <<phase, bootstrapGeneration, scanned, durable,
                  durableGeneration, publishedGeneration, streamPosition>>

RestartWholeSnapshot ==
  /\ phase = "Snapshot"
  /\ ~exporterAlive
  /\ bootstrapGeneration < MaxBootstrapGeneration
  /\ bootstrapGeneration' = bootstrapGeneration + 1
  /\ exporterAlive' = TRUE
  /\ process' = "Up"
  /\ scanned' = [p \in Partitions |-> {}]
  /\ durable' = [p \in Partitions |-> {}]
  /\ durableGeneration' = bootstrapGeneration + 1
  /\ UNCHANGED <<phase, publishedGeneration, streamPosition>>

CompleteSnapshot ==
  /\ phase = "Snapshot"
  /\ process = "Up"
  /\ exporterAlive
  /\ durableGeneration = bootstrapGeneration
  /\ UNION {durable[p] : p \in Partitions} = Rows
  /\ phase' = "Transition"
  /\ publishedGeneration' = bootstrapGeneration
  /\ UNCHANGED <<process, exporterAlive, bootstrapGeneration, scanned, durable,
                  durableGeneration, streamPosition>>

StartStreaming ==
  /\ phase = "Transition"
  /\ process = "Up"
  /\ publishedGeneration = bootstrapGeneration
  /\ phase' = "Streaming"
  /\ exporterAlive' = FALSE
  /\ streamPosition' = SnapshotLSN
  /\ UNCHANGED <<process, bootstrapGeneration, scanned, durable,
                  durableGeneration, publishedGeneration>>

ReadStream ==
  /\ phase = "Streaming"
  /\ process = "Up"
  /\ streamPosition < MaxStreamLSN
  /\ streamPosition' = streamPosition + 1
  /\ UNCHANGED <<phase, process, exporterAlive, bootstrapGeneration, scanned,
                  durable, durableGeneration, publishedGeneration>>

Idle ==
  /\ phase = "Streaming"
  /\ streamPosition = MaxStreamLSN
  /\ UNCHANGED vars

Next ==
  \/ ReadSnapshot
  \/ PersistPartition
  \/ CrashTask
  \/ RestartTaskSameExporter
  \/ LoseExporter
  \/ RestartWholeSnapshot
  \/ CompleteSnapshot
  \/ StartStreaming
  \/ ReadStream
  \/ Idle

Spec == Init /\ [][Next]_vars

TypeInvariant ==
  /\ phase \in Phases
  /\ process \in Processes
  /\ exporterAlive \in BOOLEAN
  /\ bootstrapGeneration \in 1..MaxBootstrapGeneration
  /\ scanned \in [Partitions -> SUBSET Rows]
  /\ durable \in [Partitions -> SUBSET Rows]
  /\ durableGeneration \in 1..MaxBootstrapGeneration
  /\ publishedGeneration \in 0..MaxBootstrapGeneration
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
NoCursorCrossesBootstrapGeneration ==
  (\E partition \in Partitions: durable[partition] # {}) => durableGeneration = bootstrapGeneration
PublishedGenerationIsCurrent ==
  publishedGeneration > 0 => publishedGeneration = bootstrapGeneration
ExporterRequiredUntilPublication ==
  phase = "Snapshot" /\ process = "Up" => exporterAlive

====
