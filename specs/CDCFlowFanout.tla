---- MODULE CDCFlowFanout ----
EXTENDS Naturals, TLC

(***************************************************************************
 Fan-out CDC model with per-destination delivery and acknowledgement.
 Durable checkpoint persistence and source acknowledgement are distinct:
 source progress may advance only through already-persisted progress.
***************************************************************************)

CONSTANTS MaxLSN, Destinations, AckPolicy, PrimaryDestinations, FailureMode

AckPolicies == {"All", "Primary"}
FailureModes == {"HoldSlot", "DropSlot"}
ProcessStates == {"Up", "Crashed"}

ASSUME MaxLSN \in Nat
ASSUME AckPolicy \in AckPolicies
ASSUME FailureMode \in FailureModes
ASSUME PrimaryDestinations \subseteq Destinations
ASSUME PrimaryDestinations # {}

FlowStates == {"Created", "Running", "Paused", "Stopped", "Failed"}

VARIABLES
  flow,
  process,
  nextRead,
  inflight,
  delivered,    \* [dest -> subset of LSNs delivered]
  acked,        \* [dest -> subset of LSNs acknowledged]
  lastAcked,    \* source-acknowledged progress
  lastCheckpoint,
  checkpointFailed

vars == <<flow, process, nextRead, inflight, delivered, acked, lastAcked,
          lastCheckpoint, checkpointFailed>>

ReadSet == 1..(nextRead - 1)
PrimaryAckSet == IF AckPolicy = "Primary" THEN PrimaryDestinations ELSE Destinations

Init ==
  /\ flow = "Created"
  /\ process = "Up"
  /\ nextRead = 1
  /\ inflight = {}
  /\ delivered = [d \in Destinations |-> {}]
  /\ acked = [d \in Destinations |-> {}]
  /\ lastAcked = 0
  /\ lastCheckpoint = 0
  /\ checkpointFailed = FALSE

Start ==
  /\ flow = "Created"
  /\ flow' = "Running"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, acked, lastAcked,
                 lastCheckpoint, checkpointFailed>>

Pause ==
  /\ flow = "Running"
  /\ flow' = "Paused"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, acked, lastAcked,
                 lastCheckpoint, checkpointFailed>>

Resume ==
  /\ flow = "Paused"
  /\ flow' = "Running"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, acked, lastAcked,
                 lastCheckpoint, checkpointFailed>>

Stop ==
  /\ flow \in {"Running", "Paused"}
  /\ flow' = "Stopped"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, acked, lastAcked,
                 lastCheckpoint, checkpointFailed>>

Fail ==
  /\ flow \in {"Running", "Paused"}
  /\ flow' = "Failed"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, acked, lastAcked,
                 lastCheckpoint, checkpointFailed>>

ReadBatch ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ nextRead <= MaxLSN
  /\ inflight' = inflight \cup {nextRead}
  /\ nextRead' = nextRead + 1
  /\ UNCHANGED <<flow, process, delivered, acked, lastAcked, lastCheckpoint,
                 checkpointFailed>>

Deliver ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ \E d \in Destinations:
            /\ lsn \notin delivered[d]
            /\ delivered' = [delivered EXCEPT ![d] = @ \cup {lsn}]
            /\ inflight' =
                IF \A d2 \in Destinations: lsn \in delivered'[d2]
                THEN inflight \ {lsn}
                ELSE inflight
            /\ UNCHANGED <<flow, process, nextRead, acked, lastAcked,
                           lastCheckpoint, checkpointFailed>>

AckDest ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ \E d \in Destinations:
      /\ \E lsn \in delivered[d]:
            /\ lsn \notin acked[d]
            /\ acked' = [acked EXCEPT ![d] = @ \cup {lsn}]
            /\ UNCHANGED <<flow, process, nextRead, inflight, delivered,
                           lastAcked, lastCheckpoint, checkpointFailed>>

CanPersist(lsn) ==
  /\ lsn \in ReadSet
  /\ \A d \in PrimaryAckSet: lsn \in acked[d]

PersistCheckpoint ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ lastCheckpoint < MaxLSN
  /\ CanPersist(lastCheckpoint + 1)
  /\ lastCheckpoint' = lastCheckpoint + 1
  /\ checkpointFailed' = FALSE
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, acked, lastAcked>>

CheckpointFail ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ lastCheckpoint < MaxLSN
  /\ CanPersist(lastCheckpoint + 1)
  /\ checkpointFailed' = TRUE
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, acked, lastAcked,
                 lastCheckpoint>>

AckSource ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ lastAcked < lastCheckpoint
  /\ lastAcked' = lastAcked + 1
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, acked,
                 lastCheckpoint, checkpointFailed>>

Crash ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ process' = "Crashed"
  /\ UNCHANGED <<flow, nextRead, inflight, delivered, acked, lastAcked,
                 lastCheckpoint, checkpointFailed>>

Restart ==
  /\ flow = "Running"
  /\ process = "Crashed"
  /\ process' = "Up"
  /\ nextRead' = lastCheckpoint + 1
  /\ inflight' = {}
  /\ checkpointFailed' = FALSE
  /\ UNCHANGED <<flow, delivered, acked, lastAcked, lastCheckpoint>>

Idle ==
  /\ flow \in {"Stopped", "Failed"}
  /\ UNCHANGED vars

Next ==
  \/ Start
  \/ Pause
  \/ Resume
  \/ Stop
  \/ Fail
  \/ ReadBatch
  \/ Deliver
  \/ AckDest
  \/ PersistCheckpoint
  \/ CheckpointFail
  \/ AckSource
  \/ Crash
  \/ Restart
  \/ Idle

Spec == Init /\ [][Next]_vars

(***************************************************************************
 Invariants
***************************************************************************)
TypeInvariant ==
  /\ flow \in FlowStates
  /\ process \in ProcessStates
  /\ nextRead \in Nat
  /\ nextRead <= MaxLSN + 1
  /\ inflight \subseteq Nat
  /\ \A lsn \in inflight: lsn >= 1 /\ lsn <= MaxLSN
  /\ lastAcked \in Nat
  /\ lastAcked <= MaxLSN
  /\ lastCheckpoint \in Nat
  /\ lastCheckpoint <= MaxLSN
  /\ checkpointFailed \in BOOLEAN
  /\ Destinations # {}
  /\ AckPolicy \in AckPolicies
  /\ FailureMode \in FailureModes
  /\ PrimaryDestinations \subseteq Destinations
  /\ PrimaryDestinations # {}
  /\ delivered \in [Destinations -> SUBSET Nat]
  /\ \A d \in Destinations:
      \A lsn \in delivered[d]: lsn >= 1 /\ lsn <= MaxLSN
  /\ acked \in [Destinations -> SUBSET Nat]
  /\ \A d \in Destinations:
      \A lsn \in acked[d]: lsn >= 1 /\ lsn <= MaxLSN

AckedImpliesDelivered ==
  /\ \A d \in Destinations:
      acked[d] \subseteq delivered[d]

SourceAckRequiresPolicy ==
  /\ \A lsn \in 1..lastAcked:
      \A d \in PrimaryAckSet: lsn \in acked[d]

CheckpointMonotonic == lastAcked <= lastCheckpoint

====
