---- MODULE CDCFlowFanout ----
EXTENDS Naturals, TLC

(***************************************************************************
 Fan-out CDC model with per-destination delivery + ack.
 Source ack advances only when all destinations ack a given LSN.
***************************************************************************)

CONSTANTS MaxLSN, Destinations, AckPolicy, PrimaryDestinations, FailureMode

AckPolicies == {"All", "Primary"}
FailureModes == {"HoldSlot", "DropSlot"}

ASSUME MaxLSN \in Nat
ASSUME AckPolicy \in AckPolicies
ASSUME FailureMode \in FailureModes
ASSUME PrimaryDestinations \subseteq Destinations
ASSUME PrimaryDestinations # {}

FlowStates == {"Created", "Running", "Paused", "Stopped", "FailedHoldingSlot", "FailedDroppedSlot"}

VARIABLES
  flow,
  nextRead,
  inflight,
  delivered,    \* [dest -> subset of LSNs delivered]
  acked,        \* [dest -> subset of LSNs acked]
  lastAcked,
  lastCheckpoint

vars == <<flow, nextRead, inflight, delivered, acked, lastAcked, lastCheckpoint>>

ReadSet == 1..(nextRead - 1)
PrimaryAckSet == IF AckPolicy = "Primary" THEN PrimaryDestinations ELSE Destinations

Init ==
  /\ flow = "Created"
  /\ nextRead = 1
  /\ inflight = {}
  /\ delivered = [d \in Destinations |-> {}]
  /\ acked = [d \in Destinations |-> {}]
  /\ lastAcked = 0
  /\ lastCheckpoint = 0

Start ==
  /\ flow = "Created"
  /\ flow' = "Running"
  /\ UNCHANGED <<nextRead, inflight, delivered, acked, lastAcked, lastCheckpoint>>

Pause ==
  /\ flow = "Running"
  /\ flow' = "Paused"
  /\ UNCHANGED <<nextRead, inflight, delivered, acked, lastAcked, lastCheckpoint>>

Resume ==
  /\ flow = "Paused"
  /\ flow' = "Running"
  /\ UNCHANGED <<nextRead, inflight, delivered, acked, lastAcked, lastCheckpoint>>

Stop ==
  /\ flow \in {"Running", "Paused"}
  /\ flow' = "Stopped"
  /\ UNCHANGED <<nextRead, inflight, delivered, acked, lastAcked, lastCheckpoint>>

Fail ==
  /\ flow \in {"Running", "Paused"}
  /\ flow' = IF FailureMode = "HoldSlot" THEN "FailedHoldingSlot" ELSE "FailedDroppedSlot"
  /\ UNCHANGED <<nextRead, inflight, delivered, acked, lastAcked, lastCheckpoint>>

ReadBatch ==
  /\ flow = "Running"
  /\ nextRead <= MaxLSN
  /\ inflight' = inflight \cup {nextRead}
  /\ nextRead' = nextRead + 1
  /\ UNCHANGED <<flow, delivered, acked, lastAcked, lastCheckpoint>>

Deliver ==
  /\ flow = "Running"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ \E d \in Destinations:
            /\ lsn \notin delivered[d]
            /\ delivered' = [delivered EXCEPT ![d] = @ \cup {lsn}]
            /\ inflight' =
                IF \A d2 \in Destinations: lsn \in delivered'[d2]
                THEN inflight \ {lsn}
                ELSE inflight
            /\ UNCHANGED <<flow, nextRead, acked, lastAcked, lastCheckpoint>>

AckDest ==
  /\ flow = "Running"
  /\ \E d \in Destinations:
      /\ \E lsn \in delivered[d]:
            /\ lsn \notin acked[d]
            /\ acked' = [acked EXCEPT ![d] = @ \cup {lsn}]
            /\ UNCHANGED <<flow, nextRead, inflight, delivered, lastAcked, lastCheckpoint>>

CanAckSource(lsn) ==
  /\ lsn \in ReadSet
  /\ \A d \in PrimaryAckSet: lsn \in acked[d]

AckSource ==
  /\ flow = "Running"
  /\ lastAcked < MaxLSN
  /\ CanAckSource(lastAcked + 1)
  /\ lastAcked' = lastAcked + 1
  /\ lastCheckpoint' = lastAcked'
  /\ UNCHANGED <<flow, nextRead, inflight, delivered, acked>>

Idle ==
  /\ flow \in {"Stopped", "FailedHoldingSlot", "FailedDroppedSlot"}
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
  \/ AckSource
  \/ Idle

Spec == Init /\ [][Next]_vars

(***************************************************************************
 Invariants
***************************************************************************)
TypeInvariant ==
  /\ flow \in FlowStates
  /\ nextRead \in Nat
  /\ nextRead <= MaxLSN + 1
  /\ inflight \subseteq Nat
  /\ \A lsn \in inflight: lsn >= 1 /\ lsn <= MaxLSN
  /\ lastAcked \in Nat
  /\ lastAcked <= MaxLSN
  /\ lastCheckpoint \in Nat
  /\ lastCheckpoint <= MaxLSN
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

CheckpointMonotonic == lastCheckpoint <= lastAcked

====
