---- MODULE CDCFlow ----
EXTENDS Naturals, Sequences, TLC, FiniteSets

(***************************************************************************
 Runtime-faithful CDC durability model.

 Destination delivery, durable checkpoint persistence, source acknowledgement,
 checkpoint failure, process crash/restart, and idempotent restore acknowledgement
 are distinct actions. Persistence precedes source acknowledgement so a failed
 checkpoint write cannot advance the source beyond the recoverable position.
***************************************************************************)

CONSTANTS MaxLSN, MaxRetries, GateDDL, DDLSet, FailureMode, GiveUpMode

FailureModes == {"HoldSlot", "DropSlot"}
GiveUpModes == {"Never", "OnRetryExhaustion"}
FlowStates == {"Created", "Running", "Paused", "Stopping", "Stopped", "Failed"}
ProcessStates == {"Up", "Crashed"}

ASSUME FailureMode \in FailureModes
ASSUME GiveUpMode \in GiveUpModes

VARIABLES
  flow,
  process,
  nextRead,
  inflight,
  delivered,
  lastAcked,       \* last source-acknowledged LSN
  lastCheckpoint,  \* last durably persisted LSN
  checkpointFailed,
  ddlPending,
  ddlApproved,
  ddlApplied,
  readAttempts,
  writeAttempts

vars == <<flow, process, nextRead, inflight, delivered, lastAcked,
          lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
          ddlApplied, readAttempts, writeAttempts>>

\* FailureMode is slot-retention policy and does not alter lifecycle state.
ZeroAttempts == [lsn \in 1..MaxLSN |-> 0]

Init ==
  /\ flow = "Created"
  /\ process = "Up"
  /\ nextRead = 1
  /\ inflight = {}
  /\ delivered = {}
  /\ lastAcked = 0
  /\ lastCheckpoint = 0
  /\ checkpointFailed = FALSE
  /\ ddlPending = {}
  /\ ddlApproved = {}
  /\ ddlApplied = {}
  /\ readAttempts = ZeroAttempts
  /\ writeAttempts = ZeroAttempts

Start ==
  /\ flow = "Created"
  /\ flow' = "Running"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

Pause ==
  /\ flow = "Running"
  /\ flow' = "Paused"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

Resume ==
  /\ flow = "Paused"
  /\ (\/ ~GateDDL \/ ddlPending = {})
  /\ flow' = "Running"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

StopBegin ==
  /\ flow \in {"Running", "Paused"}
  /\ flow' = "Stopping"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

StopComplete ==
  /\ flow = "Stopping"
  /\ flow' = "Stopped"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

Fail ==
  /\ flow \in {"Running", "Paused", "Stopping"}
  /\ flow' = "Failed"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

ReadBatch ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ nextRead <= MaxLSN
  /\ nextRead \notin DDLSet
  /\ readAttempts[nextRead] < MaxRetries \/ GiveUpMode = "Never"
  /\ inflight' = inflight \cup {nextRead}
  /\ nextRead' = nextRead + 1
  /\ readAttempts' =
      [readAttempts EXCEPT ![nextRead] = IF readAttempts[nextRead] < MaxRetries THEN @ + 1 ELSE @]
  /\ UNCHANGED <<flow, process, delivered, lastAcked, lastCheckpoint,
                 checkpointFailed, ddlPending, ddlApproved, ddlApplied,
                 writeAttempts>>

ReadDDL ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ nextRead <= MaxLSN
  /\ nextRead \in DDLSet
  /\ readAttempts[nextRead] < MaxRetries \/ GiveUpMode = "Never"
  /\ inflight' = inflight \cup {nextRead}
  /\ ddlPending' = ddlPending \cup {nextRead}
  /\ nextRead' = nextRead + 1
  /\ readAttempts' =
      [readAttempts EXCEPT ![nextRead] = IF readAttempts[nextRead] < MaxRetries THEN @ + 1 ELSE @]
  /\ flow' = IF GateDDL THEN "Paused" ELSE flow
  /\ UNCHANGED <<process, delivered, lastAcked, lastCheckpoint,
                 checkpointFailed, ddlApproved, ddlApplied, writeAttempts>>

ReadFail ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ nextRead <= MaxLSN
  /\ readAttempts[nextRead] <= MaxRetries
  /\ readAttempts' =
      [readAttempts EXCEPT ![nextRead] = IF readAttempts[nextRead] < MaxRetries THEN @ + 1 ELSE @]
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, writeAttempts>>

ReadGiveUp ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ nextRead <= MaxLSN
  /\ GiveUpMode = "OnRetryExhaustion"
  /\ readAttempts[nextRead] >= MaxRetries
  /\ flow' = "Failed"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

Deliver ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ delivered' = delivered \cup {lsn}
      /\ inflight' = inflight \ {lsn}
      /\ writeAttempts[lsn] < MaxRetries \/ GiveUpMode = "Never"
      /\ writeAttempts' =
          [writeAttempts EXCEPT ![lsn] = IF writeAttempts[lsn] < MaxRetries THEN @ + 1 ELSE @]
      /\ UNCHANGED <<flow, process, nextRead, lastAcked, lastCheckpoint,
                     checkpointFailed, ddlPending, ddlApproved, ddlApplied,
                     readAttempts>>

WriteFail ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ writeAttempts[lsn] <= MaxRetries
      /\ writeAttempts' =
          [writeAttempts EXCEPT ![lsn] = IF writeAttempts[lsn] < MaxRetries THEN @ + 1 ELSE @]
      /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, lastAcked,
                     lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                     ddlApplied, readAttempts>>

WriteGiveUp ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ inflight # {}
  /\ \E lsn \in inflight:
      /\ GiveUpMode = "OnRetryExhaustion"
      /\ writeAttempts[lsn] >= MaxRetries
      /\ flow' = "Failed"
      /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                     lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                     ddlApplied, readAttempts, writeAttempts>>

PersistCheckpoint ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ lastCheckpoint = lastAcked
  /\ lastCheckpoint < MaxLSN
  /\ (lastCheckpoint + 1) \in delivered
  /\ lastCheckpoint' = lastCheckpoint + 1
  /\ checkpointFailed' = FALSE
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, lastAcked,
                 ddlPending, ddlApproved, ddlApplied, readAttempts, writeAttempts>>

CheckpointFail ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ lastCheckpoint = lastAcked
  /\ lastCheckpoint < MaxLSN
  /\ (lastCheckpoint + 1) \in delivered
  /\ checkpointFailed' = TRUE
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, ddlPending, ddlApproved, ddlApplied,
                 readAttempts, writeAttempts>>

Ack ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ lastAcked < lastCheckpoint
  /\ lastAcked' = lastAcked + 1
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

\* A restart may resend the durable position. Repeating this action is a stutter
\* on the source acknowledgement and therefore explicitly idempotent.
RestoreAck ==
  /\ flow = "Running"
  /\ process = "Up"
  /\ lastCheckpoint > 0
  /\ lastAcked <= lastCheckpoint
  /\ lastAcked' = lastCheckpoint
  /\ UNCHANGED <<flow, process, nextRead, inflight, delivered,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

Crash ==
  /\ process = "Up"
  /\ flow = "Running"
  /\ process' = "Crashed"
  /\ UNCHANGED <<flow, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

Restart ==
  /\ process = "Crashed"
  /\ process' = "Up"
  /\ nextRead' = lastCheckpoint + 1
  /\ inflight' = {}
  /\ readAttempts' = ZeroAttempts
  /\ writeAttempts' = ZeroAttempts
  /\ UNCHANGED <<flow, delivered, lastAcked, lastCheckpoint,
                 checkpointFailed, ddlPending, ddlApproved, ddlApplied>>

ApproveDDL ==
  /\ ddlPending # {}
  /\ \E lsn \in ddlPending:
      /\ ddlPending' = ddlPending \ {lsn}
      /\ ddlApproved' = ddlApproved \cup {lsn}
      /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, lastAcked,
                     lastCheckpoint, checkpointFailed, ddlApplied,
                     readAttempts, writeAttempts>>

ApplyDDL ==
  /\ ddlApproved # {}
  /\ \E lsn \in ddlApproved:
      /\ ddlApplied' = ddlApplied \cup {lsn}
      /\ UNCHANGED <<flow, process, nextRead, inflight, delivered, lastAcked,
                     lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                     readAttempts, writeAttempts>>

ResumeAfterDDL ==
  /\ flow = "Paused"
  /\ GateDDL
  /\ ddlPending = {}
  /\ flow' = "Running"
  /\ UNCHANGED <<process, nextRead, inflight, delivered, lastAcked,
                 lastCheckpoint, checkpointFailed, ddlPending, ddlApproved,
                 ddlApplied, readAttempts, writeAttempts>>

Idle ==
  /\ flow \in {"Stopped", "Failed"}
  /\ UNCHANGED vars

Next ==
  \/ Start
  \/ Pause
  \/ Resume
  \/ StopBegin
  \/ StopComplete
  \/ Fail
  \/ ReadBatch
  \/ ReadDDL
  \/ ReadFail
  \/ ReadGiveUp
  \/ Deliver
  \/ WriteFail
  \/ WriteGiveUp
  \/ CheckpointFail
  \/ PersistCheckpoint
  \/ Ack
  \/ RestoreAck
  \/ Crash
  \/ Restart
  \/ ApproveDDL
  \/ ApplyDDL
  \/ ResumeAfterDDL
  \/ Idle

Spec == Init /\ [][Next]_vars

SpecFair ==
  Spec
  /\ WF_vars(ReadBatch)
  /\ WF_vars(ReadDDL)
  /\ WF_vars(Deliver)
  /\ WF_vars(PersistCheckpoint)
  /\ WF_vars(Ack)
  /\ WF_vars(ReadGiveUp)
  /\ WF_vars(WriteGiveUp)

\* The witness excludes operator-driven shutdown/failure actions. Using the
\* general Next under a state constraint still admitted Stopping, where the
\* constrained successor was disabled and the liveness witness could fail.
WitnessNext ==
  \/ Start
  \/ ReadBatch
  \/ ReadDDL
  \/ Deliver
  \/ PersistCheckpoint
  \/ Ack
  \/ RestoreAck
  \/ ApproveDDL
  \/ ApplyDDL
  \/ ResumeAfterDDL

SpecWitness ==
  Init /\ [][WitnessNext]_vars
  /\ WF_vars(Start)
  /\ WF_vars(ReadBatch)
  /\ WF_vars(ReadDDL)
  /\ WF_vars(Deliver)
  /\ WF_vars(PersistCheckpoint)
  /\ WF_vars(Ack)
  /\ WF_vars(ApproveDDL)
  /\ WF_vars(ApplyDDL)
  /\ WF_vars(ResumeAfterDDL)

RunningForever == <>[] (flow = "Running" /\ process = "Up")
Termination == RunningForever => <> (lastAcked = MaxLSN)

NoTerminalStates ==
  /\ flow # "Stopped"
  /\ flow # "Failed"

NoCrashes == process = "Up"

EventuallyDDLApproved == <> (ddlApproved # {})
EventuallyDDLApplied == <> (ddlApplied # {})

(***************************************************************************
 Safety invariants and temporal monotonicity properties
***************************************************************************)
TypeInvariant ==
  /\ flow \in FlowStates
  /\ process \in ProcessStates
  /\ nextRead \in 1..(MaxLSN + 1)
  /\ inflight \subseteq 1..MaxLSN
  /\ delivered \subseteq 1..MaxLSN
  /\ lastAcked \in 0..MaxLSN
  /\ lastCheckpoint \in 0..MaxLSN
  /\ checkpointFailed \in BOOLEAN
  /\ ddlPending \subseteq 1..MaxLSN
  /\ ddlApproved \subseteq 1..MaxLSN
  /\ ddlApplied \subseteq 1..MaxLSN
  /\ readAttempts \in [1..MaxLSN -> Nat]
  /\ writeAttempts \in [1..MaxLSN -> Nat]
  /\ DDLSet \subseteq 1..MaxLSN
  /\ FailureMode \in FailureModes
  /\ GiveUpMode \in GiveUpModes

NoAckWithoutDeliver == lastAcked \in delivered \cup {0}
AckMonotonic == (1..lastAcked) \subseteq delivered
CheckpointMonotonic ==
  /\ lastAcked <= lastCheckpoint
  /\ lastCheckpoint <= lastAcked + 1
  /\ (lastCheckpoint = 0 \/ lastCheckpoint \in delivered)

AckNeverRegresses == [][lastAcked' >= lastAcked]_vars
CheckpointNeverRegresses == [][lastCheckpoint' >= lastCheckpoint]_vars
DurableBeforeAck == [](lastAcked <= lastCheckpoint)

ReadAheadBounded == Cardinality(inflight) <= MaxLSN
RetryBounds ==
  /\ \A lsn \in 1..MaxLSN: readAttempts[lsn] <= MaxRetries
  /\ \A lsn \in 1..MaxLSN: writeAttempts[lsn] <= MaxRetries
DDLAppliedAfterApproval == ddlApplied \subseteq ddlApproved
DDLGatedPausesFlow == (GateDDL /\ ddlPending # {}) => flow # "Running"
PausedImpliesDDL == (flow # "Paused") \/ (ddlPending # {}) \/ (ddlApproved # {})
FlowTransitionsValid == flow \in FlowStates

====
