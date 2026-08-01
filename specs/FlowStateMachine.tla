---- MODULE FlowStateMachine ----
EXTENDS TLC

(***************************************************************************
 Canonical flow lifecycle. Pause is resumable. Stop is two phase: Stopping is
 durable while executions are cancelled; Stopped is terminal. Slot retention
 policy is deliberately not encoded in lifecycle state.
***************************************************************************)

FlowStates == {"Created", "Running", "Paused", "Stopping", "Stopped", "Failed"}

VARIABLES state

vars == <<state>>

Init == state = "Created"

Start ==
  /\ state = "Created"
  /\ state' = "Running"

Pause ==
  /\ state = "Running"
  /\ state' = "Paused"

Resume ==
  /\ state = "Paused"
  /\ state' = "Running"

StopBegin ==
  /\ state \in {"Running", "Paused"}
  /\ state' = "Stopping"

StopComplete ==
  /\ state = "Stopping"
  /\ state' = "Stopped"

Fail ==
  /\ state \in {"Running", "Paused", "Stopping"}
  /\ state' = "Failed"

RunOnce ==
  /\ state \in FlowStates
  /\ state' = state

Next ==
  \/ Start
  \/ Pause
  \/ Resume
  \/ StopBegin
  \/ StopComplete
  \/ Fail
  \/ RunOnce

Spec == Init /\ [][Next]_vars

TypeInvariant == state \in FlowStates
StoppedIsTerminal == [](state = "Stopped" => [](state = "Stopped"))

====
