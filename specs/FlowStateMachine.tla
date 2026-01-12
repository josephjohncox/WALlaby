---- MODULE FlowStateMachine ----
EXTENDS TLC

(***************************************************************************
 Flow lifecycle state machine. Models CLI-driven transitions and ensures
 only valid state changes occur. RunOnce must not alter the flow state.
***************************************************************************)

FlowStates == {"Created", "Running", "Paused", "Stopped", "FailedHoldingSlot", "FailedDroppedSlot"}

VARIABLES state

vars == <<state>>

Init ==
  /\ state = "Created"

Start ==
  /\ state \in {"Created", "Paused"}
  /\ state' = "Running"

Stop ==
  /\ state = "Running"
  /\ state' = "Paused"

Resume ==
  /\ state = "Paused"
  /\ state' = "Running"

Fail ==
  /\ state \in {"Running", "Paused"}
  /\ state' \in {"FailedHoldingSlot", "FailedDroppedSlot"}

RunOnce ==
  /\ state \in FlowStates
  /\ state' = state

Next ==
  \/ Start
  \/ Stop
  \/ Resume
  \/ Fail
  \/ RunOnce

Spec == Init /\ [][Next]_vars

TypeInvariant == state \in FlowStates

====
