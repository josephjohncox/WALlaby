---- MODULE LifecycleGeneration ----
EXTENDS Naturals, TLC

(***************************************************************************
 Generation-fenced lifecycle with durable execution leases. A pause intent
 leaves the public state Running until the active execution has finished.
 A stop intent exposes Stopping, then reaches Stopped only after quiescence.
 New registrations are possible only for the current running generation.
***************************************************************************)

CONSTANT MaxGeneration

ASSUME MaxGeneration \in Nat

PublicStates == {"Created", "Running", "Paused", "Stopping", "Stopped", "Failed"}
Targets == {"None", "Paused", "Stopped"}

VARIABLES state, target, generation, executionGeneration, leaseHeld

vars == <<state, target, generation, executionGeneration, leaseHeld>>

Init ==
  /\ state = "Created"
  /\ target = "None"
  /\ generation = 0
  /\ executionGeneration = 0
  /\ leaseHeld = FALSE

Start ==
  /\ state \in {"Created", "Paused"}
  /\ target = "None"
  /\ executionGeneration = 0
  /\ generation < MaxGeneration
  /\ generation' = generation + 1
  /\ executionGeneration' = generation + 1
  /\ leaseHeld' = TRUE
  /\ state' = "Running"
  /\ UNCHANGED target

PauseIntent ==
  /\ state = "Running"
  /\ target = "None"
  /\ target' = "Paused"
  /\ UNCHANGED <<state, generation, executionGeneration, leaseHeld>>

StopIntent ==
  /\ state \in {"Running", "Paused"}
  /\ target = "None"
  /\ target' = "Stopped"
  /\ state' = "Stopping"
  /\ UNCHANGED <<generation, executionGeneration, leaseHeld>>

ExecutionFinished ==
  /\ executionGeneration > 0
  /\ executionGeneration' = 0
  /\ leaseHeld' = FALSE
  /\ UNCHANGED <<state, target, generation>>

PauseComplete ==
  /\ state = "Running"
  /\ target = "Paused"
  /\ executionGeneration = 0
  /\ state' = "Paused"
  /\ target' = "None"
  /\ UNCHANGED <<generation, executionGeneration, leaseHeld>>

StopComplete ==
  /\ state = "Stopping"
  /\ target = "Stopped"
  /\ executionGeneration = 0
  /\ state' = "Stopped"
  /\ target' = "None"
  /\ UNCHANGED <<generation, executionGeneration, leaseHeld>>

RestartExecution ==
  /\ state = "Running"
  /\ target = "None"
  /\ executionGeneration = 0
  /\ generation > 0
  /\ executionGeneration' = generation
  /\ leaseHeld' = TRUE
  /\ UNCHANGED <<state, target, generation>>

Fail ==
  /\ state \in {"Running", "Paused", "Stopping"}
  /\ state' = "Failed"
  /\ target' = "None"
  /\ UNCHANGED <<generation, executionGeneration, leaseHeld>>

RunOnce ==
  /\ state \in PublicStates
  /\ UNCHANGED vars

Next ==
  \/ Start
  \/ PauseIntent
  \/ StopIntent
  \/ ExecutionFinished
  \/ PauseComplete
  \/ StopComplete
  \/ RestartExecution
  \/ Fail
  \/ RunOnce

Spec == Init /\ [][Next]_vars

TypeInvariant ==
  /\ state \in PublicStates
  /\ target \in Targets
  /\ generation \in 0..MaxGeneration
  /\ executionGeneration \in 0..generation
  /\ leaseHeld \in BOOLEAN

LeaseMatchesExecution == leaseHeld <=> executionGeneration > 0
QuiescentTerminalState == state \in {"Paused", "Stopped"} => executionGeneration = 0
RegistrationUsesCurrentGeneration ==
  executionGeneration > 0 => executionGeneration = generation
PendingPauseIsNotPaused == target = "Paused" => state = "Running"
PendingStopIsStopping == target = "Stopped" => state = "Stopping"
StoppedIsTerminal == [](state = "Stopped" => [](state = "Stopped"))

====
