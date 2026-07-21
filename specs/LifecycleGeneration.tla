---- MODULE LifecycleGeneration ----
EXTENDS Naturals, TLC

(***************************************************************************
 Generation- and incarnation-fenced lifecycle with producer lease epochs.
 Public states remain unchanged. A takeover increments the private lease epoch;
 only the exact current incarnation/generation/epoch may mutate authoritative
 data or finish/fail execution. Reusing a public flow ID creates a new
 incarnation and clears every execution capability.
***************************************************************************)

CONSTANTS MaxGeneration, MaxIncarnation, MaxLeaseEpoch

ASSUME MaxGeneration \in Nat
ASSUME MaxIncarnation \in Nat \ {0}
ASSUME MaxLeaseEpoch \in Nat \ {0}

PublicStates == {"Created", "Running", "Paused", "Stopping", "Stopped", "Failed"}
Targets == {"None", "Paused", "Stopped"}

VARIABLES state, target, incarnation, generation,
          executionIncarnation, executionGeneration,
          leaseEpoch, executionLeaseEpoch, leaseLive,
          lastMutationIncarnation, lastMutationGeneration, lastMutationEpoch

vars == <<state, target, incarnation, generation,
          executionIncarnation, executionGeneration,
          leaseEpoch, executionLeaseEpoch, leaseLive,
          lastMutationIncarnation, lastMutationGeneration, lastMutationEpoch>>

Init ==
  /\ state = "Created"
  /\ target = "None"
  /\ incarnation = 1
  /\ generation = 0
  /\ executionIncarnation = 0
  /\ executionGeneration = 0
  /\ leaseEpoch = 0
  /\ executionLeaseEpoch = 0
  /\ leaseLive = FALSE
  /\ lastMutationIncarnation = 0
  /\ lastMutationGeneration = 0
  /\ lastMutationEpoch = 0

Start ==
  /\ state \in {"Created", "Paused"}
  /\ target = "None"
  /\ executionGeneration = 0
  /\ generation < MaxGeneration
  /\ leaseEpoch < MaxLeaseEpoch
  /\ generation' = generation + 1
  /\ executionGeneration' = generation + 1
  /\ executionIncarnation' = incarnation
  /\ leaseEpoch' = leaseEpoch + 1
  /\ executionLeaseEpoch' = leaseEpoch + 1
  /\ leaseLive' = TRUE
  /\ state' = "Running"
  /\ UNCHANGED <<target, incarnation, lastMutationIncarnation,
                  lastMutationGeneration, lastMutationEpoch>>

ExpireLease ==
  /\ executionGeneration > 0
  /\ leaseLive
  /\ leaseLive' = FALSE
  /\ UNCHANGED <<state, target, incarnation, generation,
                  executionIncarnation, executionGeneration, leaseEpoch,
                  executionLeaseEpoch, lastMutationIncarnation,
                  lastMutationGeneration, lastMutationEpoch>>

Takeover ==
  /\ state = "Running"
  /\ target = "None"
  /\ executionGeneration = generation
  /\ ~leaseLive
  /\ leaseEpoch < MaxLeaseEpoch
  /\ leaseEpoch' = leaseEpoch + 1
  /\ executionIncarnation' = incarnation
  /\ executionGeneration' = generation
  /\ executionLeaseEpoch' = leaseEpoch + 1
  /\ leaseLive' = TRUE
  /\ UNCHANGED <<state, target, incarnation, generation,
                  lastMutationIncarnation, lastMutationGeneration,
                  lastMutationEpoch>>

AuthoritativeMutation ==
  /\ state = "Running"
  /\ target = "None"
  /\ leaseLive
  /\ executionIncarnation = incarnation
  /\ executionGeneration = generation
  /\ executionLeaseEpoch = leaseEpoch
  /\ lastMutationIncarnation' = incarnation
  /\ lastMutationGeneration' = generation
  /\ lastMutationEpoch' = leaseEpoch
  /\ UNCHANGED <<state, target, incarnation, generation,
                  executionIncarnation, executionGeneration,
                  leaseEpoch, executionLeaseEpoch, leaseLive>>

PauseIntent ==
  /\ state = "Running"
  /\ target = "None"
  /\ target' = "Paused"
  /\ UNCHANGED <<state, incarnation, generation, executionIncarnation,
                  executionGeneration, leaseEpoch, executionLeaseEpoch,
                  leaseLive, lastMutationIncarnation,
                  lastMutationGeneration, lastMutationEpoch>>

StopIntent ==
  /\ state \in {"Running", "Paused"}
  /\ target = "None"
  /\ target' = "Stopped"
  /\ state' = "Stopping"
  /\ UNCHANGED <<incarnation, generation, executionIncarnation,
                  executionGeneration, leaseEpoch, executionLeaseEpoch,
                  leaseLive, lastMutationIncarnation,
                  lastMutationGeneration, lastMutationEpoch>>

ExecutionFinished ==
  /\ executionGeneration > 0
  /\ leaseLive
  /\ executionIncarnation = incarnation
  /\ executionGeneration = generation
  /\ executionLeaseEpoch = leaseEpoch
  /\ executionIncarnation' = 0
  /\ executionGeneration' = 0
  /\ executionLeaseEpoch' = 0
  /\ leaseLive' = FALSE
  /\ UNCHANGED <<state, target, incarnation, generation, leaseEpoch,
                  lastMutationIncarnation, lastMutationGeneration,
                  lastMutationEpoch>>

PauseComplete ==
  /\ state = "Running"
  /\ target = "Paused"
  /\ executionGeneration = 0
  /\ state' = "Paused"
  /\ target' = "None"
  /\ UNCHANGED <<incarnation, generation, executionIncarnation,
                  executionGeneration, leaseEpoch, executionLeaseEpoch,
                  leaseLive, lastMutationIncarnation,
                  lastMutationGeneration, lastMutationEpoch>>

StopComplete ==
  /\ state = "Stopping"
  /\ target = "Stopped"
  /\ executionGeneration = 0
  /\ state' = "Stopped"
  /\ target' = "None"
  /\ UNCHANGED <<incarnation, generation, executionIncarnation,
                  executionGeneration, leaseEpoch, executionLeaseEpoch,
                  leaseLive, lastMutationIncarnation,
                  lastMutationGeneration, lastMutationEpoch>>

Fail ==
  /\ state \in {"Running", "Paused", "Stopping"}
  /\ \/ executionGeneration = 0
     \/ /\ leaseLive
        /\ executionIncarnation = incarnation
        /\ executionGeneration = generation
        /\ executionLeaseEpoch = leaseEpoch
  /\ state' = "Failed"
  /\ target' = "None"
  /\ UNCHANGED <<incarnation, generation, executionIncarnation,
                  executionGeneration, leaseEpoch, executionLeaseEpoch,
                  leaseLive, lastMutationIncarnation,
                  lastMutationGeneration, lastMutationEpoch>>

RecreatePublicID ==
  /\ state \in {"Stopped", "Failed"}
  /\ executionGeneration = 0
  /\ incarnation < MaxIncarnation
  /\ incarnation' = incarnation + 1
  /\ state' = "Created"
  /\ target' = "None"
  /\ generation' = 0
  /\ executionIncarnation' = 0
  /\ executionGeneration' = 0
  /\ leaseEpoch' = 0
  /\ executionLeaseEpoch' = 0
  /\ leaseLive' = FALSE
  /\ lastMutationIncarnation' = 0
  /\ lastMutationGeneration' = 0
  /\ lastMutationEpoch' = 0

RunOnce ==
  /\ state \in PublicStates
  /\ UNCHANGED vars

Next ==
  \/ Start
  \/ ExpireLease
  \/ Takeover
  \/ AuthoritativeMutation
  \/ PauseIntent
  \/ StopIntent
  \/ ExecutionFinished
  \/ PauseComplete
  \/ StopComplete
  \/ Fail
  \/ RecreatePublicID
  \/ RunOnce

Spec == Init /\ [][Next]_vars

TypeInvariant ==
  /\ state \in PublicStates
  /\ target \in Targets
  /\ incarnation \in 1..MaxIncarnation
  /\ generation \in 0..MaxGeneration
  /\ executionIncarnation \in 0..MaxIncarnation
  /\ executionGeneration \in 0..MaxGeneration
  /\ leaseEpoch \in 0..MaxLeaseEpoch
  /\ executionLeaseEpoch \in 0..MaxLeaseEpoch
  /\ leaseLive \in BOOLEAN
  /\ lastMutationIncarnation \in 0..MaxIncarnation
  /\ lastMutationGeneration \in 0..MaxGeneration
  /\ lastMutationEpoch \in 0..MaxLeaseEpoch

LeaseMatchesExecution == leaseLive => executionGeneration > 0
QuiescentTerminalState == state \in {"Paused", "Stopped"} => executionGeneration = 0
RegistrationUsesCurrentGeneration ==
  executionGeneration > 0 =>
    /\ executionIncarnation = incarnation
    /\ executionGeneration = generation
    /\ executionLeaseEpoch = leaseEpoch
PendingPauseIsNotPaused == target = "Paused" => state = "Running"
PendingStopIsStopping == target = "Stopped" => state = "Stopping"
AuthoritativeMutationUsesCurrentFence ==
  lastMutationEpoch > 0 =>
    /\ lastMutationIncarnation = incarnation
    /\ lastMutationGeneration <= generation
    /\ lastMutationEpoch <= leaseEpoch
FlowReuseClearsOldAuthority ==
  state = "Created" =>
    /\ executionGeneration = 0
    /\ lastMutationEpoch = 0

====
