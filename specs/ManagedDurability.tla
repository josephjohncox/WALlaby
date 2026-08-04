---- MODULE ManagedDurability ----
EXTENDS Naturals, TLC

(***************************************************************************
 Managed CDC authority across PostgreSQL and external side effects.

 External target commits and S3 uploads may survive a producer takeover.
 PostgreSQL-authoritative receipts, artifact roots, checkpoints, source ACK
 intents, delivery completion, and retention marks are written only by the
 current lease epoch. Final publication writes each root, delivery row,
 checkpoint, and ACK intent atomically. Mark/sweep may remove an orphan, or a
 released root whose ACK is observed, whose deliveries are complete, and whose
 position is older than the current checkpoint.
***************************************************************************)

CONSTANTS Workers, MaxPosition, MaxEpoch

ASSUME /\ Workers # {}
       /\ "None" \notin Workers
       /\ MaxPosition \in Nat \ {0}
       /\ MaxEpoch \in Nat \ {0}

Positions == 1..MaxPosition

VARIABLES
  currentOwner,
  currentEpoch,
  workerEpoch,
  attempts,
  externallyApplied,
  receiptEpoch,
  checkpoint,
  ackIntentEpoch,
  acked,
  uploaded,
  rootEpoch,
  deliveryDone,
  released

vars == <<currentOwner, currentEpoch, workerEpoch, attempts,
          externallyApplied, receiptEpoch, checkpoint, ackIntentEpoch,
          acked, uploaded, rootEpoch, deliveryDone, released>>

Init ==
  /\ currentOwner = "None"
  /\ currentEpoch = 0
  /\ workerEpoch = [w \in Workers |-> 0]
  /\ attempts = {}
  /\ externallyApplied = {}
  /\ receiptEpoch = [p \in Positions |-> 0]
  /\ checkpoint = 0
  /\ ackIntentEpoch = [p \in Positions |-> 0]
  /\ acked = {}
  /\ uploaded = {}
  /\ rootEpoch = [p \in Positions |-> 0]
  /\ deliveryDone = {}
  /\ released = {}

Owns(w) ==
  /\ currentOwner = w
  /\ workerEpoch[w] = currentEpoch
  /\ currentEpoch > 0

Acquire(w) ==
  /\ currentEpoch < MaxEpoch
  /\ currentOwner' = w
  /\ currentEpoch' = currentEpoch + 1
  /\ workerEpoch' = [workerEpoch EXCEPT ![w] = currentEpoch + 1]
  /\ UNCHANGED <<attempts, externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, uploaded, rootEpoch,
                  deliveryDone, released>>

Prepare(w, p) ==
  /\ Owns(w)
  /\ p \notin attempts
  /\ attempts' = attempts \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, uploaded, rootEpoch,
                  deliveryDone, released>>

ApplyExternal(p) ==
  /\ p \in attempts
  /\ p \notin externallyApplied
  /\ externallyApplied' = externallyApplied \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  receiptEpoch, checkpoint, ackIntentEpoch, acked,
                  uploaded, rootEpoch, deliveryDone, released>>

FinalizeDelivery(w, p) ==
  /\ Owns(w)
  /\ p \in externallyApplied
  /\ p >= checkpoint
  /\ receiptEpoch[p] = 0
  /\ ackIntentEpoch[p] = 0
  /\ receiptEpoch' = [receiptEpoch EXCEPT ![p] = currentEpoch]
  /\ checkpoint' = p
  /\ ackIntentEpoch' = [ackIntentEpoch EXCEPT ![p] = currentEpoch]
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, acked, uploaded, rootEpoch,
                  deliveryDone, released>>

UploadArtifact(p) ==
  /\ p \notin uploaded
  /\ uploaded' = uploaded \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, rootEpoch, deliveryDone, released>>

PublishArtifact(w, p) ==
  /\ Owns(w)
  /\ p \in uploaded
  /\ p >= checkpoint
  /\ rootEpoch[p] = 0
  /\ ackIntentEpoch[p] = 0
  /\ rootEpoch' = [rootEpoch EXCEPT ![p] = currentEpoch]
  /\ checkpoint' = p
  /\ ackIntentEpoch' = [ackIntentEpoch EXCEPT ![p] = currentEpoch]
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, acked, uploaded,
                  deliveryDone, released>>

AuthorizeInitialCut(w, p) ==
  /\ Owns(w)
  /\ p >= checkpoint
  /\ ackIntentEpoch[p] = 0
  /\ checkpoint' = p
  /\ ackIntentEpoch' = [ackIntentEpoch EXCEPT ![p] = currentEpoch]
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, acked, uploaded,
                  rootEpoch, deliveryDone, released>>

AckSource(w, p) ==
  /\ Owns(w)
  /\ ackIntentEpoch[p] > 0
  /\ p <= checkpoint
  /\ p \notin acked
  /\ acked' = acked \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, uploaded, rootEpoch, deliveryDone, released>>

CompleteArtifactDelivery(w, p) ==
  /\ Owns(w)
  /\ rootEpoch[p] > 0
  /\ p \notin deliveryDone
  /\ deliveryDone' = deliveryDone \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, uploaded, rootEpoch, released>>

MarkRetainedRoot(w, p) ==
  /\ Owns(w)
  /\ rootEpoch[p] > 0
  /\ p \in acked
  /\ p \in deliveryDone
  /\ p < checkpoint
  /\ p \notin released
  /\ released' = released \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, uploaded, rootEpoch, deliveryDone>>

CollectOrphan(p) ==
  /\ p \in uploaded
  /\ rootEpoch[p] = 0
  /\ uploaded' = uploaded \ {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, rootEpoch, deliveryDone, released>>

SweepRetained(p) ==
  /\ p \in released
  /\ p \in uploaded
  /\ uploaded' = uploaded \ {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, rootEpoch, deliveryDone, released>>

Next ==
  \/ \E w \in Workers: Acquire(w)
  \/ \E w \in Workers, p \in Positions: Prepare(w, p)
  \/ \E p \in Positions: ApplyExternal(p)
  \/ \E w \in Workers, p \in Positions: FinalizeDelivery(w, p)
  \/ \E p \in Positions: UploadArtifact(p)
  \/ \E w \in Workers, p \in Positions: PublishArtifact(w, p)
  \/ \E w \in Workers, p \in Positions: AuthorizeInitialCut(w, p)
  \/ \E w \in Workers, p \in Positions: AckSource(w, p)
  \/ \E w \in Workers, p \in Positions: CompleteArtifactDelivery(w, p)
  \/ \E w \in Workers, p \in Positions: MarkRetainedRoot(w, p)
  \/ \E p \in Positions: CollectOrphan(p)
  \/ \E p \in Positions: SweepRetained(p)

Spec == Init /\ [][Next]_vars

TypeInvariant ==
  /\ currentOwner \in Workers \cup {"None"}
  /\ currentEpoch \in 0..MaxEpoch
  /\ workerEpoch \in [Workers -> 0..MaxEpoch]
  /\ attempts \subseteq Positions
  /\ externallyApplied \subseteq Positions
  /\ receiptEpoch \in [Positions -> 0..MaxEpoch]
  /\ checkpoint \in 0..MaxPosition
  /\ ackIntentEpoch \in [Positions -> 0..MaxEpoch]
  /\ acked \subseteq Positions
  /\ uploaded \subseteq Positions
  /\ rootEpoch \in [Positions -> 0..MaxEpoch]
  /\ deliveryDone \subseteq Positions
  /\ released \subseteq Positions

ExternalCommitRequiresAttempt == externallyApplied \subseteq attempts
ReceiptRequiresExternalCommit ==
  \A p \in Positions: receiptEpoch[p] > 0 => p \in externallyApplied
ReceiptCheckpointAckAtomic ==
  \A p \in Positions:
    receiptEpoch[p] > 0 =>
      /\ ackIntentEpoch[p] = receiptEpoch[p]
      /\ checkpoint >= p
ArtifactCheckpointAckAtomic ==
  \A p \in Positions:
    rootEpoch[p] > 0 =>
      /\ ackIntentEpoch[p] = rootEpoch[p]
      /\ checkpoint >= p
AckSafety ==
  \A p \in acked:
    /\ ackIntentEpoch[p] > 0
    /\ checkpoint >= p
ActivePublishedArtifactsRemainPresent ==
  \A p \in Positions: rootEpoch[p] > 0 /\ p \notin released => p \in uploaded
RetentionSafety ==
  \A p \in released:
    /\ rootEpoch[p] > 0
    /\ p \in acked
    /\ p \in deliveryDone
    /\ p < checkpoint
AuthoritativeWritesHaveFence ==
  \A p \in Positions:
    /\ receiptEpoch[p] <= currentEpoch
    /\ rootEpoch[p] <= currentEpoch
    /\ ackIntentEpoch[p] <= currentEpoch

====
