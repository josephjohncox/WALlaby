---- MODULE ManagedDurability ----
EXTENDS Naturals, TLC

(***************************************************************************
 Managed CDC authority across PostgreSQL and external side effects.

 External target commits and S3 uploads may survive a producer takeover.
 PostgreSQL-authoritative receipts, artifact roots, checkpoints, and source
 ACK intents are written only by the current lease epoch. Finalization writes
 each receipt/root, checkpoint, and ACK intent atomically. Garbage collection
 may remove only unrooted uploads.
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
  rootEpoch

vars == <<currentOwner, currentEpoch, workerEpoch, attempts,
          externallyApplied, receiptEpoch, checkpoint, ackIntentEpoch,
          acked, uploaded, rootEpoch>>

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
                  ackIntentEpoch, acked, uploaded, rootEpoch>>

Prepare(w, p) ==
  /\ Owns(w)
  /\ p \notin attempts
  /\ attempts' = attempts \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, uploaded, rootEpoch>>

ApplyExternal(p) ==
  /\ p \in attempts
  /\ p \notin externallyApplied
  /\ externallyApplied' = externallyApplied \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  receiptEpoch, checkpoint, ackIntentEpoch, acked,
                  uploaded, rootEpoch>>

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
                  externallyApplied, acked, uploaded, rootEpoch>>

UploadArtifact(p) ==
  /\ p \notin uploaded
  /\ uploaded' = uploaded \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, rootEpoch>>

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
                  externallyApplied, receiptEpoch, acked, uploaded>>

AuthorizeInitialCut(w, p) ==
  /\ Owns(w)
  /\ p >= checkpoint
  /\ ackIntentEpoch[p] = 0
  /\ checkpoint' = p
  /\ ackIntentEpoch' = [ackIntentEpoch EXCEPT ![p] = currentEpoch]
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, acked, uploaded, rootEpoch>>

AckSource(w, p) ==
  /\ Owns(w)
  /\ ackIntentEpoch[p] > 0
  /\ p <= checkpoint
  /\ p \notin acked
  /\ acked' = acked \cup {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, uploaded, rootEpoch>>

CollectOrphan(p) ==
  /\ p \in uploaded
  /\ rootEpoch[p] = 0
  /\ uploaded' = uploaded \ {p}
  /\ UNCHANGED <<currentOwner, currentEpoch, workerEpoch, attempts,
                  externallyApplied, receiptEpoch, checkpoint,
                  ackIntentEpoch, acked, rootEpoch>>

Next ==
  \/ \E w \in Workers: Acquire(w)
  \/ \E w \in Workers, p \in Positions: Prepare(w, p)
  \/ \E p \in Positions: ApplyExternal(p)
  \/ \E w \in Workers, p \in Positions: FinalizeDelivery(w, p)
  \/ \E p \in Positions: UploadArtifact(p)
  \/ \E w \in Workers, p \in Positions: PublishArtifact(w, p)
  \/ \E w \in Workers, p \in Positions: AuthorizeInitialCut(w, p)
  \/ \E w \in Workers, p \in Positions: AckSource(w, p)
  \/ \E p \in Positions: CollectOrphan(p)

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
PublishedArtifactsRemainPresent ==
  \A p \in Positions: rootEpoch[p] > 0 => p \in uploaded
AuthoritativeWritesHaveFence ==
  \A p \in Positions:
    /\ receiptEpoch[p] <= currentEpoch
    /\ rootEpoch[p] <= currentEpoch
    /\ ackIntentEpoch[p] <= currentEpoch

====
