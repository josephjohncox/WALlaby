---- MODULE ManagedPostgresDelivery ----
EXTENDS Naturals, TLC

(***************************************************************************
 Full-transaction managed PostgreSQL delivery.

 PostgreSQL-authoritative attempts precede target side effects. A target-side
 logical-batch marker reconciles an ambiguous commit. Receipt, checkpoint, and
 ACK intent finalize atomically under the current producer epoch. Source flush
 is a separate monotonic external step; a crash or takeover may occur before
 the current owner records its observation, and recovery may re-send the same
 authorized checkpoint. Side-effect attempts, reconciliation retries, and
 retained terminal history are bounded.
***************************************************************************)

CONSTANTS Workers, MaxPosition, MaxEpoch, MaxAttempts

ASSUME /\ Workers # {}
       /\ "None" \notin Workers
       /\ MaxPosition \in Nat \ {0}
       /\ MaxEpoch \in Nat \ {0}
       /\ MaxAttempts \in Nat \ {0}

Positions == 1..MaxPosition

VARIABLES owner, epoch, workerEpoch, attemptCount, reconcileCount,
          targetApplied, receipts, checkpoint, ackIntents, sourceFlushed,
          flushReceipts, retained, retentionRoot

vars == <<owner, epoch, workerEpoch, attemptCount, reconcileCount,
          targetApplied, receipts, checkpoint, ackIntents, sourceFlushed,
          flushReceipts, retained, retentionRoot>>

Init ==
  /\ owner = "None"
  /\ epoch = 0
  /\ workerEpoch = [w \in Workers |-> 0]
  /\ attemptCount = [p \in Positions |-> 0]
  /\ reconcileCount = [p \in Positions |-> 0]
  /\ targetApplied = {}
  /\ receipts = {}
  /\ checkpoint = 0
  /\ ackIntents = {}
  /\ sourceFlushed = {}
  /\ flushReceipts = {}
  /\ retained = {}
  /\ retentionRoot = 0

Owns(w) ==
  /\ owner = w
  /\ epoch > 0
  /\ workerEpoch[w] = epoch

Acquire(w) ==
  /\ epoch < MaxEpoch
  /\ owner' = w
  /\ epoch' = epoch + 1
  /\ workerEpoch' = [workerEpoch EXCEPT ![w] = epoch + 1]
  /\ UNCHANGED <<attemptCount, reconcileCount, targetApplied, receipts,
                  checkpoint, ackIntents, sourceFlushed, flushReceipts,
                  retained, retentionRoot>>

Prepare(w, p) ==
  /\ Owns(w)
  /\ p \notin receipts
  /\ attemptCount[p] < MaxAttempts
  /\ attemptCount' = [attemptCount EXCEPT ![p] = @ + 1]
  /\ UNCHANGED <<owner, epoch, workerEpoch, reconcileCount, targetApplied,
                  receipts, checkpoint, ackIntents, sourceFlushed,
                  flushReceipts, retained, retentionRoot>>

ApplyExternal(p) ==
  /\ attemptCount[p] > 0
  /\ targetApplied' = targetApplied \cup {p}
  /\ UNCHANGED <<owner, epoch, workerEpoch, attemptCount, reconcileCount,
                  receipts, checkpoint, ackIntents, sourceFlushed,
                  flushReceipts, retained, retentionRoot>>

ReconcileIndeterminate(w, p) ==
  /\ Owns(w)
  /\ attemptCount[p] > 0
  /\ p \notin receipts
  /\ reconcileCount[p] < MaxAttempts
  /\ reconcileCount' = [reconcileCount EXCEPT ![p] = @ + 1]
  /\ UNCHANGED <<owner, epoch, workerEpoch, attemptCount, targetApplied,
                  receipts, checkpoint, ackIntents, sourceFlushed,
                  flushReceipts, retained, retentionRoot>>

Finalize(w, p) ==
  /\ Owns(w)
  /\ p \in targetApplied
  /\ p \notin receipts
  /\ p >= checkpoint
  /\ receipts' = receipts \cup {p}
  /\ checkpoint' = p
  /\ ackIntents' = ackIntents \cup {p}
  /\ retained' = retained \cup {p}
  /\ UNCHANGED <<owner, epoch, workerEpoch, attemptCount, reconcileCount,
                  targetApplied, sourceFlushed, flushReceipts, retentionRoot>>

FlushSource(w, p) ==
  /\ Owns(w)
  /\ p \in ackIntents
  /\ p <= checkpoint
  /\ sourceFlushed' = sourceFlushed \cup {p}
  /\ UNCHANGED <<owner, epoch, workerEpoch, attemptCount, reconcileCount,
                  targetApplied, receipts, checkpoint, ackIntents,
                  flushReceipts, retained, retentionRoot>>

RecordFlushReceipt(w, p) ==
  /\ Owns(w)
  /\ p \in sourceFlushed
  /\ p \in ackIntents
  /\ p <= checkpoint
  /\ flushReceipts' = flushReceipts \cup {p}
  /\ UNCHANGED <<owner, epoch, workerEpoch, attemptCount, reconcileCount,
                  targetApplied, receipts, checkpoint, ackIntents,
                  sourceFlushed, retained, retentionRoot>>

AdvanceRetentionRoot(w, p) ==
  /\ Owns(w)
  /\ p \in flushReceipts
  /\ p <= checkpoint
  /\ p >= retentionRoot
  /\ retentionRoot' = p
  /\ UNCHANGED <<owner, epoch, workerEpoch, attemptCount, reconcileCount,
                  targetApplied, receipts, checkpoint, ackIntents,
                  sourceFlushed, flushReceipts, retained>>

PruneTerminal(w, p) ==
  /\ Owns(w)
  /\ p \in retained
  /\ p \in flushReceipts
  /\ p < retentionRoot
  /\ retained' = retained \ {p}
  /\ UNCHANGED <<owner, epoch, workerEpoch, attemptCount, reconcileCount,
                  targetApplied, receipts, checkpoint, ackIntents,
                  sourceFlushed, flushReceipts, retentionRoot>>

Next ==
  \/ \E w \in Workers: Acquire(w)
  \/ \E w \in Workers, p \in Positions: Prepare(w, p)
  \/ \E p \in Positions: ApplyExternal(p)
  \/ \E w \in Workers, p \in Positions: ReconcileIndeterminate(w, p)
  \/ \E w \in Workers, p \in Positions: Finalize(w, p)
  \/ \E w \in Workers, p \in Positions: FlushSource(w, p)
  \/ \E w \in Workers, p \in Positions: RecordFlushReceipt(w, p)
  \/ \E w \in Workers, p \in Positions: AdvanceRetentionRoot(w, p)
  \/ \E w \in Workers, p \in Positions: PruneTerminal(w, p)

Spec == Init /\ [][Next]_vars

TypeInvariant ==
  /\ owner \in Workers \cup {"None"}
  /\ epoch \in 0..MaxEpoch
  /\ workerEpoch \in [Workers -> 0..MaxEpoch]
  /\ attemptCount \in [Positions -> 0..MaxAttempts]
  /\ reconcileCount \in [Positions -> 0..MaxAttempts]
  /\ targetApplied \subseteq Positions
  /\ receipts \subseteq Positions
  /\ checkpoint \in 0..MaxPosition
  /\ ackIntents \subseteq Positions
  /\ sourceFlushed \subseteq Positions
  /\ flushReceipts \subseteq Positions
  /\ retained \subseteq Positions
  /\ retentionRoot \in 0..MaxPosition

ExternalCommitRequiresAttempt ==
  \A p \in targetApplied: attemptCount[p] > 0
ReceiptRequiresExternalCommit == receipts \subseteq targetApplied
ReceiptCheckpointAckAtomic ==
  \A p \in receipts: /\ p \in ackIntents /\ p <= checkpoint
SourceFlushRequiresAuthorization == sourceFlushed \subseteq ackIntents
FlushReceiptRequiresObservedSourceFlush == flushReceipts \subseteq sourceFlushed
RetryBounded ==
  /\ \A p \in Positions: attemptCount[p] <= MaxAttempts
  /\ \A p \in Positions: reconcileCount[p] <= MaxAttempts
RetentionRootProtectsCheckpoint ==
  checkpoint = 0 \/ checkpoint \in retained

====
