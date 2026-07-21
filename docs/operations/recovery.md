# Recovery operations

## Delivery

On restart, the current producer fence inspects unfinished delivery attempts. It reconciles the destination marker before replay. Exact matching evidence is adoptable; conflicting or missing ambiguous evidence stops the flow.

## Bootstrap

If the exporter connection is alive, restart snapshot workers against the same exported snapshot. If it is gone, abandon the unpublished generation and create a new generation-qualified slot. Do not reuse task cursors across exported snapshots.

## Artifacts

PostgreSQL reservations survive process failure. Re-encoding produces the same artifact identity and bytes. The publisher reconciles exact S3 versions, verifies the checksum, and then publishes. `RecomputeQuota` rebuilds reserved and rooted byte totals from PostgreSQL rows only.

## Stale workers

Do not force a stale worker to finish or fail a flow. Let the lease expire, acquire a higher lease epoch, and recover under the new fence. Stale evidence may exist externally, but it cannot authorize a checkpoint or source ACK.
