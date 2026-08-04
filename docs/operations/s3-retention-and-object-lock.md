# S3 retention and object lock

Canonical recovery buckets must have versioning status `Enabled`. Worker admission queries bucket versioning and rejects unversioned or versioning-suspended buckets. WALlaby records and verifies the exact non-null `VersionId`; a later delete marker or replacement of the current key does not change the rooted version.

Configure bucket Object Lock and any minimum compliance retention outside WALlaby. Exact-version HEAD responses populate PostgreSQL encryption mode, Object Lock mode, retain-until date, and legal-hold evidence when S3 returns those fields. If S3 refuses deletion because a version remains locked, the PostgreSQL GC claim stays durable, quota is not released, and artifact maintenance fails closed. Startup recovery or a subsequent pre-read maintenance pass will continue returning that error until S3 permits the exact-version delete; set `artifacts.retention` no shorter than the bucket retention period if this availability tradeoff is unacceptable.

Artifact garbage collection is PostgreSQL-authoritative epoch-based mark/sweep:

- unpublished reserved/uploaded/verified intents are eligible after `artifacts.orphan_grace`;
- a reserved intent with a prepared PUT but no stored version remains charged until replay reconciles it; GC does not treat one absence observation as proof that an old-fence PUT cannot still complete;
- published objects are eligible after `artifacts.retention` only when source feedback has an observed ACK receipt, every publication delivery is complete, and a newer authoritative checkpoint exists;
- the mark transaction records the fenced claim and removes the active root before exact-version deletion;
- finalization revalidates the claim and safety predicates before releasing retained-byte quota; and
- a publisher cannot root an object carrying a GC claim.

The hard retained-byte limit remains fail-stop containment. Batch-count, byte, and age backlog high-water marks pause source reads until consumers reduce the PostgreSQL backlog.

The worker IAM principal needs bucket-versioning inspection plus object-version operations used by the protocol: `s3:GetBucketVersioning`, `s3:PutObject`, `s3:ListBucketVersions`, `s3:GetObjectVersion`, `s3:GetObjectVersionAttributes` or the equivalent checksum-capable HEAD permission, and `s3:DeleteObjectVersion`. Object Lock deployments also need the read permissions for retention and legal-hold evidence. SSE-KMS deployments need the applicable KMS decrypt/data-key permissions for checksum retrieval. Scope these permissions to the configured bucket and artifact prefix.

Do not build recovery from bucket listings, object metadata alone, or a mutable `latest` pointer. Those sources provide reconciliation evidence only; PostgreSQL owns publication, delivery state, quota, and reachability.
