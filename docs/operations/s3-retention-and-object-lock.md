# S3 retention and object lock

Canonical recovery buckets must enable versioning. Wallaby records and verifies the exact `VersionId`; a later delete marker or replacement of the current key does not change the rooted version.

Configure retention and object lock outside Wallaby for the required recovery window. PostgreSQL stores encryption and object-lock evidence when available, but this slice does not manage retention policies.

Version-one garbage collection is limited to unpublished, unrooted objects. Published retention GC is deferred. The artifact stream therefore enforces a hard total retained-byte limit and consumer backlog high-water marks.

Do not build recovery from bucket listings, object metadata alone, or a mutable `latest` pointer. Those sources provide reconciliation evidence only; PostgreSQL owns publication and reachability.
