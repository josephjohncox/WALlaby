DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM artifact_delivery_attempts AS attempt
    LEFT JOIN artifact_publications AS publication
      ON publication.publication_id=attempt.publication_id
     AND publication.flow_incarnation_id=attempt.flow_incarnation_id
    WHERE publication.publication_id IS NULL
       OR attempt.manifest_sha256 IS NULL
       OR attempt.manifest_sha256 !~ '^[0-9a-f]{64}$'
       OR attempt.logical_batch_id IS NULL
       OR btrim(attempt.logical_batch_id)=''
       OR attempt.logical_batch_id LIKE 'legacy:%'
       OR attempt.logical_batch_id IS DISTINCT FROM publication.logical_batch_id
       OR publication.logical_batch_id IS DISTINCT FROM 'logical-batch:'||pg_catalog.encode(
         pg_catalog.sha256(
           pg_catalog.convert_to(publication.source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||
           pg_catalog.convert_to(publication.position_id,'UTF8')||pg_catalog.decode('00','hex')||
           pg_catalog.convert_to(publication.content_hash,'UTF8')
         ),
         'hex'
       )
       OR attempt.commit_id IS DISTINCT FROM 'wallaby-iceberg-'||pg_catalog.encode(
         pg_catalog.sha256(
           pg_catalog.convert_to('wallaby.iceberg.commit.v1','UTF8')||pg_catalog.decode('00','hex')||
           pg_catalog.convert_to(attempt.flow_incarnation_id::text,'UTF8')||pg_catalog.decode('00','hex')||
           pg_catalog.convert_to(btrim(attempt.consumer_revision_id),'UTF8')||pg_catalog.decode('00','hex')||
           pg_catalog.convert_to(attempt.publication_id::text,'UTF8')||pg_catalog.decode('00','hex')||
           pg_catalog.convert_to(btrim(attempt.manifest_sha256),'UTF8')||pg_catalog.decode('00','hex')
         ),
         'hex'
       )
  ) THEN
    RAISE EXCEPTION 'artifact catalog-attempt migration refuses noncanonical attempt identities; recreate incompatible artifact consumer state'
      USING ERRCODE='55000';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM artifact_delivery_receipts AS receipt
    LEFT JOIN artifact_delivery_attempts AS attempt
      ON attempt.attempt_id=receipt.attempt_id
    WHERE attempt.attempt_id IS NULL
       OR receipt.flow_incarnation_id<>attempt.flow_incarnation_id
       OR receipt.consumer_revision_id<>attempt.consumer_revision_id
       OR receipt.publication_id<>attempt.publication_id
       OR receipt.content_hash<>attempt.manifest_sha256
       OR receipt.commit_id IS NULL
       OR receipt.commit_id<>attempt.commit_id
       OR receipt.logical_batch_id IS NULL
       OR receipt.logical_batch_id<>attempt.logical_batch_id
       OR btrim(receipt.logical_batch_id)=''
       OR receipt.logical_batch_id LIKE 'legacy:%'
  ) OR EXISTS (
    SELECT 1
    FROM artifact_consumer_checkpoints AS checkpoint
    LEFT JOIN artifact_delivery_receipts AS receipt
      ON receipt.flow_incarnation_id=checkpoint.flow_incarnation_id
     AND receipt.consumer_revision_id=checkpoint.consumer_revision_id
     AND receipt.publication_id=checkpoint.publication_id
    WHERE receipt.publication_id IS NULL
       OR checkpoint.commit_id<>receipt.commit_id
       OR checkpoint.position_id<>receipt.position_id
       OR checkpoint.checkpoint_lsn<>receipt.checkpoint_lsn
       OR checkpoint.snapshot_id<>receipt.snapshot_id
  ) THEN
    RAISE EXCEPTION 'artifact catalog-attempt migration refuses conflicting receipt or checkpoint identities; recreate incompatible artifact consumer state'
      USING ERRCODE='55000';
  END IF;

  IF EXISTS (
    SELECT 1 FROM artifact_delivery_attempts
    GROUP BY flow_incarnation_id,consumer_revision_id,publication_id
    HAVING count(*)<>1
  ) OR EXISTS (
    SELECT 1 FROM artifact_delivery_attempts
    GROUP BY flow_incarnation_id,consumer_revision_id,commit_id
    HAVING count(DISTINCT ROW(publication_id,manifest_sha256,logical_batch_id))<>1
  ) OR EXISTS (
    SELECT 1 FROM artifact_delivery_receipts
    GROUP BY attempt_id
    HAVING count(*)<>1
  ) THEN
    RAISE EXCEPTION 'artifact catalog-attempt migration refuses ambiguous attempt identities; recreate incompatible artifact consumer state'
      USING ERRCODE='55000';
  END IF;
END
$$;

ALTER TABLE artifact_delivery_attempts ALTER COLUMN commit_id DROP DEFAULT;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN manifest_sha256 DROP DEFAULT;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN logical_batch_id DROP DEFAULT;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN commit_id SET NOT NULL;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN manifest_sha256 SET NOT NULL;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE artifact_delivery_attempts ADD CONSTRAINT artifact_delivery_attempts_current_identity CHECK (
  manifest_sha256 ~ '^[0-9a-f]{64}$'
  AND logical_batch_id ~ '^logical-batch:[0-9a-f]{64}$'
  AND commit_id='wallaby-iceberg-'||pg_catalog.encode(
    pg_catalog.sha256(
      pg_catalog.convert_to('wallaby.iceberg.commit.v1','UTF8')||pg_catalog.decode('00','hex')||
      pg_catalog.convert_to(flow_incarnation_id::text,'UTF8')||pg_catalog.decode('00','hex')||
      pg_catalog.convert_to(btrim(consumer_revision_id),'UTF8')||pg_catalog.decode('00','hex')||
      pg_catalog.convert_to(publication_id::text,'UTF8')||pg_catalog.decode('00','hex')||
      pg_catalog.convert_to(btrim(manifest_sha256),'UTF8')||pg_catalog.decode('00','hex')
    ),
    'hex'
  )
);
ALTER TABLE artifact_delivery_attempts ADD CONSTRAINT artifact_delivery_attempts_publication_unique
  UNIQUE (flow_incarnation_id,consumer_revision_id,publication_id);
ALTER TABLE artifact_delivery_attempts ADD CONSTRAINT artifact_delivery_attempts_commit_unique
  UNIQUE (flow_incarnation_id,consumer_revision_id,commit_id);

ALTER TABLE artifact_delivery_receipts ALTER COLUMN commit_id DROP DEFAULT;
ALTER TABLE artifact_delivery_receipts ALTER COLUMN logical_batch_id DROP DEFAULT;
ALTER TABLE artifact_delivery_receipts ALTER COLUMN commit_id SET NOT NULL;
ALTER TABLE artifact_delivery_receipts ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE artifact_delivery_receipts ADD CONSTRAINT artifact_delivery_receipts_current_identity CHECK (
  commit_id ~ '^wallaby-iceberg-[0-9a-f]{64}$'
  AND logical_batch_id ~ '^logical-batch:[0-9a-f]{64}$'
);
ALTER TABLE artifact_delivery_receipts ADD CONSTRAINT artifact_delivery_receipts_attempt_unique UNIQUE (attempt_id);
