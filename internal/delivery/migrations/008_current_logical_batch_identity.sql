DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM delivery_manifests
    WHERE logical_batch_id IS DISTINCT FROM 'logical-batch:'||pg_catalog.encode(
      pg_catalog.sha256(pg_catalog.convert_to(source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(position_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(content_hash,'UTF8')),
      'hex'
    )
  ) OR EXISTS (
    SELECT 1 FROM delivery_attempts
    WHERE logical_batch_id IS DISTINCT FROM 'logical-batch:'||pg_catalog.encode(
      pg_catalog.sha256(pg_catalog.convert_to(source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(position_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(content_hash,'UTF8')),
      'hex'
    )
  ) OR EXISTS (
    SELECT 1 FROM delivery_receipts
    WHERE logical_batch_id IS DISTINCT FROM 'logical-batch:'||pg_catalog.encode(
      pg_catalog.sha256(pg_catalog.convert_to(source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(position_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(content_hash,'UTF8')),
      'hex'
    )
  ) THEN
    RAISE EXCEPTION 'delivery current-schema migration refuses noncanonical logical batch identities; recreate incompatible delivery state'
      USING ERRCODE='55000';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM (
      SELECT flow_incarnation_id,destination_revision_id,logical_batch_id,
             position_id,source_lineage_id,content_hash
      FROM delivery_manifests
      UNION ALL
      SELECT flow_incarnation_id,destination_revision_id,logical_batch_id,
             position_id,source_lineage_id,content_hash
      FROM delivery_attempts
      UNION ALL
      SELECT flow_incarnation_id,destination_revision_id,logical_batch_id,
             position_id,source_lineage_id,content_hash
      FROM delivery_receipts
    ) AS identity_rows
    GROUP BY flow_incarnation_id,destination_revision_id,logical_batch_id
    HAVING count(DISTINCT ROW(position_id,source_lineage_id,content_hash))<>1
  ) OR EXISTS (
    SELECT 1
    FROM (
      SELECT flow_incarnation_id,destination_revision_id,logical_batch_id,position_id
      FROM delivery_manifests
      UNION ALL
      SELECT flow_incarnation_id,destination_revision_id,logical_batch_id,position_id
      FROM delivery_attempts
      UNION ALL
      SELECT flow_incarnation_id,destination_revision_id,logical_batch_id,position_id
      FROM delivery_receipts
    ) AS position_rows
    GROUP BY flow_incarnation_id,destination_revision_id,position_id
    HAVING count(DISTINCT logical_batch_id)<>1
  ) OR EXISTS (
    SELECT 1 FROM delivery_attempts
    GROUP BY flow_incarnation_id,destination_revision_id,logical_batch_id,attempt_number
    HAVING count(*)<>1
  ) OR EXISTS (
    SELECT 1
    FROM delivery_attempts AS attempt
    LEFT JOIN delivery_manifests AS manifest
      ON manifest.flow_incarnation_id=attempt.flow_incarnation_id
     AND manifest.destination_revision_id=attempt.destination_revision_id
     AND manifest.logical_batch_id=attempt.logical_batch_id
     AND manifest.position_id=attempt.position_id
     AND manifest.source_lineage_id=attempt.source_lineage_id
     AND manifest.content_hash=attempt.content_hash
    WHERE manifest.flow_incarnation_id IS NULL
  ) OR EXISTS (
    SELECT 1
    FROM delivery_receipts AS receipt
    LEFT JOIN delivery_attempts AS attempt
      ON attempt.attempt_id=receipt.attempt_id
     AND attempt.flow_incarnation_id=receipt.flow_incarnation_id
     AND attempt.destination_revision_id=receipt.destination_revision_id
     AND attempt.logical_batch_id=receipt.logical_batch_id
     AND attempt.position_id=receipt.position_id
     AND attempt.source_lineage_id=receipt.source_lineage_id
     AND attempt.content_hash=receipt.content_hash
    WHERE attempt.attempt_id IS NULL
  ) THEN
    RAISE EXCEPTION 'delivery current-schema migration refuses ambiguous logical batch rows; recreate incompatible delivery state'
      USING ERRCODE='55000';
  END IF;
END
$$;

ALTER TABLE delivery_manifests ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE delivery_attempts ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE delivery_receipts ALTER COLUMN logical_batch_id SET NOT NULL;

ALTER TABLE delivery_manifests DROP CONSTRAINT IF EXISTS delivery_manifests_logical_batch_current;
ALTER TABLE delivery_manifests ADD CONSTRAINT delivery_manifests_logical_batch_current CHECK (
  logical_batch_id='logical-batch:'||pg_catalog.encode(
    pg_catalog.sha256(pg_catalog.convert_to(source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(position_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(content_hash,'UTF8')),
    'hex'
  )
);
ALTER TABLE delivery_attempts DROP CONSTRAINT IF EXISTS delivery_attempts_logical_batch_current;
ALTER TABLE delivery_attempts ADD CONSTRAINT delivery_attempts_logical_batch_current CHECK (
  logical_batch_id='logical-batch:'||pg_catalog.encode(
    pg_catalog.sha256(pg_catalog.convert_to(source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(position_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(content_hash,'UTF8')),
    'hex'
  )
);
ALTER TABLE delivery_receipts DROP CONSTRAINT IF EXISTS delivery_receipts_logical_batch_current;
ALTER TABLE delivery_receipts ADD CONSTRAINT delivery_receipts_logical_batch_current CHECK (
  logical_batch_id='logical-batch:'||pg_catalog.encode(
    pg_catalog.sha256(pg_catalog.convert_to(source_lineage_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(position_id,'UTF8')||pg_catalog.decode('00','hex')||pg_catalog.convert_to(content_hash,'UTF8')),
    'hex'
  )
);

DROP INDEX IF EXISTS delivery_manifests_logical_batch_idx;
CREATE UNIQUE INDEX delivery_manifests_logical_batch_idx
  ON delivery_manifests(flow_incarnation_id,destination_revision_id,logical_batch_id);
DROP INDEX IF EXISTS delivery_receipts_logical_batch_idx;
CREATE UNIQUE INDEX delivery_receipts_logical_batch_idx
  ON delivery_receipts(flow_incarnation_id,destination_revision_id,logical_batch_id);
DROP INDEX IF EXISTS delivery_attempts_logical_batch_idx;
CREATE UNIQUE INDEX delivery_attempts_logical_batch_idx
  ON delivery_attempts(flow_incarnation_id,destination_revision_id,logical_batch_id,attempt_number);
