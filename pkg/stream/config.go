package stream

// AckPolicy controls when the source LSN is acknowledged.
type AckPolicy string

const (
	AckPolicyAll     AckPolicy = "all"
	AckPolicyPrimary AckPolicy = "primary"
	// AckPolicyMaterialized acknowledges a source position only after the
	// canonical_cdc_parquet_v1 objects and their generation-fenced PostgreSQL
	// publication/checkpoint commit. The configured destination is not written
	// on the CDC path, and this release registers no production catalog consumer.
	AckPolicyMaterialized AckPolicy = "materialized"
)

// FailureMode describes how a flow treats its replication slot on failure.
type FailureMode string

const (
	FailureModeHoldSlot FailureMode = "hold_slot"
	FailureModeDropSlot FailureMode = "drop_slot"
)

// GiveUpPolicy controls whether to give up after retry exhaustion.
type GiveUpPolicy string

const (
	GiveUpPolicyNever             GiveUpPolicy = "never"
	GiveUpPolicyOnRetryExhaustion GiveUpPolicy = "on_retry_exhaustion"
)
