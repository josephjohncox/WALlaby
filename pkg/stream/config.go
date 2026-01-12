package stream

// AckPolicy controls when the source LSN is acknowledged.
type AckPolicy string

const (
	AckPolicyAll     AckPolicy = "all"
	AckPolicyPrimary AckPolicy = "primary"
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
