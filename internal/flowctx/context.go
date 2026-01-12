package flowctx

import "context"

type flowIDKey struct{}

// ContextWithFlowID annotates a context with a flow ID.
func ContextWithFlowID(ctx context.Context, flowID string) context.Context {
	if flowID == "" || ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, flowIDKey{}, flowID)
}

// FlowIDFromContext returns the flow ID stored in the context, if any.
func FlowIDFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	value, ok := ctx.Value(flowIDKey{}).(string)
	if !ok || value == "" {
		return "", false
	}
	return value, true
}
