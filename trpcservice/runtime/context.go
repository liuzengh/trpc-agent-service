package runtime

import "context"

type ExecutionContext struct {
	TenantID          string
	RequestID         string
	SubjectID         string
	PolicyVersion     int64
	GrantID           string
	GrantVersion      int64
	ToolCallID        string
	ArgsDigest        string
	PayloadKeyVersion int64
}

type executionContextKey struct{}

func WithExecutionContext(ctx context.Context, value ExecutionContext) context.Context {
	return context.WithValue(ctx, executionContextKey{}, value)
}

func ExecutionContextFrom(ctx context.Context) (ExecutionContext, bool) {
	value, ok := ctx.Value(executionContextKey{}).(ExecutionContext)
	return value, ok && value.TenantID != "" && value.RequestID != ""
}
