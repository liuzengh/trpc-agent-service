package knowledgedriver

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestClassifyUsesStableSemanticClasses(t *testing.T) {
	for name, input := range map[string]struct {
		err  error
		want string
	}{
		"none":                {err: nil, want: ""},
		"cancelled":           {err: context.Canceled, want: "cancelled"},
		"wrapped deadline":    {err: errors.Join(errors.New("request failed"), context.DeadlineExceeded), want: "deadline"},
		"invariant":           {err: runtime.ErrInvariantViolation, want: "invariant"},
		"tenant scope":        {err: runtime.ErrTenantScope, want: "tenant_scope"},
		"backend unavailable": {err: errors.New("vector endpoint reset"), want: "target_unavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := classify(input.err); got != input.want {
				t.Fatalf("classify(%v)=%q, want %q", input.err, got, input.want)
			}
		})
	}
}
