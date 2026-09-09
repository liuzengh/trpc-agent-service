package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func TestHTTPApprovalPauseCannotMakeUnknownExecutionRetrySafe(t *testing.T) {
	challenge, err := governance.NewMemoryApprovalStore().CreateChallenge(context.Background(), preflightApprovalRequest(preflightRequest()), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	approvalErr := &governance.ApprovalRequiredError{Challenge: challenge}
	for _, test := range []struct {
		name   string
		err    error
		paused bool
	}{
		{name: "verified pause", err: approvalErr, paused: true},
		{name: "unknown with nested approval", err: errors.Join(worker.ErrWorkerExecutionOutcomeUnknown, approvalErr)},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			markedPaused := 0
			paused := writeHTTPApprovalPause(response, test.err, func() { markedPaused++ })
			if paused != test.paused {
				t.Fatalf("HTTP approval branch = %v, want %v", paused, test.paused)
			}
			if paused {
				if response.Code != http.StatusPreconditionRequired || markedPaused != 1 {
					t.Fatalf("verified approval did not persist/respond as paused: status=%d writes=%d", response.Code, markedPaused)
				}
				return
			}
			failure := classifyWorkerProcessFailure(test.err)
			if response.Body.Len() != 0 || markedPaused != 0 || failure.safeToRetry || failure.statusCode != http.StatusLocked {
				t.Fatalf("unknown outcome reached approval response or retry-safe persistence: body=%q writes=%d failure=%+v", response.Body.String(), markedPaused, failure)
			}
		})
	}
}
