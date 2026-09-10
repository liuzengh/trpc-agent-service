package worker_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestExecutionFailureClassesPreserveCause(t *testing.T) {
	cause := errors.New("backend unavailable")
	if err := worker.NewRetryableExecutionError(cause); !worker.IsRetryableExecutionError(err) || !errors.Is(err, cause) {
		t.Fatalf("retryable classification = %v", err)
	}
	if err := worker.NewPermanentExecutionError(cause); !worker.IsPermanentExecutionError(err) || !errors.Is(err, cause) {
		t.Fatalf("permanent classification = %v", err)
	}
	if err := worker.NewSideEffectUncertainError(cause); !worker.IsSideEffectUncertainError(err) || !errors.Is(err, cause) {
		t.Fatalf("side-effect classification = %v", err)
	}
}

func TestExecutionFailureClassesSupportWrappedPointers(t *testing.T) {
	cause := errors.New("backend unavailable")
	if err := fmt.Errorf("wrapped: %w", &worker.RetryableExecutionError{Err: cause}); !worker.IsRetryableExecutionError(err) {
		t.Fatal("wrapped pointer retryable classification was lost")
	}
	if err := fmt.Errorf("wrapped: %w", &worker.PermanentExecutionError{Err: cause}); !worker.IsPermanentExecutionError(err) {
		t.Fatal("wrapped pointer permanent classification was lost")
	}
	if err := fmt.Errorf("wrapped: %w", &worker.SideEffectUncertainError{Err: cause}); !worker.IsSideEffectUncertainError(err) {
		t.Fatal("wrapped pointer side-effect classification was lost")
	}
}
