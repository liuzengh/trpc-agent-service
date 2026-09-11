package delivery

import (
	"errors"
	"testing"
)

func TestResultValidationAndTypedFailure(t *testing.T) {
	if err := (Result{Outcome: Confirmed}).Validate(); err != nil {
		t.Fatalf("confirmed result: %v", err)
	}
	if err := (Result{}).Validate(); err == nil {
		t.Fatal("empty outcome unexpectedly valid")
	}
	failure := (Result{Outcome: Unknown, ErrorType: "network_unknown"}).Failure()
	var typed *FailureError
	if !errors.As(failure, &typed) || typed.Outcome != Unknown || typed.Category != "network_unknown" {
		t.Fatalf("typed failure = %#v", failure)
	}
	if typed.RetryWholeTask {
		t.Fatal("unknown outcome must never permit whole-task retry")
	}
	retryFailure := (Result{Outcome: RetryableNotSent, ErrorType: "rate_limited"}).Failure()
	if !errors.As(retryFailure, &typed) || !typed.RetryWholeTask {
		t.Fatalf("first retryable operation should permit whole-task retry: %#v", retryFailure)
	}
}
