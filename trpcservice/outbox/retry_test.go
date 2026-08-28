package outbox

import (
	"errors"
	"testing"
	"time"
)

func TestRetryPolicyDecidesEveryOutcome(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: 2 * time.Second, MaxDelay: 5 * time.Second}
	tests := []struct {
		name    string
		attempt int
		outcome SenderOutcome
		action  RetryAction
		code    string
		next    time.Time
	}{
		{name: "delivered", attempt: 1, outcome: SenderOutcome{Class: Delivered}, action: Complete},
		{name: "retryable", attempt: 1, outcome: SenderOutcome{Class: RetryableFailure, Code: SenderTimeoutCode}, action: Retry, code: SenderTimeoutCode, next: now.Add(2 * time.Second)},
		{name: "permanent", attempt: 1, outcome: SenderOutcome{Class: PermanentFailure, Code: SenderRejectedCode}, action: DeadLetter, code: SenderRejectedCode},
		{name: "unknown", attempt: 1, outcome: SenderOutcome{Class: Unknown, Code: "provider response body"}, action: Retry, code: DeliveryOutcomeUnknownCode, next: now.Add(2 * time.Second)},
		{name: "retry-at-boundary", attempt: 3, outcome: SenderOutcome{Class: RetryableFailure, Code: SenderUnavailableCode}, action: DeadLetter, code: SenderUnavailableCode},
		{name: "unknown-at-boundary", attempt: 3, outcome: SenderOutcome{Class: Unknown}, action: DeadLetter, code: DeliveryOutcomeUnknownCode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, err := policy.Decide(test.attempt, test.outcome, now)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Action != test.action || decision.Code != test.code {
				t.Fatalf("decision=%+v, want action=%s code=%s", decision, test.action, test.code)
			}
			if !test.next.IsZero() && !decision.NextAttempt.Equal(test.next) {
				t.Fatalf("next attempt=%s, want %s", decision.NextAttempt, test.next)
			}
			if test.next.IsZero() && !decision.NextAttempt.IsZero() {
				t.Fatalf("unexpected next attempt=%s", decision.NextAttempt)
			}
		})
	}
}

func TestRetryPolicyCapsDelayAndRejectsInvalidConfiguration(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	decision, err := (RetryPolicy{MaxAttempts: 10, BaseDelay: 3 * time.Second, MaxDelay: 5 * time.Second}).Decide(4, SenderOutcome{Class: RetryableFailure, Code: SenderUnavailableCode}, now)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.NextAttempt.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("delay was not capped: %+v", decision)
	}

	tests := []RetryPolicy{
		{MaxAttempts: 0, BaseDelay: time.Second, MaxDelay: time.Second},
		{MaxAttempts: MaxPolicyAttempts + 1, BaseDelay: time.Second, MaxDelay: time.Second},
		{MaxAttempts: 2, BaseDelay: -time.Second, MaxDelay: time.Second},
		{MaxAttempts: 2, BaseDelay: 2 * time.Second, MaxDelay: time.Second},
		{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: MaxPolicyDelay + time.Nanosecond},
	}
	for _, policy := range tests {
		if err := policy.Validate(); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("policy=%+v error=%v", policy, err)
		}
	}
	if _, err := (RetryPolicy{MaxAttempts: 2}).Decide(0, SenderOutcome{Class: RetryableFailure}, now); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("invalid attempt error=%v", err)
	}
	if _, err := (RetryPolicy{MaxAttempts: 2}).Decide(1, SenderOutcome{Class: OutcomeClass("other")}, now); !errors.Is(err, ErrInvalidOutcome) {
		t.Fatalf("invalid outcome error=%v", err)
	}
	if _, err := (RetryPolicy{MaxAttempts: 2}).Decide(1, SenderOutcome{Class: RetryableFailure}, time.Time{}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("zero clock error=%v", err)
	}
}

func TestClassifyOutcomeNeverKeepsArbitraryCode(t *testing.T) {
	for _, test := range []struct {
		outcome SenderOutcome
		want    string
	}{
		{SenderOutcome{Class: RetryableFailure, Code: "provider body with token"}, SenderUnavailableCode},
		{SenderOutcome{Class: PermanentFailure, Code: "https://example.invalid/?token=secret"}, SenderRejectedCode},
		{SenderOutcome{Class: Unknown, Code: "authorization: Bearer secret"}, DeliveryOutcomeUnknownCode},
	} {
		classified := ClassifyOutcome(test.outcome)
		if classified.Code != test.want {
			t.Fatalf("classified=%+v, want code %q", classified, test.want)
		}
	}
}
