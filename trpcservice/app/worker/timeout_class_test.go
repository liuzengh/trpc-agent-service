package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestTimeoutAwareKeepsThePlatformDeadlineVisible pins the fix for what the
// model-timeout drill measured: a hung upstream was cut by the turn budget at
// ~283s and audited as run_error, because the framework reports the cancelled
// model call in its own words instead of wrapping context.DeadlineExceeded.
// "The provider hung and we cut it" has to be distinguishable from "the
// provider answered with an error" for an operator looking at the audit trail.
func TestTimeoutAwareKeepsThePlatformDeadlineVisible(t *testing.T) {
	expired, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-expired.Done()

	frameworksOwnWords := errors.New("worker: run agent \"a\": model call failed")
	wrapped := timeoutAware(expired, frameworksOwnWords)
	if !errors.Is(wrapped, context.DeadlineExceeded) {
		t.Errorf("wrapped error = %v, want it to carry context.DeadlineExceeded", wrapped)
	}
	if got := classifyRunError(wrapped); got != "timeout" {
		t.Errorf("classifyRunError = %q, want \"timeout\"", got)
	}
	if !errors.Is(wrapped, frameworksOwnWords) {
		t.Error("the original error must stay inspectable in the logs")
	}

	// A live context (the ordinary provider-error case) must not be relabelled.
	live := context.Background()
	plain := errors.New("provider returned 500")
	if got := timeoutAware(live, plain); got != plain {
		t.Errorf("timeoutAware on a live context = %v, want the error unchanged", got)
	}
	if got := classifyRunError(timeoutAware(live, plain)); got != "run_error" {
		t.Errorf("classifyRunError = %q, want \"run_error\"", got)
	}
	if got := timeoutAware(live, nil); got != nil {
		t.Errorf("timeoutAware(nil) = %v, want nil", got)
	}
}
