package gateway

import (
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestParkPolicyValidation(t *testing.T) {
	if err := DefaultParkPolicy().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []ParkPolicy{
		{},
		{BaseDelay: time.Second, MaxDelay: time.Second, Deadline: time.Second, MaxAttempts: 0},
		{BaseDelay: 2 * time.Second, MaxDelay: time.Second, Deadline: time.Minute, MaxAttempts: 1},
		{BaseDelay: time.Second, MaxDelay: time.Minute, Deadline: time.Second, MaxAttempts: 1},
	} {
		if err := policy.Validate(); !errors.Is(err, runtime.ErrInvariantViolation) {
			t.Fatalf("policy=%#v err=%v", policy, err)
		}
	}
}
