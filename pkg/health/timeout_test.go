package health

import (
	"strings"
	"testing"
	"time"
)

func TestValidateDrainTimeoutRequiresOperationAndPersistenceBudget(t *testing.T) {
	if err := ValidateDrainTimeout("consumer", time.Minute, 90*time.Second, 30*time.Second); err != nil {
		t.Fatalf("valid drain budget rejected: %v", err)
	}
	err := ValidateDrainTimeout("consumer", time.Minute, 89*time.Second, 30*time.Second)
	if err == nil || !strings.Contains(err.Error(), "at least") {
		t.Fatalf("short drain budget error = %v", err)
	}
}

func TestValidateDrainTimeoutRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name   string
		op     time.Duration
		drain  time.Duration
		margin time.Duration
	}{
		{"zero operation", 0, time.Second, 0},
		{"zero drain", time.Second, 0, 0},
		{"negative margin", time.Second, time.Second, -time.Second},
	} {
		if err := ValidateDrainTimeout(test.name, test.op, test.drain, test.margin); err == nil {
			t.Fatalf("%s unexpectedly accepted", test.name)
		}
	}
}
