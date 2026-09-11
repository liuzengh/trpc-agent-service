package domain

import (
	"errors"
	"testing"
)

func TestIntakeLimitsAreExplicitAndIndependent(t *testing.T) {
	for name, limits := range map[string]IntakeLimits{
		"zero":              {},
		"missing retained":  {MaxQueuedRuns: 1},
		"missing queued":    {MaxRetainedRuns: 1},
		"negative queued":   {MaxQueuedRuns: -1, MaxRetainedRuns: 1},
		"negative retained": {MaxQueuedRuns: 1, MaxRetainedRuns: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(limits.Validate(), ErrInvalid) {
				t.Fatal("invalid intake limits accepted")
			}
		})
	}
	// A lowered retained limit may be below the active limit. That restricts new
	// admissions, not recovery of already accepted Runs.
	for _, limits := range []IntakeLimits{{MaxQueuedRuns: 1, MaxRetainedRuns: 2}, {MaxQueuedRuns: 2, MaxRetainedRuns: 1}} {
		if err := limits.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
