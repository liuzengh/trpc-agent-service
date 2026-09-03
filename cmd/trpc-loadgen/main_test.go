package main

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	values := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
	}
	if got := percentile(values, 0.50); got != 20*time.Millisecond {
		t.Fatalf("p50 = %s", got)
	}
	if got := percentile(values, 1); got != 40*time.Millisecond {
		t.Fatalf("max = %s", got)
	}
}
