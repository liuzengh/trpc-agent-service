package main

import (
	"testing"
	"time"
)

func TestPositiveInt(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		t.Setenv("TEST_SUMMARY_INT", "")
		got, err := positiveInt("TEST_SUMMARY_INT", 4, 128)
		if err != nil || got != 4 {
			t.Fatalf("positiveInt() = %d, %v; want 4, nil", got, err)
		}
	})
	t.Run("valid", func(t *testing.T) {
		t.Setenv("TEST_SUMMARY_INT", " 17 ")
		got, err := positiveInt("TEST_SUMMARY_INT", 4, 128)
		if err != nil || got != 17 {
			t.Fatalf("positiveInt() = %d, %v; want 17, nil", got, err)
		}
	})
	for _, raw := range []string{"0", "-1", "129", "not-a-number"} {
		t.Run("reject_"+raw, func(t *testing.T) {
			t.Setenv("TEST_SUMMARY_INT", raw)
			if _, err := positiveInt("TEST_SUMMARY_INT", 4, 128); err == nil {
				t.Fatalf("positiveInt(%q) unexpectedly succeeded", raw)
			}
		})
	}
}

func TestPositiveDuration(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		t.Setenv("TEST_SUMMARY_DURATION", "")
		got, err := positiveDuration("TEST_SUMMARY_DURATION", time.Minute)
		if err != nil || got != time.Minute {
			t.Fatalf("positiveDuration() = %s, %v; want 1m, nil", got, err)
		}
	})
	t.Run("valid", func(t *testing.T) {
		t.Setenv("TEST_SUMMARY_DURATION", " 45s ")
		got, err := positiveDuration("TEST_SUMMARY_DURATION", time.Minute)
		if err != nil || got != 45*time.Second {
			t.Fatalf("positiveDuration() = %s, %v; want 45s, nil", got, err)
		}
	})
	for _, raw := range []string{"0s", "-1s", "not-a-duration"} {
		t.Run("reject_"+raw, func(t *testing.T) {
			t.Setenv("TEST_SUMMARY_DURATION", raw)
			if _, err := positiveDuration("TEST_SUMMARY_DURATION", time.Minute); err == nil {
				t.Fatalf("positiveDuration(%q) unexpectedly succeeded", raw)
			}
		})
	}
}

func TestSummaryLatencyBucketsCoverProductionTimeouts(t *testing.T) {
	want := []float64{30, 60, 120, 300}
	for _, expected := range want {
		found := false
		for _, bucket := range summaryLatencyBuckets {
			if bucket == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("summary latency histogram is missing %.0fs bucket", expected)
		}
	}
}
