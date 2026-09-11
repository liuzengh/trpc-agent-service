package metrics

import (
	"math"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestHistogramCumulativeBucketsAndBoundedLabels(t *testing.T) {
	m := NewMetrics()
	labels := map[string]string{"tenant": "tenant-a", "session_id": "must-not-appear", "le": "injected"}
	m.Observe("latency_seconds", "Latency.", .01, labels)
	m.Observe("latency_seconds", "Latency.", 2, labels)
	m.Observe("latency_seconds", "Latency.", math.NaN(), labels)
	out := httptest.NewRecorder()
	m.ServeHTTP(out, httptest.NewRequest("GET", "/metrics", nil))
	body := out.Body.String()
	for _, expected := range []string{`# TYPE latency_seconds histogram`, `latency_seconds_bucket{tenant="tenant-a",le="0.005"} 0`, `latency_seconds_bucket{tenant="tenant-a",le="0.01"} 1`, `latency_seconds_bucket{tenant="tenant-a",le="2.5"} 2`, `latency_seconds_bucket{tenant="tenant-a",le="+Inf"} 2`, `latency_seconds_count{tenant="tenant-a"} 2`, `latency_seconds_sum{tenant="tenant-a"} 2.01`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %s: %s", expected, body)
		}
	}
	if strings.Contains(body, "must-not-appear") || strings.Contains(body, "injected") {
		t.Fatal("unbounded/injected label escaped")
	}
}
func TestConcurrentHistogramObservations(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.Observe("duration", "Duration.", 1, nil) }()
	}
	wg.Wait()
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.values["duration_count"] != 100 || m.values[bucketKey("duration", nil, "+Inf")] != 100 {
		t.Fatal("lost observations")
	}
}
