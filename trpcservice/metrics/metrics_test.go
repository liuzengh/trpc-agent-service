package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsExposeTenantLabels(t *testing.T) {
	recorder := New()
	recorder.ObserveRequest("tenant-a", "webui", "ingested", "", 25*time.Millisecond)
	recorder.ObserveRequest("tenant-a", "webui", "failed", "model_timeout", time.Second)
	recorder.ObserveModel("tenant-a", "model-a", 100*time.Millisecond, 42)
	recorder.ObserveTool("tenant-a", "search", 10*time.Millisecond, false)
	recorder.ObserveDelivery("tenant-a", "webui", false)
	recorder.ObserveSessionBackend("redis", 5*time.Millisecond)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	recorder.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	body := response.Body.String()
	for _, expected := range []string{
		`agent_requests_total{channel="webui",decision="ingested",tenant="tenant-a"} 1`,
		`agent_errors_total{error_type="model_timeout",tenant="tenant-a"} 1`,
		`model_calls_total{model="model-a",tenant="tenant-a"} 1`,
		`token_usage_total{model="model-a",tenant="tenant-a"} 42`,
		`im_delivery_total{channel="webui",status="success",tenant="tenant-a"} 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("metrics body missing %q", expected)
		}
	}
}
