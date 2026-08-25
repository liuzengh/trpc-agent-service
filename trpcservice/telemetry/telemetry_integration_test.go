package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

func TestOTLPJaegerIntegration(t *testing.T) {
	jaegerURL := strings.TrimRight(os.Getenv("TEST_JAEGER_URL"), "/")
	if jaegerURL == "" {
		t.Skip("TEST_JAEGER_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	shutdown, err := Init(ctx, "trpc-agent-service-telemetry-integration")
	if err != nil {
		t.Fatal(err)
	}
	traceID := uuid.NewString()
	spanCtx := ContextWithTraceID(ctx, traceID)
	_, span := otel.Tracer("trpc-agent-service/telemetry-test").Start(spanCtx, "telemetry.integration")
	span.End()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("flush telemetry: %v", err)
	}

	hexTraceID := strings.ReplaceAll(traceID, "-", "")
	deadline := time.Now().Add(10 * time.Second)
	for {
		found, lookupErr := jaegerContainsTrace(ctx, jaegerURL, hexTraceID)
		if lookupErr == nil && found {
			t.Logf("Jaeger trace verified: %s", traceID)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Jaeger did not return trace %s: %v", traceID, lookupErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func jaegerContainsTrace(ctx context.Context, baseURL, traceID string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/traces/"+traceID, nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("Jaeger returned HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return false, err
	}
	return len(payload.Data) > 0, nil
}
