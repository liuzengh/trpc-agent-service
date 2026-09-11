package reply

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSenderTelegramUnknownHasDiagnosticsAndNeverResends(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "malformed_response"
		if timeout {
			name = "timeout_after_write"
		}
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			stop := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				if timeout {
					select {
					case <-r.Context().Done():
					case <-stop:
					}
					return
				}
				_, _ = io.WriteString(w, "private-provider-canary")
			}))
			t.Cleanup(server.Close)
			t.Cleanup(func() { close(stop) })
			server.Client().Timeout = 250 * time.Millisecond
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
			journal, writer, requestID, newSender := telegramRetryFixture(t, server)
			count, err := newSender("sender-a").ProcessOnce(context.Background())
			var delivery *channels.DeliveryError
			if count != 0 || !errors.As(err, &delivery) || !delivery.Unknown || len(journal.failures) != 1 || !journal.failures[0].terminal {
				t.Fatal("unknown delivery did not stop", err)
			}
			if count, err := newSender("sender-b").ProcessOnce(context.Background()); count != 0 || err != nil || requests.Load() != 1 {
				t.Fatal("new Sender repeated uncertain send", err)
			}
			events := writer.Events()
			if len(events) != 1 || events[0].RequestID != requestID || events[0].Decision != "reply_delivery_unknown" {
				t.Fatal("audit correlation missing")
			}
			kind, phase := "invalid_response", "response_body"
			if timeout {
				kind, phase = "timeout", "wait_response"
			}
			if events[0].Details["delivery_error_kind"] != kind || events[0].Details["delivery_phase"] != phase {
				t.Fatal("safe diagnostics missing from audit", events[0].Details)
			}
			spans := recorder.Ended()
			if len(spans) != 1 || spans[0].Name() != "reply.send" || spans[0].Status().Code != codes.Error {
				t.Fatal("failed reply trace was not marked error")
			}
			attrs := map[string]string{}
			for _, a := range spans[0].Attributes() {
				attrs[string(a.Key)] = a.Value.AsString()
			}
			if attrs["delivery.error.kind"] != kind || attrs["delivery.phase"] != phase || attrs["error.type"] != "channel_delivery_unknown" {
				t.Fatal("failed reply trace lacks diagnostics")
			}
			raw, _ := json.Marshal(events)
			if strings.Contains(string(raw), "canary") || strings.Contains(err.Error(), "canary") {
				t.Fatal("private provider payload leaked")
			}
		})
	}
}
