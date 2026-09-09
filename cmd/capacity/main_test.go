package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteHealthAndGateway(t *testing.T) {
	var gatewayCalls atomic.Int32
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusNotFound
		switch request.URL.Path {
		case "/healthz":
			status = http.StatusOK
		case "/v1/gateway/messages":
			if request.Header.Get("Authorization") != "Bearer test-token" || request.Header.Get("X-Channel-Binding") != "binding" {
				status = http.StatusUnauthorized
				break
			}
			gatewayCalls.Add(1)
			status = http.StatusAccepted
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})
	for _, scenario := range []string{"health", "gateway"} {
		var output bytes.Buffer
		err := execute(context.Background(), options{BaseURL: "http://service.test", Scenario: scenario, Requests: 25, Concurrency: 4, Timeout: time.Second, Message: "test", MaxErrorRate: 0, Token: "test-token", Binding: "binding", RunID: "test", Output: &output, HTTPTransport: transport})
		if err != nil {
			t.Fatalf("scenario %s: %v", scenario, err)
		}
		var report summary
		if err := json.Unmarshal(output.Bytes(), &report); err != nil || report.Succeeded != 25 || report.Failed != 0 {
			t.Fatalf("scenario %s report=%+v err=%v", scenario, report, err)
		}
		if strings.Contains(output.String(), "test-token") {
			t.Fatal("credential leaked into report")
		}
	}
	if gatewayCalls.Load() != 25 {
		t.Fatalf("gateway calls=%d", gatewayCalls.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestExecuteRejectsUnsafeGatewayAndThresholds(t *testing.T) {
	var output bytes.Buffer
	base := options{BaseURL: "http://example.invalid", Scenario: "gateway", Requests: 1, Concurrency: 1, Timeout: time.Second, Output: &output}
	if err := execute(context.Background(), base); err == nil {
		t.Fatal("gateway without environment credentials accepted")
	}
	base.Scenario, base.MaxErrorRate = "health", 1.1
	if err := execute(context.Background(), base); err == nil {
		t.Fatal("invalid threshold accepted")
	}
	base.MaxErrorRate, base.Rate = 0, -1
	if err := execute(context.Background(), base); err == nil {
		t.Fatal("negative request rate accepted")
	}
}

func TestExecutePacesRequestStarts(t *testing.T) {
	var output bytes.Buffer
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})
	err := execute(context.Background(), options{
		BaseURL: "http://service.test", Scenario: "health", Requests: 3, Concurrency: 3,
		Timeout: time.Second, MaxErrorRate: 0, Rate: 100, Output: &output, HTTPTransport: transport,
	})
	if err != nil {
		t.Fatal(err)
	}
	var report summary
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.DurationMS < 15 {
		t.Fatalf("rate limit was not applied: duration_ms=%f", report.DurationMS)
	}
}

func TestPercentile(t *testing.T) {
	values := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond, 5 * time.Millisecond}
	if got := percentile(values, 0.95); got != 5*time.Millisecond {
		t.Fatalf("p95=%s", got)
	}
}
