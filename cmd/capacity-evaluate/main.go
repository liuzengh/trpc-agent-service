package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultCapacityPayload = `{"model":"capacity","messages":[{"role":"user","content":"capacity"}]}`

type options struct {
	endpoint      string
	concurrency   int
	requests      int
	duration      time.Duration
	apiKey        string
	requireAPIKey bool
	maxErrorRate  float64
	payload       string
	timeout       time.Duration
	sessionPrefix string
}

type report struct {
	Endpoint        string  `json:"endpoint"`
	Concurrency     int     `json:"concurrency"`
	Configured      int     `json:"configured_requests"`
	DurationSeconds float64 `json:"duration_seconds"`
	TotalRequests   int64   `json:"total_requests"`
	Success         int64   `json:"success"`
	Failure         int64   `json:"failure"`
	ErrorRate       float64 `json:"error_rate"`
	MaxErrorRate    float64 `json:"max_error_rate"`
	SuccessCriteria bool    `json:"success_criteria_passed"`
	Throughput      float64 `json:"throughput_requests_per_second"`
	MinLatencyMS    float64 `json:"latency_min_ms"`
	P50LatencyMS    float64 `json:"latency_p50_ms"`
	P95LatencyMS    float64 `json:"latency_p95_ms"`
	P99LatencyMS    float64 `json:"latency_p99_ms"`
	MaxLatencyMS    float64 `json:"latency_max_ms"`
}

func main() {
	options, err := parseOptions(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	result, err := measure(context.Background(), options, http.DefaultClient)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !result.SuccessCriteria {
		fmt.Fprintf(os.Stderr, "capacity success criteria failed: total_requests=%d error_rate=%.4f max_error_rate=%.4f\n", result.TotalRequests, result.ErrorRate, result.MaxErrorRate)
		os.Exit(1)
	}
}

func parseOptions(args []string, getenv func(string) string) (options, error) {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	values := options{}
	flags := flag.NewFlagSet("capacity-evaluate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&values.endpoint, "endpoint", "http://127.0.0.1:8080/v1/chat/completions", "full Gateway endpoint")
	flags.IntVar(&values.concurrency, "concurrency", 1, "number of concurrent requests")
	flags.IntVar(&values.requests, "requests", 100, "number of requests; use 0 with duration for an open run")
	flags.DurationVar(&values.duration, "duration", 0, "optional run duration, for example 2m")
	flags.StringVar(&values.apiKey, "api-key", getenv("TRPC_AGENT_SERVICE_CAPACITY_API_KEY"), "Bearer API key")
	flags.BoolVar(&values.requireAPIKey, "require-api-key", false, "fail when the API key is empty")
	flags.Float64Var(&values.maxErrorRate, "max-error-rate", 0.05, "maximum allowed fraction of failed requests")
	flags.StringVar(&values.payload, "payload", defaultCapacityPayload, "JSON request payload")
	flags.DurationVar(&values.timeout, "timeout", 2*time.Minute, "per-request timeout")
	flags.StringVar(&values.sessionPrefix, "session-prefix", "capacity-session", "prefix for generated session IDs")
	payloadFile := ""
	flags.StringVar(&payloadFile, "payload-file", "", "read JSON request payload from this file")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if strings.TrimSpace(payloadFile) != "" {
		encoded, err := os.ReadFile(payloadFile)
		if err != nil {
			return options{}, fmt.Errorf("read payload file: %w", err)
		}
		values.payload = string(encoded)
	}
	if err := validateOptions(values); err != nil {
		return options{}, err
	}
	return values, nil
}

func validateOptions(values options) error {
	parsed, err := url.Parse(strings.TrimSpace(values.endpoint))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return errors.New("endpoint must be an absolute http or https URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("endpoint must not contain credentials, query parameters, or fragments")
	}
	if values.concurrency <= 0 {
		return errors.New("concurrency must be positive")
	}
	if values.requests < 0 {
		return errors.New("requests must be non-negative")
	}
	if values.duration < 0 {
		return errors.New("duration must be non-negative")
	}
	if values.requests == 0 && values.duration <= 0 {
		return errors.New("requests or duration is required")
	}
	if values.timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if math.IsNaN(values.maxErrorRate) || values.maxErrorRate < 0 || values.maxErrorRate > 1 {
		return errors.New("max-error-rate must be between 0 and 1")
	}
	if values.requireAPIKey && strings.TrimSpace(values.apiKey) == "" {
		return errors.New("api-key is required")
	}
	if strings.TrimSpace(values.sessionPrefix) == "" {
		return errors.New("session-prefix is required")
	}
	if !json.Valid([]byte(values.payload)) {
		return errors.New("payload must be valid JSON")
	}
	return nil
}

func measure(parent context.Context, values options, client *http.Client) (report, error) {
	if parent == nil {
		parent = context.Background()
	}
	if client == nil {
		return report{}, errors.New("http client is required")
	}
	if err := validateOptions(values); err != nil {
		return report{}, err
	}
	safeClient := *client
	safeClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if values.duration > 0 {
		timer := time.AfterFunc(values.duration, cancel)
		defer timer.Stop()
	}

	started := time.Now()
	var next atomic.Int64
	var success atomic.Int64
	var failure atomic.Int64
	var latenciesMu sync.Mutex
	latencies := make([]time.Duration, 0)
	var wg sync.WaitGroup
	for worker := 0; worker < values.concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				sequence := next.Add(1)
				if values.requests > 0 && sequence > int64(values.requests) {
					return
				}
				if err := ctx.Err(); err != nil {
					return
				}
				requestID, err := randomID()
				if err != nil {
					failure.Add(1)
					continue
				}
				requestCtx, requestCancel := context.WithTimeout(ctx, values.timeout)
				request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, values.endpoint, bytes.NewReader([]byte(values.payload)))
				if err == nil {
					request.Header.Set("Content-Type", "application/json")
					if values.apiKey != "" {
						request.Header.Set("Authorization", "Bearer "+values.apiKey)
					}
					request.Header.Set("X-Request-ID", requestID)
					request.Header.Set("Idempotency-Key", requestID)
					request.Header.Set("X-Session-ID", fmt.Sprintf("%s-%d", values.sessionPrefix, worker))
				}
				requestStarted := time.Now()
				var response *http.Response
				requestErr := err
				if requestErr == nil {
					response, requestErr = safeClient.Do(request)
				}
				latency := time.Since(requestStarted)
				if response != nil {
					_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
					_ = response.Body.Close()
				}
				requestCancel()
				latenciesMu.Lock()
				latencies = append(latencies, latency)
				latenciesMu.Unlock()
				if requestErr == nil && response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
					success.Add(1)
				} else {
					failure.Add(1)
				}
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(started)
	latenciesMu.Lock()
	valuesCopy := append([]time.Duration(nil), latencies...)
	latenciesMu.Unlock()
	return buildReport(values, elapsed, success.Load(), failure.Load(), valuesCopy), nil
}

func buildReport(values options, elapsed time.Duration, success, failure int64, latencies []time.Duration) report {
	total := success + failure
	result := report{
		Endpoint:        values.endpoint,
		Concurrency:     values.concurrency,
		Configured:      values.requests,
		DurationSeconds: elapsed.Seconds(),
		TotalRequests:   total,
		Success:         success,
		Failure:         failure,
		MaxErrorRate:    values.maxErrorRate,
	}
	if total > 0 {
		result.ErrorRate = float64(failure) / float64(total)
	}
	result.SuccessCriteria = total > 0 && result.ErrorRate <= values.maxErrorRate
	if elapsed > 0 {
		result.Throughput = float64(total) / elapsed.Seconds()
	}
	if len(latencies) == 0 {
		return result
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	result.MinLatencyMS = latencyMilliseconds(latencies[0])
	result.P50LatencyMS = latencyMilliseconds(percentile(latencies, 0.50))
	result.P95LatencyMS = latencyMilliseconds(percentile(latencies, 0.95))
	result.P99LatencyMS = latencyMilliseconds(percentile(latencies, 0.99))
	result.MaxLatencyMS = latencyMilliseconds(latencies[len(latencies)-1])
	return result
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	position := p * float64(len(values)-1)
	lower := int(position)
	upper := lower + 1
	if upper >= len(values) {
		return values[lower]
	}
	weight := position - float64(lower)
	return time.Duration(float64(values[lower])*(1-weight) + float64(values[upper])*weight)
}

func latencyMilliseconds(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
