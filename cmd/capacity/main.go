// Command capacity runs a bounded HTTP load probe and prints only aggregate
// latency/error statistics. Gateway credentials are read from environment so
// they do not appear in process arguments or reports.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	loadTokenEnv   = "TRPC_AGENT_LOAD_TOKEN"
	loadBindingEnv = "TRPC_AGENT_LOAD_BINDING"
)

type options struct {
	BaseURL       string
	Scenario      string
	Requests      int
	Concurrency   int
	Timeout       time.Duration
	Message       string
	MaxErrorRate  float64
	MaxP95        time.Duration
	Rate          float64
	Token         string
	Binding       string
	RunID         string
	Output        io.Writer
	HTTPTransport http.RoundTripper
}

type result struct {
	latency time.Duration
	status  int
	err     error
}

type summary struct {
	Scenario     string         `json:"scenario"`
	Requests     int            `json:"requests"`
	Succeeded    int            `json:"succeeded"`
	Failed       int            `json:"failed"`
	DurationMS   float64        `json:"duration_ms"`
	RPS          float64        `json:"rps"`
	P50MS        float64        `json:"p50_ms"`
	P95MS        float64        `json:"p95_ms"`
	P99MS        float64        `json:"p99_ms"`
	StatusCounts map[string]int `json:"status_counts"`
}

func main() {
	flags := flag.NewFlagSet("capacity", flag.ExitOnError)
	baseURL := flags.String("base-url", "http://127.0.0.1:8080", "service base URL")
	scenario := flags.String("scenario", "health", "health, readiness, or gateway")
	requests := flags.Int("requests", 1000, "total requests")
	concurrency := flags.Int("concurrency", 20, "parallel requests")
	timeout := flags.Duration("timeout", 10*time.Second, "per-request timeout")
	message := flags.String("message", "capacity probe", "gateway message (may invoke the configured model)")
	runID := flags.String("run-id", "", "optional stable run identifier for correlating accepted requests")
	maxErrorRate := flags.Float64("max-error-rate", 0.01, "failure threshold from 0 to 1")
	maxP95 := flags.Duration("max-p95", 0, "optional p95 failure threshold")
	rate := flags.Float64("rate", 0, "optional maximum request starts per second; zero is unbounded")
	_ = flags.Parse(os.Args[1:])
	if err := execute(context.Background(), options{
		BaseURL: *baseURL, Scenario: *scenario, Requests: *requests, Concurrency: *concurrency,
		Timeout: *timeout, Message: *message, MaxErrorRate: *maxErrorRate, MaxP95: *maxP95, Rate: *rate,
		Token: os.Getenv(loadTokenEnv), Binding: os.Getenv(loadBindingEnv),
		RunID: strings.TrimSpace(*runID), Output: os.Stdout,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(parent context.Context, config options) error {
	if parent == nil || config.Output == nil {
		return errors.New("capacity: context and output are required")
	}
	config.BaseURL = strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	if config.BaseURL == "" || config.Requests <= 0 || config.Requests > 1_000_000 || config.Concurrency <= 0 || config.Concurrency > 4096 || config.Timeout <= 0 {
		return errors.New("capacity: invalid URL, requests, concurrency, or timeout")
	}
	if config.MaxErrorRate < 0 || config.MaxErrorRate > 1 || config.MaxP95 < 0 || config.Rate < 0 || config.Rate > 1_000_000 {
		return errors.New("capacity: thresholds are invalid")
	}
	switch config.Scenario {
	case "health", "readiness":
	case "gateway":
		if strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.Binding) == "" {
			return fmt.Errorf("capacity: gateway requires %s and %s", loadTokenEnv, loadBindingEnv)
		}
	default:
		return errors.New("capacity: scenario must be health, readiness, or gateway")
	}
	if config.RunID == "" {
		config.RunID = time.Now().UTC().Format("20060102T150405.000000000")
	}
	transport := config.HTTPTransport
	if transport == nil {
		transport = &http.Transport{MaxIdleConns: config.Concurrency * 2, MaxIdleConnsPerHost: config.Concurrency}
	}
	client := &http.Client{Transport: transport}
	defer client.CloseIdleConnections()
	jobs := make(chan int)
	results := make(chan result, config.Concurrency)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var workers sync.WaitGroup
	for workerID := 0; workerID < config.Concurrency; workerID++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				results <- runRequest(ctx, client, config, index)
			}
		}()
	}
	started := time.Now()
	go func() {
		defer close(jobs)
		for index := 0; index < config.Requests; index++ {
			if index > 0 && config.Rate > 0 {
				delay := time.Duration(float64(time.Second) / config.Rate)
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				}
			}
			select {
			case jobs <- index:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { workers.Wait(); close(results) }()
	latencies := make([]time.Duration, 0, config.Requests)
	statusCounts := make(map[string]int)
	succeeded, failed := 0, 0
	for current := range results {
		latencies = append(latencies, current.latency)
		key := "transport_error"
		if current.status != 0 {
			key = fmt.Sprintf("http_%d", current.status)
		}
		statusCounts[key]++
		if current.err == nil && expectedStatus(config.Scenario, current.status) {
			succeeded++
		} else {
			failed++
		}
	}
	elapsed := time.Since(started)
	rps := 0.0
	if elapsed > 0 {
		rps = float64(len(latencies)) / elapsed.Seconds()
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	report := summary{
		Scenario: config.Scenario, Requests: len(latencies), Succeeded: succeeded, Failed: failed,
		DurationMS: milliseconds(elapsed), RPS: rps,
		P50MS: milliseconds(percentile(latencies, 0.50)), P95MS: milliseconds(percentile(latencies, 0.95)),
		P99MS: milliseconds(percentile(latencies, 0.99)), StatusCounts: statusCounts,
	}
	encoder := json.NewEncoder(config.Output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return err
	}
	if report.Requests != config.Requests {
		return errors.New("capacity: run was interrupted")
	}
	if float64(failed)/float64(config.Requests) > config.MaxErrorRate {
		return errors.New("capacity: error rate exceeded threshold")
	}
	if config.MaxP95 > 0 && percentile(latencies, 0.95) > config.MaxP95 {
		return errors.New("capacity: p95 exceeded threshold")
	}
	return nil
}

func runRequest(parent context.Context, client *http.Client, config options, index int) result {
	ctx, cancel := context.WithTimeout(parent, config.Timeout)
	defer cancel()
	method, target := http.MethodGet, config.BaseURL+"/healthz"
	var body io.Reader
	if config.Scenario == "readiness" {
		target = config.BaseURL + "/readyz"
	}
	if config.Scenario == "gateway" {
		method, target = http.MethodPost, config.BaseURL+"/v1/gateway/messages"
		payload, _ := json.Marshal(map[string]string{
			"channel": "http", "from": fmt.Sprintf("load-user-%d", index%100),
			"message_id": fmt.Sprintf("load-%s-%d", config.RunID, index), "text": config.Message,
		})
		body = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return result{err: errors.New("request creation failed")}
	}
	if config.Scenario == "gateway" {
		request.Header.Set("Authorization", "Bearer "+config.Token)
		request.Header.Set("X-Channel-Binding", config.Binding)
		request.Header.Set("Content-Type", "application/json")
	}
	started := time.Now()
	response, err := client.Do(request)
	latency := time.Since(started)
	if err != nil {
		return result{latency: latency, err: errors.New("request failed")}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	return result{latency: latency, status: response.StatusCode}
}

func expectedStatus(scenario string, status int) bool {
	if scenario == "gateway" {
		return status == http.StatusAccepted
	}
	return status == http.StatusOK
}

func percentile(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1)*quantile + 0.5)
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func milliseconds(value time.Duration) float64 { return float64(value.Microseconds()) / 1000 }
