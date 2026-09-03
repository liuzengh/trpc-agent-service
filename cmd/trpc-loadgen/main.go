package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type requestBody struct {
	BindingKey string `json:"binding_key"`
	MessageID  string `json:"message_id"`
	UserID     string `json:"user_id"`
	SessionID  string `json:"session_id"`
	ChatType   string `json:"chat_type"`
	Message    string `json:"message"`
}

func main() {
	endpoint := flag.String("url", "http://127.0.0.1:8080/inbound", "inbound endpoint")
	binding := flag.String("binding", "tutorial-http", "Channel Binding callback key")
	requests := flag.Int("requests", 1000, "total requests")
	concurrency := flag.Int("concurrency", 50, "parallel workers")
	sessions := flag.Int("sessions", 100, "session cardinality")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	messagePrefix := flag.String("message-prefix", "", "external message ID prefix")
	flag.Parse()
	if *requests <= 0 || *concurrency <= 0 || *sessions <= 0 {
		fmt.Fprintln(os.Stderr, "requests, concurrency and sessions must be positive")
		os.Exit(2)
	}

	client := &http.Client{Timeout: *timeout}
	work := make(chan int)
	latencies := make([]time.Duration, 0, *requests)
	var latencyMu sync.Mutex
	var succeeded atomic.Int64
	var failed atomic.Int64
	started := time.Now()
	prefix := *messagePrefix
	if prefix == "" {
		prefix = fmt.Sprintf("load-%d", started.UnixNano())
	}
	var group sync.WaitGroup
	for range *concurrency {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range work {
				body, _ := json.Marshal(requestBody{
					BindingKey: *binding,
					MessageID:  fmt.Sprintf("%s-%d", prefix, index),
					UserID:     fmt.Sprintf("load-user-%d", index%*sessions),
					SessionID:  fmt.Sprintf("load-session-%d", index%*sessions),
					ChatType:   "direct",
					Message:    "capacity test",
				})
				ctx, cancel := context.WithTimeout(context.Background(), *timeout)
				request, _ := http.NewRequestWithContext(ctx, http.MethodPost, *endpoint, bytes.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				requestStarted := time.Now()
				response, err := client.Do(request)
				latency := time.Since(requestStarted)
				cancel()
				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()
				if err != nil {
					failed.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode >= 200 && response.StatusCode < 300 {
					succeeded.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	for index := 0; index < *requests; index++ {
		work <- index
	}
	close(work)
	group.Wait()
	elapsed := time.Since(started)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("prefix=%s requests=%d success=%d failed=%d elapsed=%s throughput=%.2f req/s\n",
		prefix, *requests, succeeded.Load(), failed.Load(), elapsed,
		float64(*requests)/elapsed.Seconds())
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n",
		percentile(latencies, 0.50), percentile(latencies, 0.95),
		percentile(latencies, 0.99), percentile(latencies, 1))
}

func percentile(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * quantile)
	return values[index]
}
