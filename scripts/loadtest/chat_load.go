// Command chat_load is a small, dependency-free load generator for the admin
// chat pipeline: it drives C concurrent sessions against POST /chat, each
// sending N sequential messages, and prints throughput + latency percentiles.
//
// Usage (from repo root):
//
//	go run ./scripts/loadtest/chat_load.go \
//	  -tenant t-demo -agent a-demo -concurrency 8 -each 5 -text "hi"
//
// Every goroutine owns one session id (session-i) so sessions stay isolated,
// mirroring N independent IM conversations hitting one worker fleet.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type reqBody struct {
	TenantID string `json:"tenant_id"`
	AgentID  string `json:"agent_id"`
	SessionID string `json:"session_id"`
	Text     string `json:"text"`
}

type result struct {
	code    int
	latency time.Duration
}

func main() {
	var (
		baseURL     = flag.String("url", "http://127.0.0.1:8080", "admin API base url")
		tenantID    = flag.String("tenant", "", "tenant id (required)")
		agentID     = flag.String("agent", "", "agent id (required)")
		concurrency = flag.Int("concurrency", 8, "number of concurrent sessions")
		each        = flag.Int("each", 5, "sequential messages per session")
		text        = flag.String("text", "hi", "message text")
	)
	flag.Parse()
	if *tenantID == "" || *agentID == "" {
		fmt.Fprintln(os.Stderr, "tenant and agent are required")
		flag.Usage()
		os.Exit(2)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	var (
		mu       sync.Mutex
		results  []result
		wg       sync.WaitGroup
		start    = time.Now()
		failures = 0
	)
	rate := make(chan struct{}, *concurrency)
	sem := make(chan struct{}, *concurrency) // bound in-flight requests too

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sessionID := fmt.Sprintf("load-%d", i)
			for j := 0; j < *each; j++ {
				rate <- struct{}{}
				sem <- struct{}{}
				body, _ := json.Marshal(reqBody{
					TenantID: *tenantID, AgentID: *agentID,
					SessionID: sessionID, Text: *text,
				})
				begin := time.Now()
				resp, err := client.Post(*baseURL+"/chat", "application/json", bytes.NewReader(body))
				var code int
				if err != nil {
					code = 0
				} else {
					code = resp.StatusCode
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				mu.Lock()
				if err != nil || code != http.StatusAccepted {
					failures++
				}
				results = append(results, result{code: code, latency: time.Since(begin)})
				mu.Unlock()
				<-sem
				<-rate
			}
		}(i)
	}
	wg.Wait()
	total := time.Since(start)
	n := len(results)
	fmt.Printf("total=%d ok=%d fail=%d elapsed=%s\n", n, n-failures, failures, total.Round(time.Millisecond))
	if n > 0 {
		fmt.Printf("throughput(accepted/s)=%.1f\n", float64(n-failures)/total.Seconds())
	}
	sort.Slice(results, func(a, b int) bool { return results[a].latency < results[b].latency })
	for _, p := range []float64{50, 95, 99} {
		if n > 0 {
			idx := int(float64(n-1) * p / 100)
			fmt.Printf("p%02.0f=%s\n", p, results[idx].latency.Round(time.Millisecond))
		}
	}
	if n > 0 {
		fmt.Printf("max=%s\n", results[n-1].latency.Round(time.Millisecond))
	}
	// 4xx/5xx breakdown
	byCode := map[int]int{}
	for _, r := range results {
		byCode[r.code]++
	}
	for code, c := range byCode {
		fmt.Printf("http_%d=%d\n", code, c)
	}
}
