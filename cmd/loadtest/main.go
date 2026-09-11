// Command loadtest drives the admin chat API to measure platform capacity.
//
// It exists because the numbers in the capacity section have to be measured on
// this deployment rather than estimated: two modes cover the two different
// questions an operator actually asks.
//
//	publish  — POST /chat only. This is the IM-callback-shaped question: how
//	           many inbound messages per second can the ingress accept before
//	           it becomes the bottleneck? The endpoint returns as soon as the
//	           message is on the stream, so this measures the publish path
//	           (HTTP + rate limit + XADD), not the model.
//
//	turn     — POST /chat, then poll the conversation ledger until the
//	           assistant row for that turn appears. This is the user-facing
//	           question: how long does a full turn take, and how many turns per
//	           second does one node sustain with a real model behind it?
//
// Both modes print a latency distribution (p50/p90/p99, max) and the achieved
// throughput. Nothing is written to a file: the harness is a measuring
// instrument, and the numbers belong in the runbook next to the date and the
// conditions they were taken under.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	base := flag.String("base", "http://127.0.0.1:8080", "platform base URL")
	user := flag.String("user", "admin", "login user id")
	pass := flag.String("pass", "admin123", "login password")
	tenantID := flag.String("tenant", "t-demo", "tenant id")
	agentID := flag.String("agent", "", "agent id (required)")
	mode := flag.String("mode", "publish", "publish | turn")
	n := flag.Int("n", 32, "total messages")
	conc := flag.Int("c", 8, "concurrency")
	timeout := flag.Duration("timeout", 6*time.Minute, "per-turn wait budget in turn mode")
	poll := flag.Duration("poll", 500*time.Millisecond, "ledger poll interval in turn mode")
	flag.Parse()

	if *agentID == "" {
		fmt.Fprintln(os.Stderr, "loadtest: -agent is required")
		os.Exit(2)
	}
	token, err := login(*base, *user, *pass)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadtest: login: %v\n", err)
		os.Exit(1)
	}

	lt := &runner{
		base: *base, token: token, tenant: *tenantID, agent: *agentID,
		client:  &http.Client{Timeout: 30 * time.Second},
		mode:    *mode,
		timeout: *timeout,
		poll:    *poll,
	}

	fmt.Printf("mode=%s n=%d concurrency=%d tenant=%s agent=%s\n", *mode, *n, *conc, *tenantID, *agentID)
	start := time.Now()
	lt.run(*n, *conc)
	elapsed := time.Since(start)

	lt.report(elapsed)
}

type runner struct {
	base, token, tenant, agent string
	client                     *http.Client
	mode                       string
	timeout, poll              time.Duration

	mu        sync.Mutex
	latencies []time.Duration
	ok        int64
	failed    int64
	timeouts  int64
}

func (r *runner) run(n, conc int) {
	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				r.one()
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

// one runs a single measurement; the caller only ever sees the aggregate.
func (r *runner) one() {
	// A per-iteration session keeps the measurements independent: sharing one
	// session would serialize them behind the platform's session lock and
	// measure the lock instead of the pipeline.
	session := fmt.Sprintf("lt-%d-%d", time.Now().UnixNano(), rand.Int63())

	body, _ := json.Marshal(map[string]string{
		"tenant_id": r.tenant, "agent_id": r.agent, "session_id": session,
		"text": "ping",
	})

	started := time.Now()
	req, _ := http.NewRequest(http.MethodPost, r.base+"/chat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.client.Do(req)
	if err != nil {
		atomic.AddInt64(&r.failed, 1)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		atomic.AddInt64(&r.failed, 1)
		return
	}

	if r.mode == "turn" {
		if !r.waitForReply(session, started) {
			return
		}
	}
	r.record(time.Since(started))
	atomic.AddInt64(&r.ok, 1)
}

// waitForReply polls the conversation ledger until the assistant row of this
// turn is visible. The ledger is written after the reply is durable, so its
// appearance is a real end-to-end signal rather than a hint.
func (r *runner) waitForReply(session string, started time.Time) bool {
	deadline := started.Add(r.timeout)
	for time.Now().Before(deadline) {
		if r.hasAssistant(session) {
			return true
		}
		time.Sleep(r.poll)
	}
	atomic.AddInt64(&r.timeouts, 1)
	atomic.AddInt64(&r.failed, 1)
	return false
}

func (r *runner) hasAssistant(session string) bool {
	req, _ := http.NewRequest(http.MethodGet, r.base+"/sessions/"+session+"/messages", nil)
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var rows []struct {
		Role string `json:"role"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return false
	}
	for _, row := range rows {
		if row.Role == "ASSISTANT" {
			return true
		}
	}
	return false
}

func (r *runner) record(d time.Duration) {
	r.mu.Lock()
	r.latencies = append(r.latencies, d)
	r.mu.Unlock()
}

// report prints the distribution. Percentiles come from the sorted sample, so
// the numbers are the ones that were actually observed — no interpolation that
// could be quoted as if more was measured than happened.
func (r *runner) report(elapsed time.Duration) {
	r.mu.Lock()
	lat := append([]time.Duration(nil), r.latencies...)
	r.mu.Unlock()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	total := r.ok + r.failed
	fmt.Printf("\nresults: ok=%d failed=%d (timeouts=%d) in %s\n", r.ok, r.failed, r.timeouts, elapsed.Round(time.Millisecond))
	if total > 0 {
		fmt.Printf("achieved throughput: %.2f msg/s\n", float64(r.ok)/elapsed.Seconds())
	}
	if len(lat) == 0 {
		fmt.Println("no successful samples")
		return
	}
	pct := func(p float64) time.Duration {
		idx := int(p * float64(len(lat)-1))
		return lat[idx]
	}
	fmt.Printf("latency (n=%d): p50=%s p90=%s p99=%s max=%s\n",
		len(lat),
		pct(0.50).Round(time.Millisecond), pct(0.90).Round(time.Millisecond),
		pct(0.99).Round(time.Millisecond), lat[len(lat)-1].Round(time.Millisecond))
}

func login(base, user, pass string) (string, error) {
	body, _ := json.Marshal(map[string]string{"user_id": user, "password": pass})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("empty token")
	}
	return out.Token, nil
}
