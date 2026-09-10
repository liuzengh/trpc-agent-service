// local-webui-load sends a bounded number of signed WebUI callbacks and waits
// for their durable replies. It is deliberately a low-volume local acceptance
// client, not a production load-testing framework.
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
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui"
)

const defaultToken = "local-webui-token-change-me"

type config struct {
	baseURL     string
	routeKey    string
	accountID   string
	token       string
	requests    int
	concurrency int
	timeout     time.Duration
	prompt      string
}

type result struct {
	Index         int     `json:"index"`
	LatencyMillis float64 `json:"latency_millis"`
	Error         string  `json:"error,omitempty"`
}

type summary struct {
	StartedAt     time.Time `json:"started_at"`
	EndedAt       time.Time `json:"ended_at"`
	Offered       int       `json:"offered_requests"`
	Completed     int       `json:"completed_requests"`
	Failed        int       `json:"failed_requests"`
	P95Millis     float64   `json:"p95_latency_millis"`
	SustainedRPS  float64   `json:"sustained_rps"`
	ResultDetails []result  `json:"results"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	value := config{}
	flags := flag.NewFlagSet("local-webui-load", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&value.baseURL, "base-url", "http://127.0.0.1:58086", "WebUI runtime URL")
	flags.StringVar(&value.routeKey, "route-key", "local-webui", "WebUI route key")
	flags.StringVar(&value.accountID, "account-id", "local-webui", "WebUI external account")
	flags.StringVar(&value.token, "token", defaultToken, "WebUI local verification token")
	flags.IntVar(&value.requests, "requests", 4, "number of requests")
	flags.IntVar(&value.concurrency, "concurrency", 2, "maximum concurrent requests")
	flags.DurationVar(&value.timeout, "timeout", 2*time.Minute, "per-request terminal timeout")
	flags.StringVar(&value.prompt, "prompt", "Reply with exactly: capacity-ok.", "short model prompt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := validate(value); err != nil {
		return err
	}

	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), value.timeout+30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	results := make([]result, value.requests)
	jobs := make(chan int)
	var workers sync.WaitGroup
	for worker := 0; worker < value.concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				begin := time.Now()
				err := sendAndAwait(ctx, client, value, index)
				elapsed := time.Since(begin)
				results[index] = result{Index: index + 1, LatencyMillis: float64(elapsed) / float64(time.Millisecond)}
				if err != nil {
					results[index].Error = err.Error()
				}
			}
		}()
	}
	for index := range results {
		jobs <- index
	}
	close(jobs)
	workers.Wait()

	ended := time.Now().UTC()
	output := summary{StartedAt: started, EndedAt: ended, Offered: value.requests, ResultDetails: results}
	latencies := make([]time.Duration, 0, len(results))
	for _, item := range results {
		if item.Error == "" {
			output.Completed++
			latencies = append(latencies, time.Duration(item.LatencyMillis*float64(time.Millisecond)))
		} else {
			output.Failed++
		}
	}
	if elapsed := ended.Sub(started).Seconds(); elapsed > 0 {
		output.SustainedRPS = float64(output.Completed) / elapsed
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
		position := (95*len(latencies) + 99) / 100
		output.P95Millis = float64(latencies[position-1]) / float64(time.Millisecond)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		return err
	}
	if output.Failed > 0 {
		return fmt.Errorf("local WebUI load failed: completed=%d failed=%d", output.Completed, output.Failed)
	}
	return nil
}

func validate(value config) error {
	if _, err := url.ParseRequestURI(value.baseURL); err != nil || !strings.HasPrefix(value.baseURL, "http") || value.routeKey == "" || value.accountID == "" || len(value.token) < 16 ||
		value.requests < 1 || value.concurrency < 1 || value.concurrency > value.requests || value.timeout <= 0 || strings.TrimSpace(value.prompt) == "" {
		return errors.New("invalid local WebUI load configuration")
	}
	return nil
}

func sendAndAwait(ctx context.Context, client *http.Client, value config, index int) error {
	userID := fmt.Sprintf("capacity-user-%d", index+1)
	chatID := fmt.Sprintf("capacity-chat-%d", index+1)
	messageID, err := nonce()
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"schema_version":      1,
		"external_account_id": value.accountID,
		"external_message_id": messageID,
		"external_user_id":    userID,
		"external_chat_id":    chatID,
		"conversation_type":   "p2p",
		"message_type":        "text",
		"text":                value.prompt,
		"occurred_at":         time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	postURI := "/webui/api/messages?" + url.Values{"route_key": []string{value.routeKey}}.Encode()
	if err := signedRequest(ctx, client, http.MethodPost, value.baseURL+postURI, value.token, body, body); err != nil {
		return err
	}
	deadline := time.NewTimer(value.timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("terminal reply timeout")
		case <-ticker.C:
			query := url.Values{"route_key": []string{value.routeKey}, "external_user_id": []string{userID}, "external_chat_id": []string{chatID}}
			uri := "/webui/api/replies?" + query.Encode()
			response, queryErr := signedQuery(ctx, client, value.baseURL+uri, value.token)
			if queryErr != nil {
				return queryErr
			}
			if len(response.Messages) > 0 {
				return nil
			}
		}
	}
}

func signedRequest(ctx context.Context, client *http.Client, method, rawURL, token string, body, payload []byte) error {
	request, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if err := signRequest(request, token, payload); err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("callback status=%d", response.StatusCode)
	}
	return nil
}

type replies struct {
	Messages []json.RawMessage `json:"messages"`
}

func signedQuery(ctx context.Context, client *http.Client, rawURL, token string) (replies, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return replies{}, err
	}
	if err := signRequest(request, token, []byte(request.Method+"\n"+request.URL.RequestURI())); err != nil {
		return replies{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return replies{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return replies{}, fmt.Errorf("reply query status=%d", response.StatusCode)
	}
	var value replies
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		return replies{}, err
	}
	return value, nil
}

func signRequest(request *http.Request, token string, payload []byte) error {
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	nonceValue, err := nonce()
	if err != nil {
		return err
	}
	request.Header.Set("X-WebUI-Timestamp", timestamp)
	request.Header.Set("X-WebUI-Nonce", nonceValue)
	request.Header.Set("X-WebUI-Signature", webui.SignCallback(token, timestamp, nonceValue, payload))
	return nil
}

func nonce() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
