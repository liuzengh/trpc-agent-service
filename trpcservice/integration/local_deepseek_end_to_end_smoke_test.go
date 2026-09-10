package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// TestLocalGatewayWorkerDeepSeekSmoke is intentionally endpoint-driven: the
// target deployment must have its Gateway, Redis/PostgreSQL Worker and frozen
// DeepSeek model profile configured before this test is enabled.
func TestLocalGatewayWorkerDeepSeekSmoke(t *testing.T) {
	if os.Getenv("TRPC_LOCAL_DEEPSEEK_SMOKE") != "1" {
		t.Skip("requires explicit Gateway/Worker/DeepSeek smoke")
	}
	for _, name := range []string{"TRPC_LOCAL_GATEWAY_URL", "TRPC_LOCAL_GATEWAY_BEARER_TOKEN", "TRPC_LOCAL_DEEPSEEK_PROFILE_ID"} {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Fatalf("%s is required", name)
		}
	}
	base, err := url.Parse(strings.TrimSpace(os.Getenv("TRPC_LOCAL_GATEWAY_URL")))
	if err != nil || (base.Scheme != "https" && !(base.Scheme == "http" && os.Getenv("TRPC_LOCAL_ALLOW_INSECURE") == "1")) || base.Host == "" || base.User != nil {
		t.Fatal("TRPC_LOCAL_GATEWAY_URL must be an authorized absolute HTTP(S) endpoint")
	}
	random := make([]byte, 12)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	nonce := "local-deepseek-" + hex.EncodeToString(random)
	payload, err := json.Marshal(map[string]interface{}{"model": os.Getenv("TRPC_LOCAL_DEEPSEEK_PROFILE_ID"), "stream": false,
		"messages": []map[string]string{{"role": "user", "content": "Reply with exactly this token and nothing else: " + nonce}}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := *base
	endpoint.Path = strings.TrimRight(base.Path, "/") + "/v1/chat/completions"
	endpoint.RawQuery, endpoint.Fragment = "", ""
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+os.Getenv("TRPC_LOCAL_GATEWAY_BEARER_TOKEN"))
	request.Header.Set("Idempotency-Key", nonce)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("Gateway/Worker/DeepSeek request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Gateway returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var completion struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &completion); err != nil || len(completion.Choices) != 1 {
		t.Fatalf("invalid completion response: %v body=%s", err, body)
	}
	if got := strings.TrimSpace(completion.Choices[0].Message.Content); got != nonce {
		t.Fatalf("DeepSeek semantic sentinel mismatch: got %q want %q", got, nonce)
	}
	t.Logf("Gateway -> durable dispatch -> Worker -> DeepSeek -> terminal replay passed for profile %s", os.Getenv("TRPC_LOCAL_DEEPSEEK_PROFILE_ID"))
}
