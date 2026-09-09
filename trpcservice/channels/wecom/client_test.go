package wecom_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

type tokenSource struct {
	mu          sync.Mutex
	tokens      []string
	invalidated []string
}

type limiterCounter struct {
	mu    sync.Mutex
	calls int
}

func (limiter *limiterCounter) Wait(context.Context, gateway.OutboundMessage) error {
	limiter.mu.Lock()
	limiter.calls++
	limiter.mu.Unlock()
	return nil
}

func (source *tokenSource) Token(context.Context) (string, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.tokens) == 0 {
		return "", errors.New("no token")
	}
	return source.tokens[0], nil
}

func (source *tokenSource) Invalidate(token string) {
	source.mu.Lock()
	source.invalidated = append(source.invalidated, token)
	if len(source.tokens) > 1 {
		source.tokens = source.tokens[1:]
	}
	source.mu.Unlock()
}

func TestSenderSplitsUTF8AndRefreshesRejectedToken(t *testing.T) {
	var mu sync.Mutex
	var contents []string
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Query().Get("access_token") == "stale" {
			return jsonResponse(http.StatusOK, `{"errcode":42001}`), nil
		}
		if request.URL.Path != "/cgi-bin/message/send" {
			t.Errorf("path=%q", request.URL.Path)
		}
		var body struct {
			ToUser  string `json:"touser"`
			AgentID int64  `json:"agentid"`
			Text    struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.ToUser != "alice" || body.AgentID != 1000002 {
			t.Errorf("body=%+v", body)
		}
		mu.Lock()
		contents = append(contents, body.Text.Content)
		mu.Unlock()
		return jsonResponse(http.StatusOK, `{"errcode":0}`), nil
	})}
	tokens := &tokenSource{tokens: []string{"stale", "fresh"}}
	limiter := &limiterCounter{}
	sender := &wecom.Sender{AgentID: 1000002, Tokens: tokens, BaseURL: "https://wecom.invalid", Client: client, MaxBytes: 7, Limiter: limiter}
	if err := sender.SendText(context.Background(), gateway.OutboundMessage{ExternalUserID: "alice", Text: "你好abc世界"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(contents, "") != "你好abc世界" || len(contents) < 2 {
		t.Fatalf("chunks=%q", contents)
	}
	for _, content := range contents {
		if len(content) > 7 {
			t.Fatalf("oversized chunk %q (%d bytes)", content, len(content))
		}
	}
	if len(tokens.invalidated) != 1 || tokens.invalidated[0] != "stale" {
		t.Fatalf("invalidated=%v", tokens.invalidated)
	}
	if limiter.calls != len(contents)+1 {
		t.Fatalf("limiter calls=%d chunks=%d (one token refresh retry)", limiter.calls, len(contents))
	}
}

func TestSenderUsesGroupEndpointAndClassifiesRateLimit(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/cgi-bin/appchat/send" {
			t.Errorf("path=%q", request.URL.Path)
		}
		return jsonResponse(http.StatusOK, `{"errcode":45009}`), nil
	})}
	sender := &wecom.Sender{AgentID: 1000002, Tokens: &tokenSource{tokens: []string{"token"}}, BaseURL: "https://wecom.invalid", Client: client}
	err := sender.SendText(context.Background(), gateway.OutboundMessage{ExternalUserID: "alice", ConversationID: "chat-1", Text: "hello"})
	var apiErr *wecom.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != 45009 || !apiErr.Retryable {
		t.Fatalf("error=%v", err)
	}
}

func TestCredentialTokenSourceDoesNotLeakSecret(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusInternalServerError, "no"), nil
	})}
	source := &wecom.CredentialTokenSource{CorpID: "corp", CorpSecret: "super-secret", BaseURL: "https://wecom.invalid", Client: client}
	_, err := source.Token(context.Background())
	if err == nil || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("error=%v", err)
	}
}

func TestSenderMarksFailureAfterFirstChunkUncertain(t *testing.T) {
	var calls int
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return jsonResponse(http.StatusOK, `{"errcode":0}`), nil
		}
		return jsonResponse(http.StatusOK, `{"errcode":45009}`), nil
	})}
	sender := &wecom.Sender{AgentID: 1000002, Tokens: &tokenSource{tokens: []string{"token"}}, BaseURL: "https://wecom.invalid", Client: client, MaxBytes: 3}
	err := sender.SendText(context.Background(), gateway.OutboundMessage{ExternalUserID: "alice", Text: "abcdef"})
	var uncertain interface{ DeliveryOutcomeUncertain() bool }
	if !errors.As(err, &uncertain) || !uncertain.DeliveryOutcomeUncertain() {
		t.Fatalf("error=%v", err)
	}
}
