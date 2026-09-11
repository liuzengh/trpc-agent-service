package wecommcp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDiscoveryNeverCallsBusinessToolsOrSubscribes(t *testing.T) {
	var mu sync.Mutex
	methods := map[string]int{}
	const key = "credential-canary-do-not-print"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected subscription/cleanup method %s", r.Method)
			w.WriteHeader(400)
			return
		}
		var request struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Cursor string `json:"cursor"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		methods[request.Method]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]string{"name": "test-server", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}}
		case "notifications/initialized":
			w.WriteHeader(202)
			return
		case "tools/list":
			name, next := "receive_message", "next"
			if request.Params.Cursor != "" {
				name, next = "send_message", ""
			}
			result = map[string]any{"tools": []any{map[string]any{"name": name, "description": "description " + key, "inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}}}, "nextCursor": next}
		default:
			t.Errorf("business method reached server: %s", request.Method)
			w.WriteHeader(400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := discover(ctx, server.URL+endpointPath+"?apikey="+key, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Tools) != 2 || result.Methods["tools/list"] != 2 {
		t.Fatalf("incomplete discovery: %+v", result)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), key) {
		t.Fatal("credential leaked in metadata")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 3 || methods["initialize"] != 1 || methods["notifications/initialized"] != 1 {
		t.Fatalf("unexpected calls: %v", methods)
	}
}

func TestDiscoveryTransportRejectsBusinessMethods(t *testing.T) {
	const endpoint = "https://qyapi.weixin.qq.com/mcp/v2/bot/msg?apikey=canary"
	guard := &discoveryHTTP{endpoint: endpoint, methods: map[string]int{}}
	for _, tc := range []struct{ method, body string }{
		{http.MethodGet, ""}, {http.MethodDelete, ""},
		{http.MethodPost, `{"method":"tools/call","params":{"name":"receive_message"}}`},
		{http.MethodPost, `{"method":"resources/subscribe"}`},
	} {
		req, _ := http.NewRequest(tc.method, endpoint, strings.NewReader(tc.body))
		if _, err := guard.Handle(context.Background(), nil, req); err == nil {
			t.Fatal("non-discovery call allowed")
		}
	}
}

func TestInvalidEndpointsAndProviderErrorsDoNotLeakCredentials(t *testing.T) {
	for _, raw := range []string{"", "http://qyapi.weixin.qq.com" + endpointPath + "?apikey=private-canary", "https://other.example" + endpointPath + "?apikey=private-canary", "https://qyapi.weixin.qq.com/mcp?apikey=private-canary", "https://qyapi.weixin.qq.com" + endpointPath + "?apikey=a&apikey=b"} {
		if err := ValidateEndpoint(raw); err == nil || strings.Contains(err.Error(), "private-canary") {
			t.Fatalf("unsafe validation: %v", err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		_, _ = io.WriteString(w, "private-canary: "+r.URL.RawQuery)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := discover(ctx, server.URL+endpointPath+"?apikey=private-canary", server.Client())
	if err == nil || strings.Contains(err.Error(), "private-canary") || !strings.Contains(err.Error(), "403") {
		t.Fatalf("unsafe remote error: %v", err)
	}
}

func TestDiscoveryDoesNotFollowRedirects(t *testing.T) {
	var calls int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(500) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"?apikey=private-canary", http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := discover(ctx, source.URL+"?apikey=private-canary", source.Client()); err == nil {
		t.Fatal("redirect accepted")
	}
	if calls != 0 {
		t.Fatal("credential-bearing redirect reached another endpoint")
	}
}

func TestScrubEscapedCredentialMetadata(t *testing.T) {
	endpoint := `https://qyapi.weixin.qq.com/mcp/v2/bot/msg?apikey=secret%22value`
	encoded, _ := json.Marshal(map[string]string{"url": endpoint, "key": `secret"value`})
	safe := scrub(string(encoded), endpoint)
	var decoded map[string]string
	if json.Unmarshal([]byte(safe), &decoded) != nil || strings.Contains(safe, "secret") {
		t.Fatal("escaped credential was not scrubbed safely")
	}
}
