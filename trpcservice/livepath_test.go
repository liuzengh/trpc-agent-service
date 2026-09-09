package trpcservice

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/Violet2314/trpc-agent-service/trpcservice/admin"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels/webui"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/Violet2314/trpc-agent-service/trpcservice/log"
	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/web"
	"github.com/Violet2314/trpc-agent-service/trpcservice/worker"
)

// TestLiveWebUIPath wires Admin + Gateway + WebUI the same way main does,
// without Docker, MySQL, or a real LLM. It is the automated stand-in for
// the browser dropdown + isolated chat path.
func TestLiveWebUIPath(t *testing.T) {
	store := tenant.NewMemoryStore()
	cache, err := tenant.NewConfigCache(store, time.Second)
	if err != nil {
		t.Fatalf("NewConfigCache() error = %v", err)
	}
	adminHandler, err := admin.NewHandler(store, cache, "admin", "test-password")
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	mini := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	deduper, err := gateway.NewRedisDeduper(redisClient, time.Second, time.Minute)
	if err != nil {
		t.Fatalf("NewRedisDeduper() error = %v", err)
	}
	lock, err := gateway.NewRedisSessionLock(redisClient, gateway.RedisSessionLockConfig{TTL: 3 * time.Second})
	if err != nil {
		t.Fatalf("NewRedisSessionLock() error = %v", err)
	}

	registry := channels.NewRegistry()
	adapter, err := webui.New(webui.NewHub(), web.Handler(), webui.StoreCatalog{Store: store})
	if err != nil {
		t.Fatalf("webui.New() error = %v", err)
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	router, err := gateway.NewRouter(
		cache,
		deduper,
		lock,
		gateway.NewDebouncer(0),
		storage.NewMemoryEventStore(),
		tenantEchoExecutor{},
		registry,
		platformlog.NopAuditWriter{},
	)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	t.Cleanup(router.Drain)

	mux := NewHTTPMux(HTTPOptions{AdminHandler: adminHandler})
	if err := registry.Run(context.Background(), mux, router.Handle); err != nil {
		t.Fatalf("channel Run() error = %v", err)
	}

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	page, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("GET / error = %v", err)
	}
	defer page.Body.Close()
	body, _ := io.ReadAll(page.Body)
	if page.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("选择 Agent")) {
		t.Fatalf("GET / did not serve the WebUI workbench, status=%d", page.StatusCode)
	}

	createLiveTenant(t, server.URL, "tenant-a", "Tenant A", "app-a", "support-a", "redis", "pgvector", "binding-a")
	createLiveTenant(t, server.URL, "tenant-b", "Tenant B", "app-b", "support-b", "mysql", "mem0", "binding-b")

	desks := getJSON[[]webui.Desk](t, server.URL+"/channels/webui/desks", "", "")
	if len(desks) != 2 {
		t.Fatalf("desks = %#v, want 2 webui routes", desks)
	}
	if desks[0].RouteKey != "binding-a" || desks[1].RouteKey != "binding-b" {
		t.Fatalf("desk order = %#v", desks)
	}

	replyA := chatLive(t, server.URL, "binding-a", "hello-a")
	replyB := chatLive(t, server.URL, "binding-b", "hello-b")
	if !strings.Contains(replyA, "tenant-a") {
		t.Fatalf("tenant A stream = %q, want tenant-a", replyA)
	}
	if strings.Contains(replyA, "tenant-b") {
		t.Fatalf("tenant A stream leaked tenant-b: %q", replyA)
	}
	if !strings.Contains(replyB, "tenant-b") {
		t.Fatalf("tenant B stream = %q, want tenant-b", replyB)
	}
	if strings.Contains(replyB, "tenant-a") {
		t.Fatalf("tenant B stream leaked tenant-a: %q", replyB)
	}
}

type tenantEchoExecutor struct{}

func (tenantEchoExecutor) Execute(
	_ context.Context,
	snapshot tenant.Snapshot,
	_ string,
	_ []storage.UserEvent,
) (<-chan worker.Event, error) {
	events := make(chan worker.Event, 2)
	events <- worker.Event{Type: "text_delta", Text: snapshot.Tenant.ID}
	events <- worker.Event{Type: "done"}
	close(events)
	return events, nil
}

func createLiveTenant(
	t *testing.T,
	base, tenantID, name, appID, appName, session, memory, bindingID string,
) {
	t.Helper()
	adminJSON(t, base, http.MethodPost, "/api/v1/tenants", tenant.Tenant{
		ID: tenantID, Name: name, IsActive: true,
		Quota:  tenant.Quota{DailyTokenLimit: 1000, RatePerMinute: 30},
		Policy: tenant.Policy{AuditLevel: "full"},
	})
	adminJSON(t, base, http.MethodPost, "/api/v1/tenants/"+tenantID+"/apps", tenant.AgentApp{
		ID: appID, TenantID: tenantID, AppName: appName,
		Model: tenant.ModelConfig{
			Provider: "openai-compatible", Model: "test", APIKeyRef: "env:KEY",
		},
		Backends: tenant.BackendSelection{Session: session, Memory: memory},
	})
	adminJSON(t, base, http.MethodPost, "/api/v1/apps/"+appID+"/bindings", tenant.ChannelBinding{
		ID: bindingID, TenantID: tenantID, AppID: appID,
		Channel: "webui", RouteKey: bindingID, IsActive: true,
	})
}

func adminJSON(t *testing.T, base, method, path string, body any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	request, err := http.NewRequest(method, base+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth("admin", "test-password")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(response.Body)
		t.Fatalf("%s %s status = %d, body = %s", method, path, response.StatusCode, raw)
	}
}

func getJSON[T any](t *testing.T, url, user, password string) T {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if user != "" {
		request.SetBasicAuth(user, password)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(response.Body)
		t.Fatalf("GET %s status = %d, body = %s", url, response.StatusCode, raw)
	}
	var value T
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return value
}

func chatLive(t *testing.T, base, binding, text string) string {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar: %v", err)
	}
	client := &http.Client{Jar: jar, Timeout: 5 * time.Second}
	payload, _ := json.Marshal(map[string]string{"id": "msg-" + binding + "-" + text, "text": text})
	post, err := client.Post(
		base+"/channels/webui/"+binding+"/messages",
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("POST message: %v", err)
	}
	defer post.Body.Close()
	raw, _ := io.ReadAll(post.Body)
	if post.StatusCode != http.StatusOK {
		t.Fatalf("POST status = %d, body = %s", post.StatusCode, raw)
	}
	var result gateway.Result
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode POST: %v", err)
	}
	if result.SessionID == "" {
		t.Fatal("POST omitted session_id")
	}

	deadline := time.Now().Add(3 * time.Second)
	var stream string
	for time.Now().Before(deadline) {
		stream = readSSE(t, client, base+"/channels/webui/"+binding+"/stream?session="+url.QueryEscape(result.SessionID))
		if strings.Contains(stream, "event: done") {
			return stream
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("SSE for %s never completed: %q", binding, stream)
	return ""
}

func readSSE(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	response, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET stream: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	return string(body)
}
