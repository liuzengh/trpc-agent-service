package main

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type recordingTenantDispatcher struct {
	tenants []string
}

func (d *recordingTenantDispatcher) DispatchTenant(_ context.Context, tenantID string, _ int) (int, error) {
	d.tenants = append(d.tenants, tenantID)
	return 1, nil
}

func TestReplyOutboxCycleDiscoversTenantsFromControlPlane(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	for _, tenantConfig := range []config.TenantConfig{
		{TenantID: "tenant-late", AppCode: "bot", Status: config.AgentActive, ConfigVersion: 1},
		{TenantID: "tenant-late", AppCode: "other", Status: config.AgentActive, ConfigVersion: 1},
		{TenantID: "tenant-existing", AppCode: "bot", Status: config.AgentActive, ConfigVersion: 1},
	} {
		if _, err := repository.Publish(context.Background(), tenantConfig); err != nil {
			t.Fatalf("Publish(%s) error = %v", tenantConfig.AppName(), err)
		}
	}
	dispatcher := &recordingTenantDispatcher{}
	app := &application{replyOutbox: dispatcher, configurations: repository}
	if err := app.dispatchReplyOutboxOnce(context.Background()); err != nil {
		t.Fatalf("dispatchReplyOutboxOnce() error = %v", err)
	}
	if got := strings.Join(dispatcher.tenants, ","); got != "tenant-existing,tenant-late" {
		t.Fatalf("dispatched tenants = %q, want tenant-existing,tenant-late", got)
	}
}

func TestComposeIdentityProvidersIncludesConfiguredWeComAgentID(t *testing.T) {
	environment := map[string]string{
		"LOGIN_PROVIDERS_JSON": `{
          "providers":[{
            "id":"wecom-acme","type":"wecom","display_name":"企业微信",
            "corp_id":"ww-test","agent_id":1000001,"secret_ref":"env:WECOM_LOGIN_SECRET",
            "auth_base_url":"https://open.example.test"
          }]
        }`,
		"LOGIN_CALLBACK_URL": "https://console.example.com/api/v1/auth/callback",
		"WECOM_LOGIN_SECRET": "secret",
	}
	secrets, err := credential.NewEnvironmentSecretResolver(func(name string) string { return environment[name] })
	if err != nil {
		t.Fatal(err)
	}
	providers, err := composeIdentityProviders(context.Background(), func(name string) string { return environment[name] }, identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
	if err != nil {
		t.Fatalf("composeIdentityProviders() error = %v", err)
	}
	provider := providers["wecom-acme"]
	authorizeURL, err := provider.Begin(identity.AuthRequest{State: "state-1"})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	parsed, err := url.Parse(authorizeURL)
	if err != nil {
		t.Fatalf("parse authorize URL: %v", err)
	}
	if got, want := parsed.Query().Get("agentid"), "1000001"; got != want {
		t.Fatalf("agentid = %q, want %q", got, want)
	}
}

func TestServeHTTPServerStopsAfterRootContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusNoContent) })}
	finished := make(chan error, 1)
	go func() { finished <- ServeHTTPServer(ctx, server, listener) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if requestErr == nil {
			_ = response.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("ServeHTTPServer() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeHTTPServer() did not return after cancellation")
	}
}

func TestServeHTTPServerRecoversHandlerPanicAndKeepsServing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/panic" {
			panic("malformed request state")
		}
		writer.WriteHeader(http.StatusNoContent)
	})}
	finished := make(chan error, 1)
	go func() { finished <- ServeHTTPServer(ctx, server, listener) }()
	client := &http.Client{Timeout: time.Second}
	baseURL := "http://" + listener.Addr().String()

	var panicResponse *http.Response
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		panicResponse, err = client.Get(baseURL + "/panic")
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("panic request failed: %v", err)
	}
	_ = panicResponse.Body.Close()
	if panicResponse.StatusCode != http.StatusInternalServerError {
		t.Fatalf("panic status = %d, want 500", panicResponse.StatusCode)
	}
	response, err := client.Get(baseURL + "/healthy")
	if err != nil {
		t.Fatalf("request after panic failed: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status after panic = %d, want 204", response.StatusCode)
	}
	if server.ErrorLog == nil {
		t.Fatal("ServeHTTPServer did not install ErrorLog")
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("ServeHTTPServer() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeHTTPServer did not stop")
	}
}
