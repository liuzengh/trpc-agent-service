package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestValidateProcessRequestRejectsUntrustedDirectInputs(t *testing.T) {
	base := &Request{UserID: "user-a", SessionID: "session-a", Content: "hello"}
	cases := []struct {
		name string
		edit func(*Request)
	}{
		{"oversized content", func(req *Request) { req.Content = strings.Repeat("x", maxWorkerContentBytes+1) }},
		{"invalid user control", func(req *Request) { req.UserID = "user\x00a" }},
		{"oversized metadata", func(req *Request) {
			req.Metadata = map[string]interface{}{"large": strings.Repeat("x", maxWorkerMetadataBytes)}
		}},
		{"invalid traceparent", func(req *Request) {
			req.Metadata = map[string]interface{}{"traceparent": "00-00000000000000000000000000000001-0000000000000001-zz"}
		}},
		{"non-string traceparent", func(req *Request) {
			req.Metadata = map[string]interface{}{"traceparent": 1}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			req := *base
			test.edit(&req)
			if err := validateProcessRequest(&req, false); err == nil {
				t.Fatal("invalid direct request was accepted")
			}
		})
	}
}

func TestValidateRequestIsSafeForHTTPBoundaryReuse(t *testing.T) {
	req := &Request{
		UserID: "user-a", SessionID: "session-a", Content: "hello",
		Metadata: map[string]interface{}{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
	}
	if err := ValidateRequest(req); err != nil {
		t.Fatalf("exported request boundary rejected valid input: %v", err)
	}
}

func TestValidateProcessRequestAcceptsValidTraceParent(t *testing.T) {
	req := &Request{
		UserID: "user-a", SessionID: "session-a", Content: "hello",
		Metadata: map[string]interface{}{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
	}
	if err := validateProcessRequest(req, false); err != nil {
		t.Fatalf("valid traceparent rejected: %v", err)
	}
}

func TestValidateAttachmentURLRejectsNonRoutableLiteralHosts(t *testing.T) {
	for _, rawURL := range []string{
		"https://127.0.0.1/file",
		"https://10.0.0.8/file",
		"https://172.16.0.8/file",
		"https://192.168.1.8/file",
		"https://169.254.169.254/latest/meta-data",
		"https://[::1]/file",
		"https://[fc00::1]/file",
		"https://[fe80::1]/file",
		"https://0.0.0.0/file",
		"https://localhost/file",
		"https://service.localhost/file",
	} {
		if err := validateAttachmentURL(rawURL, true); err == nil {
			t.Errorf("validateAttachmentURL(%q) accepted a non-routable host", rawURL)
		}
	}
}

func TestValidateAttachmentURLAcceptsPublicHostAndPreservesDevelopmentHTTP(t *testing.T) {
	if err := validateAttachmentURL("https://cdn.example.com/file", true); err != nil {
		t.Fatalf("public HTTPS attachment rejected: %v", err)
	}
	if err := validateAttachmentURL("http://cdn.example.com/file", false); err != nil {
		t.Fatalf("development HTTP attachment rejected: %v", err)
	}
}

func TestNormalizeRequestSessionOwnerSeparatesGroupAndDirectIdentity(t *testing.T) {
	direct := &Request{UserID: "alice", SessionID: "direct-session"}
	if err := NormalizeRequestSessionOwner(direct); err != nil {
		t.Fatalf("normalize direct request: %v", err)
	}
	if direct.SessionOwnerID != "alice" {
		t.Fatalf("direct owner=%q, want alice", direct.SessionOwnerID)
	}

	group := &Request{
		UserID: "alice", ConversationID: "group-1", IsGroupChat: true,
		SessionID: "group-session",
	}
	if err := NormalizeRequestSessionOwner(group); err != nil {
		t.Fatalf("normalize group request: %v", err)
	}
	if group.SessionOwnerID == "" || group.SessionOwnerID == group.UserID {
		t.Fatalf("group owner=%q, want opaque shared owner", group.SessionOwnerID)
	}

	group.UserID = "bob"
	if err := NormalizeRequestSessionOwner(group); err != nil {
		t.Fatalf("normalize second group actor: %v", err)
	}
	if group.SessionOwnerID == "alice" || group.SessionOwnerID == "bob" {
		t.Fatalf("group owner became actor identity: %q", group.SessionOwnerID)
	}
}

func TestNormalizeRequestSessionOwnerValidatesCanonicalDirectSession(t *testing.T) {
	identity, err := channel.BuildSessionIdentity(&channel.InboundMessage{
		TenantID:         "tenant-a",
		ChannelType:      "telegram",
		ChannelAccountID: "bot-a",
		ExternalUserID:   "alice",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := &Request{
		TenantID: "tenant-a", ChannelType: "telegram", ChannelAccountID: "bot-a",
		UserID: "alice", SessionID: identity.SessionID,
	}
	if err := NormalizeRequestSessionOwner(req); err != nil {
		t.Fatalf("canonical direct session rejected: %v", err)
	}

	req.SessionID = "sess_" + strings.Repeat("0", 64)
	if err := NormalizeRequestSessionOwner(req); err == nil {
		t.Fatal("tampered canonical direct session was accepted")
	}
}

func TestStrictWorkerRejectsInactiveTenantBeforeBackendWork(t *testing.T) {
	worker := &Worker{
		tenant:         &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusSuspended},
		logicalAppName: "support",
		versionID:      "version-a",
		deploymentID:   "deployment-a",
		strictScope:    true,
	}
	_, err := worker.Process(context.Background(), &Request{
		TenantID: "tenant-a", AgentApp: "support", AgentVersion: "version-a", DeploymentID: "deployment-a",
		UserID: "user-a", SessionID: "session-a", Content: "hello",
	})
	if err == nil || !strings.Contains(err.Error(), ErrTenantNotActive.Error()) {
		t.Fatalf("inactive tenant error=%v, want tenant not active", err)
	}
}

func TestWorkerProcessClassifiesMissingSessionCoordination(t *testing.T) {
	worker := &Worker{
		tenant:         &tenant.Tenant{ID: "tenant-a", Status: tenant.TenantStatusActive, ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"}},
		logicalAppName: "support",
		appName:        "tenant-a/support",
	}
	_, err := worker.Process(context.Background(), &Request{
		TenantID: "tenant-a", AgentApp: "support",
		UserID: "user-a", SessionID: "session-a", Content: "hello",
	})
	if !errors.Is(err, ErrDistributedSessionCoordinationRequired) {
		t.Fatalf("missing coordination error = %v, want ErrDistributedSessionCoordinationRequired", err)
	}
}

func TestClientsRejectNilDependencies(t *testing.T) {
	if _, err := (*LocalClient)(nil).ProcessMessage(context.Background(), &Request{}); err == nil {
		t.Fatal("nil local client unexpectedly succeeded")
	}
	if _, err := (&LocalClient{}).ProcessMessage(context.Background(), &Request{}); err == nil {
		t.Fatal("unconfigured local client unexpectedly succeeded")
	}
	if _, err := (*HTTPClient)(nil).ProcessMessage(context.Background(), &Request{}); err == nil {
		t.Fatal("nil HTTP client unexpectedly succeeded")
	}
	if err := (*Worker)(nil).Close(); err != nil {
		t.Fatalf("nil Worker.Close error=%v", err)
	}
}
