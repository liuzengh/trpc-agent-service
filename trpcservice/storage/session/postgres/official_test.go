package postgres

import (
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestTenantKeyEncoding(t *testing.T) {
	k := TenantKey("tenant-1", "agent-app-2", "user-3", "session-4")
	if k.AppName != "tenant-1/agent-app-2" {
		t.Fatalf("AppName = %q, want %q", k.AppName, "tenant-1/agent-app-2")
	}
	if k.UserID != "user-3" || k.SessionID != "session-4" {
		t.Fatalf("key = %+v", k)
	}
}

func TestParseTenantApp(t *testing.T) {
	tests := []struct {
		name         string
		appName      string
		wantTenant   string
		wantAgentApp string
		wantOK       bool
	}{
		{"valid", "tenant-1/agent-app-2", "tenant-1", "agent-app-2", true},
		{"empty", "", "", "", false},
		{"separator only", "/", "", "", false},
		{"missing agent app", "tenant-1/", "", "", false},
		{"missing tenant", "/agent-app-2", "", "", false},
		{"extra separator", "tenant-1/agent-app-2/extra", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenant, app, ok := ParseTenantApp(tt.appName)
			if ok != tt.wantOK || tenant != tt.wantTenant || app != tt.wantAgentApp {
				t.Fatalf("ParseTenantApp(%q) = (%q,%q,%v), want (%q,%q,%v)",
					tt.appName, tenant, app, ok, tt.wantTenant, tt.wantAgentApp, tt.wantOK)
			}
		})
	}

	// Round-trip: encoding then decoding must be lossless.
	tenant, app, ok := ParseTenantApp(TenantKey("a", "b", "u", "s").AppName)
	if !ok || tenant != "a" || app != "b" {
		t.Fatalf("round-trip = (%q,%q,%v)", tenant, app, ok)
	}
}

func TestTenantGetSessionHook(t *testing.T) {
	hook := TenantGetSessionHook("tenant-1", "agent-app-2")
	called := false
	next := func() (*session.Session, error) {
		called = true
		return &session.Session{}, nil
	}

	// In-scope key must pass through and invoke next.
	okCtx := &session.GetSessionContext{Key: TenantKey("tenant-1", "agent-app-2", "u", "s")}
	if _, err := hook(okCtx, next); err != nil || !called {
		t.Fatalf("in-scope read rejected: err=%v called=%v", err, called)
	}

	// Cross-tenant key must fail closed without invoking next.
	called = false
	badCtx := &session.GetSessionContext{Key: TenantKey("tenant-9", "agent-app-2", "u", "s")}
	if _, err := hook(badCtx, next); err == nil || called {
		t.Fatalf("cross-tenant read admitted: err=%v called=%v", err, called)
	}

	// Cross-app key must also fail closed.
	called = false
	badAppCtx := &session.GetSessionContext{Key: TenantKey("tenant-1", "agent-app-9", "u", "s")}
	if _, err := hook(badAppCtx, next); err == nil || called {
		t.Fatalf("cross-app read admitted: err=%v called=%v", err, called)
	}

	// The underlying error from next must propagate (no swallowing).
	sentinel := errors.New("boom")
	propCtx := &session.GetSessionContext{Key: TenantKey("tenant-1", "agent-app-2", "u", "s")}
	nextErr := func() (*session.Session, error) { return nil, sentinel }
	if _, err := hook(propCtx, nextErr); !errors.Is(err, sentinel) {
		t.Fatalf("underlying error not propagated: %v", err)
	}
}
