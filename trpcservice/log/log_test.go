package log_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRoutingFieldsAllowlistsNonSensitiveValues(t *testing.T) {
	tc := tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		Channel:            "wecom",
		BindingID:          "binding-1",
		SessionID:          "session-1",
		SessionPrincipalID: "private-principal",
		UserID:             "private-user",
		TraceID:            "trace-1",
	}

	got := platformlog.RoutingFields(tc)
	want := map[string]string{
		"tenant_id":      "tenant-a",
		"app_id":         "support",
		"config_version": "v1",
		"channel":        "wecom",
		"binding_id":     "binding-1",
		"session_id":     "session-1",
		"trace_id":       "trace-1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("routing fields = %#v, want %#v", got, want)
	}
}

func TestSafeErrorRedactsCredentialsAndToolArguments(t *testing.T) {
	raw := errors.New(`request failed: authorization=Bearer bearer-secret api_key=sk-1234567890123456 dsn=postgres://user:db-secret@example.test/db tool_args={"email":"person@example.test"}`)
	got := platformlog.SafeError(raw)
	for _, secret := range []string{
		"bearer-secret",
		"sk-1234567890123456",
		"db-secret",
		`"email":"person@example.test"`,
	} {
		if strings.Contains(got, secret) {
			t.Fatalf("safe error contains %q: %q", secret, got)
		}
	}
	if !strings.Contains(got, "request failed") || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("safe error = %q, want bounded diagnostic text and redactions", got)
	}
}

func TestSafeErrorBoundsOutput(t *testing.T) {
	got := platformlog.SafeError(errors.New(strings.Repeat("x", 1024)))
	if len([]rune(got)) > 512 {
		t.Fatalf("safe error length = %d, want at most 512", len([]rune(got)))
	}
}

func TestSafeErrorRedactsStandalonePII(t *testing.T) {
	got := platformlog.SafeError(errors.New("contact alice@example.com or 13800138000"))
	for _, secret := range []string{"alice@example.com", "13800138000"} {
		if strings.Contains(got, secret) {
			t.Fatalf("safe error contains %q: %q", secret, got)
		}
	}
}
