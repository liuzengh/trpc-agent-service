package log

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

func TestAuditRedactsSecretsAndPII(t *testing.T) {
	var out bytes.Buffer
	secret := "fixture-audit-credential"
	sink := NewJSONLines(&out, NewRedactor(nil, []string{secret}))
	err := sink.Write(Entry{Reason: "token=" + secret + " mail=a@example.com phone=13800138000"})
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, forbidden := range []string{secret, "a@example.com", "13800138000"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("sensitive value leaked in %q", got)
		}
	}
}

func TestRouterUsesImmutableEntryPolicyAndCurrentReferencedSecret(t *testing.T) {
	const (
		envName = "AUDIT_ROTATED_SECRET"
		secret  = "rotated-fixture-credential"
	)
	t.Setenv(envName, secret)
	var out bytes.Buffer
	router := NewRouter(nil, &out, nil)
	policy := config.AuditPolicy{Enabled: true, Sink: "stdout"}
	err := router.Write(Entry{
		TenantID: "tenant-a", ConfigRevision: "v1", Decision: "allow",
		Reason: secret, PolicySnapshot: &policy, SecretEnvNames: []string{envName},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), secret) || !strings.Contains(out.String(), "[REDACTED]") {
		t.Fatalf("snapshot-routed audit did not redact the current secret: %q", out.String())
	}
}

func TestRouterRefreshesRotatedAndRuntimeSecretsForCachedSink(t *testing.T) {
	const envName = "AUDIT_DYNAMIC_SECRET"
	var out bytes.Buffer
	router := NewRouter(nil, &out, nil)
	policy := config.AuditPolicy{Enabled: true, Sink: "stdout"}
	write := func(secret string) {
		t.Helper()
		config.RegisterSecret(envName, secret)
		if err := router.Write(Entry{
			TenantID: "tenant-a", ConfigRevision: "v1", Decision: "deny",
			Reason: secret, PolicySnapshot: &policy, SecretEnvNames: []string{envName},
		}); err != nil {
			t.Fatal(err)
		}
	}
	defer config.RegisterSecret(envName, "")
	write("first-runtime-secret")
	write("rotated-runtime-secret")
	for _, forbidden := range []string{"first-runtime-secret", "rotated-runtime-secret"} {
		if strings.Contains(out.String(), forbidden) {
			t.Fatalf("cached audit sink leaked %q: %s", forbidden, out.String())
		}
	}
}
