package main

import (
	"strings"
	"testing"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

func TestRedactionSecretsCoverServiceCredentialsAndDataSourcePasswords(t *testing.T) {
	values := map[string]string{
		"TRPC_WORKER_TOKEN":               "worker-canary",
		"TRPC_CONTROL_PLANE_POSTGRES_DSN": "postgres://control:control-canary@db/control",
		"TRPC_AUDIT_POSTGRES_DSN":         "postgres://audit:audit-canary@db/audit",
		"TRPC_POSTGRES_DSN":               "postgres://data:data-canary@db/data",
	}
	redactor := servicelog.NewRedactor(redactionSecrets(func(name string) string { return values[name] }), nil)
	got := redactor.Redact(strings.Join([]string{
		values["TRPC_WORKER_TOKEN"], values["TRPC_CONTROL_PLANE_POSTGRES_DSN"],
		"control-canary", "audit-canary", "data-canary",
	}, " "))
	for _, secret := range []string{"worker-canary", "control-canary", "audit-canary", "data-canary"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redactor leaked %q in %q", secret, got)
		}
	}
}
