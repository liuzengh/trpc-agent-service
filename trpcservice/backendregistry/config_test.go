package backendregistry

import (
	"bytes"
	"encoding/json"
	"net/url"
	"testing"
)

func TestBuildSeparatesCredentialsAndValidatesBackends(t *testing.T) {
	for _, tc := range []struct {
		name, resource, kind string
		settings             Settings
		secrets              Secrets
		existing             string
		valid                bool
	}{
		{"redis", "session", "redis", Settings{Host: "redis.example", Port: 6379, Database: "0", Username: "user", TLS: true}, Secrets{Password: "secret-canary/@?#"}, "", true},
		{"postgres", "memory", "postgres", Settings{Host: "db.example", Port: 5432, Database: "app", Username: "user", SSLMode: "require"}, Secrets{Password: "secret-canary"}, "", true},
		{"qdrant", "knowledge", "qdrant", Settings{Host: "vector.example", Port: 6334, Collection: "docs", Dimensions: 3}, Secrets{APIKey: "secret-canary"}, "", true},
		{"s3", "artifact", "s3", Settings{Bucket: "documents", Endpoint: "https://objects.example", Region: "test"}, Secrets{AccessKeyID: "test", SecretAccessKey: "secret-canary"}, "", true},
		{"memory", "knowledge", "inmemory", Settings{}, Secrets{}, "", true},
		{"existing", "memory", "redis", Settings{KeyPrefix: "tenant"}, Secrets{}, "env://REDIS", true},
		{"wrong-resource", "artifact", "redis", Settings{}, Secrets{}, "env://REDIS", false},
		{"missing-host", "session", "redis", Settings{Port: 6379, Database: "0"}, Secrets{}, "", false},
		{"host-injection", "session", "redis", Settings{Host: "user:secret-canary@evil", Port: 6379, Database: "0"}, Secrets{}, "", false},
		{"invalid-port", "session", "redis", Settings{Host: "example", Port: 65536, Database: "0"}, Secrets{}, "", false},
		{"invalid-db", "session", "redis", Settings{Host: "example", Port: 6379, Database: "-1"}, Secrets{}, "", false},
		{"inline-s3-secret", "artifact", "s3", Settings{Bucket: "docs", Endpoint: "https://user:secret-canary@objects.example"}, Secrets{AccessKeyID: "x", SecretAccessKey: "y"}, "", false},
		{"missing-s3-key", "artifact", "s3", Settings{Bucket: "docs"}, Secrets{}, "", false},
		{"mixed-credentials", "session", "redis", Settings{}, Secrets{Password: "secret-canary"}, "env://REDIS", false},
		{"inmemory-secret", "memory", "inmemory", Settings{}, Secrets{Password: "secret-canary"}, "", false},
		{"unknown", "session", "mysql", Settings{}, Secrets{}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, value, err := Build(tc.resource, tc.kind, tc.settings, tc.secrets, tc.existing)
			if (err == nil) != tc.valid {
				t.Fatalf("unexpected validity: %v", err)
			}
			if bytes.Contains(raw, []byte("secret-canary")) {
				t.Fatal("credential entered public configuration")
			}
			if !tc.valid {
				return
			}
			if !json.Valid(raw) {
				t.Fatal("invalid public JSON")
			}
			if tc.kind == "redis" && tc.existing == "" {
				u, err := url.Parse(value)
				if err != nil || u.Scheme != "rediss" {
					t.Fatal("invalid Redis endpoint")
				}
				pass, _ := u.User.Password()
				if pass != tc.secrets.Password {
					t.Fatal("credential URL encoding corrupted password")
				}
			}
		})
	}
}

func TestPublicConnectionOmitsInternalCredential(t *testing.T) {
	raw, err := json.Marshal(Connection{TenantID: "tenant", ID: "id", Name: "friendly", SecretRef: "managed://internal", Config: json.RawMessage(`{"dsn":"private"}`)})
	if err != nil || bytes.Contains(raw, []byte("managed://")) || bytes.Contains(raw, []byte("private")) {
		t.Fatal("internal connection configuration leaked")
	}
}
