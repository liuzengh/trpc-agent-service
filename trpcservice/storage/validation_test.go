package storage

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestBackendPreflightWithoutConnections(t *testing.T) {
	for _, tc := range []struct {
		name, resource, kind, config, ref string
		valid                             bool
	}{
		{"startup", "session", "startup_config", `{}`, "", true},
		{"malformed startup", "session", "startup_config", `{`, "", false},
		{"invalid ttl", "session", "inmemory", `{"ttl":"wrong"}`, "", false},
		{"redis missing address", "session", "redis", `{}`, "", false},
		{"redis deferred credentials", "session", "redis", `{}`, "env://UNSET_SESSION_KEY", true},
		{"sql deferred credentials", "session", "postgres", `{}`, "env://UNSET_SESSION_KEY", true},
		{"negative memory", "memory", "inmemory", `{"memory_limit":-1}`, "", false},
		{"memory SQL deferred credentials", "memory", "postgres", `{"skip_db_init":true}`, "env://UNSET_MEMORY_KEY", true},
		{"vector config", "knowledge", "qdrant", `{"host":"127.0.0.1","port":1,"collection_name":"fixture","dimensions":32}`, "", true},
		{"vector missing host", "knowledge", "qdrant", `{"port":6334}`, "", false},
		{"artifact missing bucket", "artifact", "s3", `{}`, "env://UNSET_S3_KEY", false},
		{"artifact deferred credentials", "artifact", "s3", `{"bucket":"fixture"}`, "env://UNSET_S3_KEY", true},
		{"unknown input", "memory", "inmemory", `{"private-canary":"private-canary"}`, "", false},
		{"null", "artifact", "inmemory", `null`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateBackendBindingConfig(controlplane.BackendBinding{ResourceType: tc.resource, BackendType: tc.kind, Config: json.RawMessage(tc.config), SecretRef: tc.ref})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if err != nil && strings.Contains(err.Error(), "private-canary") {
				t.Fatal("parser echoed private input")
			}
		})
	}
}
