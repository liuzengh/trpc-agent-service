package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBuildApplicationComposesEveryServiceRole(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_DSN")
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if databaseURL == "" || brokers == "" {
		t.Skip("TEST_POSTGRES_DSN and TEST_KAFKA_BROKERS are required")
	}
	redisPort := os.Getenv("TEST_REDIS_PORT")
	if redisPort == "" {
		redisPort = "16379"
	}

	var modelRequests atomic.Int32
	modelServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/models" {
			http.NotFound(writer, request)
			return
		}
		modelRequests.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"object":"list","data":[{"id":"support-discovered","object":"model"}]}`))
	}))
	t.Cleanup(modelServer.Close)

	raw, err := os.ReadFile(filepath.Join("..", "..", "configs", "platform.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.Replace(string(raw), "https://api.openai.com/v1", modelServer.URL+"/v1", 1))
	configPath := filepath.Join(t.TempDir(), "platform.json")
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, role := range []serviceRole{roleWorker, roleGateway, roleChannel, roleAll} {
		t.Run(string(role), func(t *testing.T) {
			getenv := mapEnvironment(map[string]string{
				"SERVICE_ROLE":       string(role),
				"CONFIG_PATH":        configPath,
				"DATABASE_URL":       databaseURL,
				"REDIS_ADDR":         "127.0.0.1:" + redisPort,
				"AUDIT_HMAC_KEY":     "support-application-audit-key-at-least-32bytes",
				"KAFKA_BROKERS":      brokers,
				"KAFKA_TOPIC":        "support-application-events-" + string(role),
				"KAFKA_GROUP_ID":     "support-application-worker-" + string(role),
				"MODEL_API_KEY":      "test-model-key",
				"NODE_ID":            "support-application-node-" + string(role),
				"PROMETHEUS_ENABLED": "false",
				"LOGIN_PROVIDER":     "mock",
				"LOGIN_STATE_SECRET": "support-login-state-secret-at-least-32-bytes",
			})
			app, err := buildApplication(context.Background(), getenv)
			if err != nil {
				t.Fatalf("buildApplication(%s) error = %v", role, err)
			}
			t.Cleanup(app.Close)
			if app.handler == nil || app.listenAddress == "" || app.nodeLifecycle == nil {
				t.Fatalf("%s application missing shared runtime", role)
			}
			if role.runsWorker() != (len(app.workers) > 0) {
				t.Fatalf("%s worker presence = %v", role, len(app.workers) > 0)
			}
			if (role.runsGateway() || role.runsChannel()) != (app.replyOutbox != nil) {
				t.Fatalf("%s reply outbox presence = %v", role, app.replyOutbox != nil)
			}
		})
	}
	if got := modelRequests.Load(); got != 4 {
		t.Fatalf("model discovery requests = %d, want 4", got)
	}
}
