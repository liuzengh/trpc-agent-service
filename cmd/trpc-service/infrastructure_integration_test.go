package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestComposeInfrastructureBuildsWorkerAgainstRealDependencies(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_DSN")
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if databaseURL == "" || brokers == "" {
		t.Skip("TEST_POSTGRES_DSN and TEST_KAFKA_BROKERS are required")
	}
	redisPort := os.Getenv("TEST_REDIS_PORT")
	if redisPort == "" {
		redisPort = "16379"
	}

	serviceConfig, err := loadServiceConfig(mapEnvironment(map[string]string{
		"CONFIG_PATH": filepath.Join("..", "..", "configs", "platform.example.json"),
	}))
	if err != nil {
		t.Fatalf("loadServiceConfig() error = %v", err)
	}
	getenv := mapEnvironment(map[string]string{
		"DATABASE_URL":       databaseURL,
		"REDIS_ADDR":         "127.0.0.1:" + redisPort,
		"AUDIT_HMAC_KEY":     "support-infrastructure-audit-key-32bytes",
		"KAFKA_BROKERS":      brokers,
		"KAFKA_TOPIC":        "support-runtime-events",
		"KAFKA_GROUP_ID":     "support-worker-integration",
		"MODEL_API_KEY":      "test-model-key",
		"NODE_ID":            "support-worker-integration",
		"PROMETHEUS_ENABLED": "false",
	})

	infra, err := composeInfrastructure(context.Background(), serviceConfig, getenv, roleWorker)
	if err != nil {
		t.Fatalf("composeInfrastructure() error = %v", err)
	}
	t.Cleanup(infra.Close)

	if len(infra.workers) != defaultWorkerConcurrency || infra.repository == nil || infra.stateStore == nil || infra.runners == nil {
		t.Fatalf("worker infrastructure is incomplete: workers=%d repository=%v state=%v runners=%v",
			len(infra.workers), infra.repository != nil, infra.stateStore != nil, infra.runners != nil)
	}
	if infra.modelProvider == nil || infra.platformStores == nil || infra.backendProfiles == nil || infra.nodeLifecycle == nil {
		t.Fatal("worker infrastructure is missing provider or node dependencies")
	}
	for _, name := range []string{"postgres", "redis", "kafka"} {
		probe := infra.probes[name]
		if probe == nil {
			t.Fatalf("probe %q is missing", name)
		}
		if err := probe(context.Background()); err != nil {
			t.Fatalf("probe %q error = %v", name, err)
		}
	}
}

func TestComposeInfrastructureRollsBackLateWorkerFailure(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_DSN")
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if databaseURL == "" || brokers == "" {
		t.Skip("TEST_POSTGRES_DSN and TEST_KAFKA_BROKERS are required")
	}
	redisPort := os.Getenv("TEST_REDIS_PORT")
	if redisPort == "" {
		redisPort = "16379"
	}
	serviceConfig, err := loadServiceConfig(mapEnvironment(map[string]string{
		"CONFIG_PATH": filepath.Join("..", "..", "configs", "platform.example.json"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	getenv := mapEnvironment(map[string]string{
		"DATABASE_URL":   databaseURL,
		"REDIS_ADDR":     "127.0.0.1:" + redisPort,
		"AUDIT_HMAC_KEY": "support-infrastructure-audit-key-32bytes",
		"KAFKA_BROKERS":  brokers,
		"KAFKA_TOPIC":    "support-runtime-events",
		"MODEL_API_KEY":  "test-model-key",
	})

	if _, err := composeInfrastructure(context.Background(), serviceConfig, getenv, roleWorker); err == nil {
		t.Fatal("composeInfrastructure() accepted missing KAFKA_GROUP_ID")
	}
}

func TestComposeInfrastructureBuildsSupportedRoles(t *testing.T) {
	databaseURL := os.Getenv("TEST_POSTGRES_DSN")
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if databaseURL == "" || brokers == "" {
		t.Skip("TEST_POSTGRES_DSN and TEST_KAFKA_BROKERS are required")
	}
	redisPort := os.Getenv("TEST_REDIS_PORT")
	if redisPort == "" {
		redisPort = "16379"
	}
	serviceConfig, err := loadServiceConfig(mapEnvironment(map[string]string{
		"CONFIG_PATH": filepath.Join("..", "..", "configs", "platform.example.json"),
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, role := range []serviceRole{roleGateway, roleChannel, roleAll} {
		t.Run(string(role), func(t *testing.T) {
			values := map[string]string{
				"DATABASE_URL":       databaseURL,
				"REDIS_ADDR":         "127.0.0.1:" + redisPort,
				"AUDIT_HMAC_KEY":     "support-role-audit-key-at-least-32bytes",
				"KAFKA_BROKERS":      brokers,
				"KAFKA_TOPIC":        "support-role-events-" + string(role),
				"KAFKA_GROUP_ID":     "support-role-worker-" + string(role),
				"MODEL_API_KEY":      "test-model-key",
				"NODE_ID":            "support-role-node-" + string(role),
				"PROMETHEUS_ENABLED": "false",
				"LOGIN_PROVIDER":     "mock",
				"LOGIN_STATE_SECRET": "support-login-state-secret-at-least-32-bytes",
			}
			infra, err := composeInfrastructure(context.Background(), serviceConfig, mapEnvironment(values), role)
			if err != nil {
				t.Fatalf("composeInfrastructure(%s) error = %v", role, err)
			}
			t.Cleanup(infra.Close)

			if infra.repository == nil || infra.stateStore == nil || infra.nodeLifecycle == nil {
				t.Fatalf("%s infrastructure missing shared components", role)
			}
			if role.runsGateway() && (infra.authHandler == nil || infra.applicationValidator == nil) {
				t.Fatalf("%s infrastructure missing gateway components", role)
			}
			if (role.runsGateway() || role.runsChannel()) && (infra.channelConnectors == nil || infra.replyOutbox == nil || infra.producer == nil) {
				t.Fatalf("%s infrastructure missing channel components", role)
			}
			if role.runsWorker() && (len(infra.workers) == 0 || infra.runners == nil || infra.knowledgeIngestWorker == nil || infra.kafkaCapacity == nil) {
				t.Fatalf("%s infrastructure missing worker components", role)
			}
		})
	}
}

func TestInfrastructureEnvironmentParsing(t *testing.T) {
	getenv := mapEnvironment(map[string]string{
		"BOOL_TRUE":  " YES ",
		"BOOL_FALSE": "off",
		"INT_VALID":  " 17 ",
		"INT_ZERO":   "0",
		"INT_BAD":    "not-a-number",
	})
	if !envBool(getenv, "BOOL_TRUE") {
		t.Fatal("envBool() rejected truthy value")
	}
	if envBool(getenv, "BOOL_FALSE") || envBool(getenv, "MISSING") {
		t.Fatal("envBool() accepted false or missing value")
	}
	if got := envInt(getenv, "INT_VALID", 9); got != 17 {
		t.Fatalf("envInt(valid) = %d, want 17", got)
	}
	for _, name := range []string{"INT_ZERO", "INT_BAD", "MISSING"} {
		if got := envInt(getenv, name, 9); got != 9 {
			t.Fatalf("envInt(%s) = %d, want fallback 9", name, got)
		}
	}
}

func TestInfrastructureCloseRunsCleanupsInReverseOrder(t *testing.T) {
	var order []int
	infra := &infrastructure{closeFuncs: []func(){
		func() { order = append(order, 1) },
		func() { order = append(order, 2) },
		func() { order = append(order, 3) },
	}}
	infra.Close()
	if got := fmt.Sprint(order); got != "[3 2 1]" {
		t.Fatalf("cleanup order = %s, want [3 2 1]", got)
	}
	var nilInfra *infrastructure
	nilInfra.Close()
}
