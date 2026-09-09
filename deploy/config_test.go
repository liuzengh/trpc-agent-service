package deploy_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestComposeAndMonitoringYAMLParse(t *testing.T) {
	files, err := filepath.Glob("*.y*ml")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 4 {
		t.Fatalf("expected compose and monitoring manifests, found %d", len(files))
	}
	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			file, err := os.Open(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			decoder := yaml.NewDecoder(file)
			for document := 1; ; document++ {
				var value map[string]interface{}
				err := decoder.Decode(&value)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("document %d: %v", document, err)
				}
				if len(value) == 0 {
					continue
				}
				if filename == "docker-compose.yml" && value["services"] == nil {
					t.Fatal("compose manifest has no services")
				}
				if filename == "prometheus-rules.yml" && value["groups"] == nil {
					t.Fatal("Prometheus rules have no groups")
				}
			}
		})
	}
}

func TestComposePublishesDevelopmentPortsOnLoopbackOnly(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Ports []string `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	for service, config := range compose.Services {
		for _, published := range config.Ports {
			if !strings.HasPrefix(published, "127.0.0.1:") {
				t.Errorf("service %s publishes a non-loopback port %q", service, published)
			}
		}
	}
}

func TestComposePinsInfrastructureImagesAndHardensApplicationContainers(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Image       string   `yaml:"image"`
			ReadOnly    bool     `yaml:"read_only"`
			Tmpfs       []string `yaml:"tmpfs"`
			CapDrop     []string `yaml:"cap_drop"`
			SecurityOpt []string `yaml:"security_opt"`
			Restart     string   `yaml:"restart"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}

	expectedImages := map[string]string{
		"postgres":       "postgres:15.8-alpine@sha256:8b963ea3038c3b32182ee7f592ccde21242fa7c5fd9d1b72aa333c27f1bfc809",
		"redis":          "redis:7.4-alpine@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf",
		"otel-collector": "otel/opentelemetry-collector-contrib:0.108.0@sha256:923eb1cfae32fe09676cfd74762b2b237349f2273888529594f6c6ffe1fb3d7e",
		"prometheus":     "prom/prometheus:v2.54.1@sha256:f6639335d34a77d9d9db382b92eeb7fc00934be8eae81dbc03b31cfe90411a94",
		"grafana":        "grafana/grafana:11.1.4@sha256:886b56d5534e54f69a8cfcb4b8928da8fc753178a7a3d20c3f9b04b660169805",
	}
	for service, expected := range expectedImages {
		if got := compose.Services[service].Image; got != expected {
			t.Errorf("service %s image=%q, want digest-pinned %q", service, got, expected)
		}
	}
	for _, service := range []string{"postgres", "redis", "otel-collector", "gateway", "worker", "summary-worker", "consumer", "delivery", "admin", "prometheus", "grafana"} {
		if got := compose.Services[service].Restart; got != "unless-stopped" {
			t.Errorf("long-running service %s restart=%q, want unless-stopped", service, got)
		}
	}

	for _, service := range []string{"migrate", "gateway", "worker", "summary-worker", "consumer", "delivery", "admin"} {
		config, ok := compose.Services[service]
		if !ok {
			t.Errorf("compose manifest has no %s service", service)
			continue
		}
		if !config.ReadOnly {
			t.Errorf("service %s root filesystem is writable", service)
		}
		if len(config.CapDrop) != 1 || !strings.EqualFold(config.CapDrop[0], "ALL") {
			t.Errorf("service %s cap_drop=%v, want ALL", service, config.CapDrop)
		}
		if len(config.SecurityOpt) != 1 || config.SecurityOpt[0] != "no-new-privileges:true" {
			t.Errorf("service %s security_opt=%v, want no-new-privileges", service, config.SecurityOpt)
		}
		if len(config.Tmpfs) != 1 || config.Tmpfs[0] != "/tmp:size=64m,mode=1777" {
			t.Errorf("service %s tmpfs=%v, want bounded /tmp", service, config.Tmpfs)
		}
	}
}

func TestComposeWorkerUsesProfileAndExecutionLeaseContracts(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	worker, ok := compose.Services["worker"]
	if !ok {
		t.Fatal("compose manifest has no worker service")
	}
	for _, name := range []string{
		"STORAGE_BACKEND_PROFILES",
		"DATA_PLANE_PROFILES",
		"TENANT_POSTGRES_DSN",
		"TENANT_REDIS_URL",
		"EXECUTION_LEASE_TTL",
		"EXECUTION_HEARTBEAT_INTERVAL",
	} {
		if _, ok := worker.Environment[name]; !ok {
			t.Errorf("worker environment is missing %s", name)
		}
	}
	if _, stale := worker.Environment["EXECUTION_STALE_AFTER"]; stale {
		t.Error("worker still exposes the removed EXECUTION_STALE_AFTER setting")
	}
}

func TestShellScriptsUsePortableUnixEncoding(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "scripts", "*.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no shell entrypoints found")
	}
	for _, filename := range files {
		t.Run(filepath.Base(filename), func(t *testing.T) {
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.ContainsRune(data, '\r') {
				t.Error("shell entrypoint contains CR bytes; Linux requires LF line endings")
			}
			if !bytes.HasPrefix(data, []byte("#!/usr/bin/env bash\n")) {
				t.Error("shell entrypoint must start with an LF-terminated shebang without a BOM")
			}
		})
	}
}

func TestComposeSharesAuditIdentityKeyOnlyWithGatewayAndWorker(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"gateway", "worker"} {
		service, ok := compose.Services[name]
		if !ok {
			t.Fatalf("compose manifest has no %s service", name)
		}
		if got := service.Environment["AUDIT_IDENTITY_HMAC_KEY"]; got != "${AUDIT_IDENTITY_HMAC_KEY:-}" {
			t.Errorf("%s audit identity key binding=%q, want shared operator configuration", name, got)
		}
	}
	for name, service := range compose.Services {
		if name == "gateway" || name == "worker" {
			continue
		}
		if _, found := service.Environment["AUDIT_IDENTITY_HMAC_KEY"]; found {
			t.Errorf("non-audit service %s receives the identity HMAC key", name)
		}
	}
}

func TestComposeDistributesPublicDataPlaneProfilesAndRestrictsCredentials(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	profileConsumers := []string{"gateway", "worker", "summary-worker", "consumer", "delivery", "admin", "data-migrate"}
	credentialNames := []string{
		"DATA_PLANE_QDRANT_API_KEY",
		"DATA_PLANE_EMBEDDING_API_KEY",
		"DATA_PLANE_S3_ACCESS_KEY",
		"DATA_PLANE_S3_SECRET_KEY",
	}
	for _, service := range profileConsumers {
		config, ok := compose.Services[service]
		if !ok {
			t.Errorf("compose manifest has no %s service", service)
			continue
		}
		if _, ok := config.Environment["DATA_PLANE_PROFILES"]; !ok {
			t.Errorf("service %s is missing public DATA_PLANE_PROFILES", service)
		}
		for _, name := range credentialNames {
			_, present := config.Environment[name]
			isDataWriter := service == "worker" || service == "data-migrate"
			if isDataWriter && !present {
				t.Errorf("data writer %s is missing runtime data-plane credential %s", service, name)
			}
			if !isDataWriter && present {
				t.Errorf("non-data writer service %s receives runtime data-plane credential %s", service, name)
			}
		}
	}

	environmentExample, err := os.ReadFile(".env.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range append([]string{"DATA_PLANE_PROFILES=[]"}, credentialNames...) {
		if !strings.Contains(string(environmentExample), required+"=") && required != "DATA_PLANE_PROFILES=[]" {
			t.Errorf(".env.example is missing %s", required)
		}
		if required == "DATA_PLANE_PROFILES=[]" && !strings.Contains(string(environmentExample), required) {
			t.Errorf(".env.example is missing %s", required)
		}
	}
}

func TestComposeSummaryWorkerHasDurableRuntimeContracts(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment     map[string]interface{} `yaml:"environment"`
			StopGracePeriod string                 `yaml:"stop_grace_period"`
			Healthcheck     map[string]interface{} `yaml:"healthcheck"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	service, ok := compose.Services["summary-worker"]
	if !ok {
		t.Fatal("compose manifest has no summary-worker service")
	}
	for _, name := range []string{
		"DATABASE_URL", "REDIS_URL", "STORAGE_BACKEND_PROFILES", "DATA_PLANE_PROFILES",
		"TENANT_POSTGRES_DSN", "TENANT_REDIS_URL", "MASTER_KEY",
		"SUMMARY_CONCURRENCY", "SUMMARY_JOB_TIMEOUT", "SUMMARY_LEASE_TTL",
		"SUMMARY_MIN_EVENTS", "SHUTDOWN_TIMEOUT", "METRICS_AUTH_TOKEN",
	} {
		if _, ok := service.Environment[name]; !ok {
			t.Errorf("summary-worker environment is missing %s", name)
		}
	}
	if service.StopGracePeriod != "360s" {
		t.Errorf("summary-worker stop_grace_period=%q, want 360s", service.StopGracePeriod)
	}
	if service.Healthcheck == nil {
		t.Error("summary-worker has no health check")
	}
}

func TestComposeConsumerDeclaresDevelopmentWorkerTransport(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	consumer, ok := compose.Services["consumer"]
	if !ok {
		t.Fatal("compose manifest has no consumer service")
	}
	if got := consumer.Environment["WORKER_TRANSPORT_MODE"]; got != "development" {
		t.Fatalf("Compose consumer WORKER_TRANSPORT_MODE=%v, want explicit development", got)
	}
}

func TestComposeConsumerWorkerTimeoutBudgetIsCoherent(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	workerService, workerOK := compose.Services["worker"]
	consumer, consumerOK := compose.Services["consumer"]
	if !workerOK || !consumerOK {
		t.Fatal("compose manifest must contain Worker and Consumer")
	}
	workerTimeout := fmt.Sprint(workerService.Environment["EXECUTION_TIMEOUT"])
	expectedTimeout := fmt.Sprint(consumer.Environment["WORKER_EXECUTION_TIMEOUT"])
	if workerTimeout != expectedTimeout || workerTimeout != "${EXECUTION_TIMEOUT:-90s}" {
		t.Fatalf("Worker timeout=%q Consumer expectation=%q, want one shared Compose value", workerTimeout, expectedTimeout)
	}
	if got := fmt.Sprint(consumer.Environment["PROCESS_TIMEOUT"]); got != "${PROCESS_TIMEOUT:-150s}" {
		t.Fatalf("Consumer PROCESS_TIMEOUT=%q, want default 150s", got)
	}
	if 150*time.Second < 90*time.Second+30*time.Second+5*time.Second {
		t.Fatal("Compose timeout constants do not leave response and persistence margin")
	}
	environmentExample, err := os.ReadFile(".env.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"EXECUTION_TIMEOUT=90s", "PROCESS_TIMEOUT=150s"} {
		if !strings.Contains(string(environmentExample), required) {
			t.Errorf(".env.example is missing %q", required)
		}
	}
}

func TestComposeOnlyDataPlaneWorkersReceiveTenantStorageConnections(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	for service, config := range compose.Services {
		_, postgres := config.Environment["TENANT_POSTGRES_DSN"]
		_, redis := config.Environment["TENANT_REDIS_URL"]
		if service == "worker" || service == "summary-worker" || service == "data-migrate" {
			if !postgres || !redis {
				t.Errorf("worker must receive both tenant storage connection variables")
			}
			continue
		}
		if postgres || redis {
			t.Errorf("non-data-plane service %s must not receive tenant storage connection variables", service)
		}
	}
}

func TestComposeKeepsUnauthenticatedRedisOnInternalNetwork(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Ports []string `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	redis, ok := compose.Services["redis"]
	if !ok {
		t.Fatal("compose manifest has no redis service")
	}
	if len(redis.Ports) != 0 {
		t.Fatalf("redis must not publish unauthenticated host ports: %v", redis.Ports)
	}
}

func TestPrometheusScrapesMetricsWithBearerTokenFile(t *testing.T) {
	data, err := os.ReadFile("prometheus.yml")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		ScrapeConfigs []struct {
			JobName         string `yaml:"job_name"`
			BearerTokenFile string `yaml:"bearer_token_file"`
		} `yaml:"scrape_configs"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.ScrapeConfigs) != 6 {
		t.Fatalf("Prometheus scrape config count = %d, want 6", len(config.ScrapeConfigs))
	}
	foundSummary := false
	for _, scrape := range config.ScrapeConfigs {
		if scrape.BearerTokenFile != "/tmp/metrics-token" {
			t.Errorf("scrape job %q bearer token file = %q, want /tmp/metrics-token", scrape.JobName, scrape.BearerTokenFile)
		}
		if scrape.JobName == "summary-worker" {
			foundSummary = true
		}
	}
	if !foundSummary {
		t.Error("Prometheus does not scrape summary-worker")
	}
}

func TestComposePrometheusReceivesMetricsToken(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	prometheus, ok := compose.Services["prometheus"]
	if !ok {
		t.Fatal("compose manifest has no prometheus service")
	}
	if _, ok := prometheus.Environment["METRICS_AUTH_TOKEN"]; !ok {
		t.Fatal("Prometheus must receive METRICS_AUTH_TOKEN so authenticated scrapes continue to work")
	}
	dataString := string(data)
	for _, required := range []string{
		`entrypoint: ["/bin/sh", "-c"]`,
		`printf '%s' "$${METRICS_AUTH_TOKEN:-}" > /tmp/metrics-token`,
		`exec /bin/prometheus`,
	} {
		if !strings.Contains(dataString, required) {
			t.Errorf("Prometheus startup is missing %q", required)
		}
	}
}

func TestComposeScopesMCPProfilesAndCredentials(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	for name, service := range compose.Services {
		_, profiles := service.Environment["MCP_PROFILES"]
		_, credential := service.Environment["TRPC_SECRET_MCP_SUPPORT_AUTH"]
		switch name {
		case "admin":
			if !profiles {
				t.Error("Admin is missing MCP_PROFILES for admission validation")
			}
			if credential {
				t.Error("Admin must not receive Worker-only MCP credentials")
			}
		case "worker":
			if !profiles || !credential {
				t.Error("Worker must receive MCP_PROFILES and its referenced credential")
			}
		default:
			if profiles || credential {
				t.Errorf("Service %s unexpectedly receives MCP profile data or credentials", name)
			}
		}
	}
}

func TestComposeScopesProviderCredentials(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]interface{} `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	channelVariables := []string{
		"TRPC_SECRET_WECOM_TOKEN", "TRPC_SECRET_WECOM_CORP_SECRET", "TRPC_SECRET_WECOM_AES",
		"TRPC_SECRET_TELEGRAM_BOT_TOKEN", "TRPC_SECRET_TELEGRAM_WEBHOOK",
	}
	for name, service := range compose.Services {
		for _, variable := range channelVariables {
			_, found := service.Environment[variable]
			allowed := name == "gateway" || name == "delivery"
			if found != allowed {
				t.Errorf("service %s channel credential %s present=%v, want %v", name, variable, found, allowed)
			}
		}
		_, foundModel := service.Environment["TRPC_SECRET_OPENAI_API_KEY"]
		allowedModel := name == "worker" || name == "summary-worker"
		if foundModel != allowedModel {
			t.Errorf("service %s model credential present=%v, want %v", name, foundModel, allowedModel)
		}
	}
}

func TestProductionDockerfilesPinSupportedBuildAndRuntimeImages(t *testing.T) {
	const (
		builder = "golang:1.26.7-alpine3.24@sha256:28d89ee9cc0ff9fec75c82ca201e6bf7fdf9a679d4b7b24dfa04f2bb766bb468"
		runtime = "alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b"
	)
	files, err := filepath.Glob("Dockerfile.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 10 {
		t.Fatalf("production Dockerfile count = %d, want 10", len(files))
	}
	for _, filename := range files {
		t.Run(filename, func(t *testing.T) {
			data, err := os.ReadFile(filename)
			if err != nil {
				t.Fatal(err)
			}
			contents := string(data)
			if !strings.Contains(contents, "ARG GO_IMAGE="+builder) {
				t.Errorf("builder image is not pinned to the supported release")
			}
			if !strings.Contains(contents, "ARG RUNTIME_IMAGE="+runtime) {
				t.Errorf("runtime image is not pinned to the supported release")
			}
			if strings.Contains(contents, "golang:1.21") || strings.Contains(contents, "alpine:3.20") {
				t.Errorf("unsupported production image remains in Dockerfile")
			}
		})
	}
}

func TestCIPinsActionsAndSeparatesToolchainContracts(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", ".github", "workflows", "verify.yml"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(data)
	for _, required := range []string{
		"actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
		"actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e",
		"actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
		"actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
		"go-version: \"1.26.7\"",
		"TEST_REDIS_URL: redis://localhost:6379/0",
		"bash ./scripts/require_secure_go.sh",
		"go-1-25-compatibility:",
		"go-version: \"1.25.14\"",
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("CI workflow is missing %q", required)
		}
	}
	for _, floating := range []string{"actions/checkout@v", "actions/setup-go@v", "actions/upload-artifact@v", "actions/download-artifact@v"} {
		if strings.Contains(contents, floating) {
			t.Errorf("CI workflow still uses floating action reference %q", floating)
		}
	}
}

func TestCIPackageChecksumGateIsQuietAndFailClosed(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", ".github", "workflows", "verify.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Needs           string `yaml:"needs"`
			ContinueOnError bool   `yaml:"continue-on-error"`
			Steps           []struct {
				Run             string `yaml:"run"`
				ContinueOnError bool   `yaml:"continue-on-error"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	job, ok := workflow.Jobs["linux-package"]
	if !ok || job.Needs != "windows-package" || job.ContinueOnError {
		t.Fatal("Linux package verification must gate the Windows-built archive")
	}
	var checksumGate, regressionGate bool
	for _, step := range job.Steps {
		checksum := strings.Contains(step.Run, "sha256sum --check --quiet SHA256SUMS_") &&
			regexp.MustCompile(`sha256sum --check --quiet SHA256SUMS_[0-9]{8}\.txt`).MatchString(step.Run)
		regression := strings.Contains(step.Run, "./scripts/test_checksum_output.sh")
		if (checksum || regression) && (step.ContinueOnError || strings.Contains(step.Run, "|| true")) {
			t.Error("package checksum gates must propagate verification failures")
		}
		checksumGate = checksumGate || checksum
		regressionGate = regressionGate || regression
	}
	if !checksumGate || !regressionGate {
		t.Error("Linux package job must run quiet checksum validation and its corruption regression")
	}
}

func TestGatewayBurnAlertsRequireEligibleTraffic(t *testing.T) {
	data, err := os.ReadFile("prometheus-rules.yml")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(data)
	for _, window := range []string{"5m", "1h", "30m", "6h"} {
		guard := `sum(rate(agent_gateway_webhooks_total{result=~"accepted|persistence_error"}[` + window + `])) > 0`
		if !strings.Contains(contents, guard) {
			t.Errorf("gateway burn alert lacks a zero-traffic guard for %s", window)
		}
	}
}

func TestQueueInspectionAlertContract(t *testing.T) {
	data, err := os.ReadFile("prometheus-rules.yml")
	if err != nil {
		t.Fatal(err)
	}
	rules := string(data)
	for _, required := range []string{
		"alert: AgentQueueInspectionFailing",
		"increase(agent_pipeline_queue_inspection_failures_total[10m]) > 0",
		"for: 5m",
		"severity: ticket",
		"runbook_url: docs/SLO.md#queue-inspection",
	} {
		if !strings.Contains(rules, required) {
			t.Errorf("queue inspection alert is missing %q", required)
		}
	}
}

func TestQueueAdmissionAlertContract(t *testing.T) {
	data, err := os.ReadFile("prometheus-rules.yml")
	if err != nil {
		t.Fatal(err)
	}
	rules := string(data)
	for _, required := range []string{
		"alert: AgentTenantQueueAdmissionRejecting",
		"agent_gateway_webhooks_total{result=\"queue_full\"}",
		"for: 5m",
		"severity: ticket",
		"runbook_url: docs/SLO.md#queue-admission",
	} {
		if !strings.Contains(rules, required) {
			t.Errorf("queue admission alert is missing %q", required)
		}
	}
}

func TestSummaryWorkerAlertContracts(t *testing.T) {
	data, err := os.ReadFile("prometheus-rules.yml")
	if err != nil {
		t.Fatal(err)
	}
	rules := string(data)
	for _, required := range []string{
		"alert: AgentSummaryFailuresBurst",
		`sum(increase(agent_summary_runs_total{result="failed"}[10m])) >= 5`,
		"alert: AgentSummaryFailureRatioHigh",
		`sum(increase(agent_summary_runs_total[10m])) >= 10`,
		"alert: AgentSummaryLatencyHigh",
		`histogram_quantile(0.95, sum by (le) (rate(agent_summary_run_duration_seconds_bucket[10m]))) > 60`,
		"runbook_url: docs/SLO.md#summary-generation",
	} {
		if !strings.Contains(rules, required) {
			t.Errorf("Summary alert contract is missing %q", required)
		}
	}
	slo, err := os.ReadFile(filepath.Join("..", "docs", "SLO.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, summarySection, found := strings.Cut(strings.ReplaceAll(string(slo), "\r\n", "\n"), "<a id=\"summary-generation\"></a>\n")
	summarySection = strings.TrimLeft(summarySection, "\n")
	if !found || !strings.HasPrefix(summarySection, "### ") {
		t.Fatal("SLO runbook is missing the linked Summary generation section")
	}
	summarySection, _, _ = strings.Cut(summarySection, "\n### ")
	for _, required := range []string{
		"agent_summary_runs_total",
		"agent_summary_run_duration_seconds",
		"summary_jobs",
		"summary_checkpoints.max_event_sequence",
		"SUMMARY_CONCURRENCY",
		"lease/retry",
	} {
		if !strings.Contains(summarySection, required) {
			t.Errorf("Summary recovery guidance is missing %q", required)
		}
	}
}

func TestValidationRunsRealRuntimeDataPlaneBackends(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "scripts", "validate.sh"))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(data)
	for _, required := range []string{
		"qdrant_container=",
		"minio_container=",
		"qdrant/qdrant:v1.16.3@sha256:0425e3e03e7fd9b3dc95c4214546afe19de2eb2e28ca621441a56663ac6e1f46",
		"minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e",
		"TEST_QDRANT_HOST=",
		"TEST_QDRANT_GRPC_PORT=",
		"TEST_MINIO_ENDPOINT=",
		"TEST_MINIO_ACCESS_KEY=",
		"TEST_MINIO_SECRET_KEY=",
		"/readyz",
		"/minio/health/ready",
		"real PostgreSQL, Redis, Qdrant and MinIO integration",
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("validation script is missing %q", required)
		}
	}

	staticData, err := os.ReadFile(filepath.Join("..", "scripts", "static_verify.sh"))
	if err != nil {
		t.Fatal(err)
	}
	staticContents := string(staticData)
	for _, required := range []string{
		"summary-worker",
		"runtime-data-plane-config.yaml",
		"worker-runtime-data-plane-egress",
		"DATA_PLANE_PROFILES",
	} {
		if !strings.Contains(staticContents, required) {
			t.Errorf("static verification is missing %q", required)
		}
	}
}

func TestDockerBuildContextExcludesLocalEnvironmentFiles(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	patterns := make(map[string]bool)
	for _, line := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			patterns[line] = true
		}
	}
	for _, required := range []string{".env", ".env.*", "**/.env", "**/.env.*"} {
		if !patterns[required] {
			t.Errorf(".dockerignore is missing credential-file pattern %q", required)
		}
	}
}

func TestWeComSandboxScriptsProtectCredentialsAndPinModelSecretRef(t *testing.T) {
	readScript := func(name string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "scripts", name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}

	for _, name := range []string{
		"wecom_sandbox_setup.ps1",
		"wecom_sandbox_tunnel.ps1",
		"wecom_sandbox_bootstrap.ps1",
	} {
		contents := readScript(name)
		if !strings.Contains(contents, "function Protect-LocalEnvFile") ||
			!strings.Contains(contents, "Protect-LocalEnvFile $temporary") {
			t.Errorf("%s does not restrict its temporary credential file before replacement", name)
		}
	}

	bootstrap := readScript("wecom_sandbox_bootstrap.ps1")
	const modelSecretRef = "apiKeyRef = 'env://TRPC_SECRET_OPENAI_API_KEY'"
	if count := strings.Count(bootstrap, modelSecretRef); count != 2 {
		t.Errorf("bootstrap model secret reference count=%d, want tenant and immutable version snapshots", count)
	}
	requiredStart := strings.Index(bootstrap, "$required = @(")
	if requiredStart < 0 {
		t.Fatal("could not locate bootstrap required-value block")
	}
	requiredEnd := strings.Index(bootstrap[requiredStart:], ")\r\nforeach ($key in $required)")
	if requiredEnd < 0 {
		requiredEnd = strings.Index(bootstrap[requiredStart:], ")\nforeach ($key in $required)")
	}
	if requiredEnd < 0 {
		t.Fatal("could not locate bootstrap required-value block")
	}
	requiredBlock := bootstrap[requiredStart : requiredStart+requiredEnd]
	if strings.Contains(requiredBlock, "TRPC_SECRET_OPENAI_API_KEY") {
		t.Error("bootstrap requires a model key even though its callback preflight makes zero provider calls")
	}

	preflight := readScript("external_acceptance_preflight.ps1")
	for _, required := range []string{
		"$healthBuilder.Path = '/health'",
		"Assert-Preflight ($healthStatusCode -eq 200)",
		"Assert-Preflight ($statusCode -eq 400)",
	} {
		if !strings.Contains(preflight, required) {
			t.Errorf("external callback preflight is missing strict Gateway proof %q", required)
		}
	}
}
