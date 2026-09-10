// Command deployment-e2e drives the Compose Golden Path. It provisions one
// isolated scope through the control-plane service boundary, then exercises
// the real Gateway HTTP endpoint and verifies durable execution state.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/e2e"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	gatewayURLEnv   = "TRPC_AGENT_SERVICE_GATEWAY_URL"
	postgresDSNEnv  = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	adminTokenEnv   = "TRPC_AGENT_SERVICE_ADMIN_TOKEN"
	reportPathEnv   = "TRPC_DEPLOYMENT_E2E_REPORT"
	defaultGateway  = "http://127.0.0.1:8080"
	defaultAdminKey = "development-only-admin-token"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Golden Path PASSED")
}

type evidence struct {
	TenantID         string `json:"tenant_id"`
	AppID            string `json:"app_id"`
	Schema           string `json:"session_schema"`
	ReadinessStatus  int    `json:"readiness_status"`
	AdminSessionCode int    `json:"admin_session_status"`
	RequestID        string `json:"request_id"`
	ResponseStatus   int    `json:"gateway_response_status"`
	DurableStatus    string `json:"durable_status"`
	ConfigVersion    string `json:"config_version"`
	Attempt          int    `json:"attempt"`
	Dispatches       int64  `json:"dispatches"`
	Consumed         int64  `json:"consumed"`
}

func run(parent context.Context) error {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()

	gatewayURL := strings.TrimRight(strings.TrimSpace(valueOr(gatewayURLEnv, defaultGateway)), "/")
	adminToken := strings.TrimSpace(valueOr(adminTokenEnv, defaultAdminKey))
	dsn := strings.TrimSpace(os.Getenv(postgresDSNEnv))
	if dsn == "" {
		return fmt.Errorf("%s is required", postgresDSNEnv)
	}
	client := &http.Client{Timeout: 10 * time.Second}

	readinessStatus, err := getStatus(ctx, client, gatewayURL+"/readyz", "")
	if err != nil {
		return fmt.Errorf("check gateway readiness: %w", err)
	}
	if readinessStatus != http.StatusOK {
		return fmt.Errorf("gateway readiness status=%d", readinessStatus)
	}
	adminSessionStatus, err := getStatus(ctx, client, gatewayURL+"/admin/v1/session", adminToken)
	if err != nil {
		return fmt.Errorf("authenticate admin session: %w", err)
	}
	if adminSessionStatus != http.StatusOK {
		return fmt.Errorf("admin session status=%d", adminSessionStatus)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	store, err := platformpostgres.New(pool)
	if err != nil {
		return fmt.Errorf("create postgres store: %w", err)
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")
	tenantID := "deployment-e2e-" + runID
	appID := "support"
	schema := "dep_" + runID[:12]
	config := deploymentConfig(tenantID, appID, schema)
	app := tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Deployment E2E",
		ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}
	if err := postJSON(ctx, client, gatewayURL+"/admin/v1/tenants", adminToken, map[string]any{
		"tenant": tenant.Tenant{ID: tenantID, Name: "Deployment E2E", Status: tenant.StatusActive},
	}); err != nil {
		return fmt.Errorf("create tenant through admin HTTP: %w", err)
	}
	if err := postJSON(ctx, client, gatewayURL+"/admin/v1/apps", adminToken, map[string]any{
		"app": app, "initial_config": config,
	}); err != nil {
		return fmt.Errorf("create app through admin HTTP: %w", err)
	}

	// The Admin HTTP contract intentionally never returns raw API keys. The
	// service API issues the one-time key in process memory for this disposable
	// acceptance command; only the subsequent Gateway request uses it.
	issued, err := (admin.API{Repository: store}).IssueCredential(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, time.Time{})
	if err != nil {
		return fmt.Errorf("issue deployment credential: %w", err)
	}

	requestID := "deployment-e2e-" + runID
	responseStatus, responseBody, err := postGatewayRequest(ctx, client, gatewayURL, issued.APIKey, requestID)
	if err != nil {
		return err
	}
	if responseStatus != http.StatusOK {
		return fmt.Errorf("gateway request status=%d", responseStatus)
	}
	projection, err := e2e.DecodeChatCompletion(responseBody, false)
	if err != nil || projection.Kind != e2e.ProjectionFinal {
		if err == nil {
			err = errors.New("response is not a final assistant projection")
		}
		return fmt.Errorf("gateway response projection: %w", err)
	}

	final, err := waitDurableExecution(ctx, pool, tenantID, appID, requestID)
	if err != nil {
		return fmt.Errorf("wait durable deployment execution: %w", err)
	}
	if final.status != "SUCCEEDED" || final.configVersion != config.Version ||
		final.dispatches == 0 || final.dispatches != final.consumed {
		return fmt.Errorf("durable deployment execution status=%s config=%s attempts=%d dispatches=%d consumed=%d",
			final.status, final.configVersion, final.attempt, final.dispatches, final.consumed)
	}
	writeEvidence(evidence{
		TenantID: tenantID, AppID: appID, Schema: schema,
		ReadinessStatus: readinessStatus, AdminSessionCode: adminSessionStatus,
		RequestID: requestID, ResponseStatus: responseStatus,
		DurableStatus: final.status, ConfigVersion: final.configVersion,
		Attempt: final.attempt, Dispatches: final.dispatches, Consumed: final.consumed,
	})
	return nil
}

func deploymentConfig(tenantID, appID, schema string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:  tenant.ModelProviderOpenAI,
			Model:     "deployment-e2e-model",
			APIKeyRef: tenant.SecretRef{Name: "deployment-e2e-model-key", Version: "v1"},
		},
		BackendConfig: tenant.BackendConfig{
			Name: "deployment-e2e-session",
			Session: tenant.BackendRef{
				Kind: tenant.BackendSQL, Provider: "postgres", Name: "deployment-e2e-session",
				Options: map[string]string{"schema": schema},
			},
		},
	}
}

func getStatus(ctx context.Context, client *http.Client, endpoint, token string) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if _, err := e2e.ReadBody(response.Body); err != nil {
		return 0, err
	}
	return response.StatusCode, nil
}

func postJSON(ctx context.Context, client *http.Client, endpoint, token string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if _, err := e2e.ReadBody(response.Body); err != nil {
		return err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("status=%d", response.StatusCode)
	}
	return nil
}

func postGatewayRequest(ctx context.Context, client *http.Client, gatewayURL, apiKey, requestID string) (int, []byte, error) {
	body := bytes.NewBufferString(`{"model":"ignored","messages":[{"role":"user","content":"deployment golden path"}]}`)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL+"/v1/chat/completions", body)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("X-Request-ID", requestID)
	request.Header.Set("Idempotency-Key", "idem-"+requestID)
	request.Header.Set("X-Session-ID", "deployment-session")
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	responseBody, err := e2e.ReadBody(response.Body)
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, responseBody, nil
}

type durableExecution struct {
	status, configVersion string
	attempt               int
	dispatches, consumed  int64
}

func waitDurableExecution(ctx context.Context, pool *pgxpool.Pool, tenantID, appID, requestID string) (durableExecution, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, err := readDurableExecution(ctx, pool, tenantID, appID, requestID)
		if err == nil {
			if isTerminal(value.status) {
				return value, nil
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return durableExecution{}, err
		}
		select {
		case <-ctx.Done():
			return durableExecution{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func readDurableExecution(ctx context.Context, pool *pgxpool.Pool, tenantID, appID, requestID string) (durableExecution, error) {
	var value durableExecution
	err := pool.QueryRow(ctx, `
SELECT e.status, e.config_version, e.attempt, count(o.outbox_id),
       count(o.outbox_id) FILTER (WHERE o.status = 'CONSUMED')
FROM platform.execution e
LEFT JOIN platform.dispatch_outbox o
  ON o.tenant_id = e.tenant_id AND o.app_id = e.app_id AND o.request_id = e.request_id
WHERE e.tenant_id = $1 AND e.app_id = $2 AND e.request_id = $3
GROUP BY e.status, e.config_version, e.attempt`, tenantID, appID, requestID).Scan(
		&value.status, &value.configVersion, &value.attempt, &value.dispatches, &value.consumed,
	)
	return value, err
}

func isTerminal(status string) bool {
	switch status {
	case "SUCCEEDED", "FAILED", "UNCERTAIN", "CANCELED":
		return true
	default:
		return false
	}
}

func valueOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func writeEvidence(value evidence) {
	path := strings.TrimSpace(os.Getenv(reportPathEnv))
	if path == "" {
		return
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(path, encoded, 0o600)
}
