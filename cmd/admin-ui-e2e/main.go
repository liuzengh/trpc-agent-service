// Command admin-ui-e2e provisions the real control-plane state consumed by the
// Playwright Admin UI golden path. It keeps credentials in process memory and
// writes only non-sensitive identifiers to the fixture file.
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
	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformmigration "github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	gatewayURLEnv  = "TRPC_AGENT_SERVICE_GATEWAY_URL"
	postgresDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	adminTokenEnv  = "TRPC_AGENT_SERVICE_ADMIN_TOKEN"
	fixturePathEnv = "TRPC_ADMIN_UI_E2E_FIXTURE"

	defaultGateway  = "http://127.0.0.1:8080"
	defaultAdminKey = "development-only-admin-token"
)

type fixture struct {
	TenantID            string `json:"tenant_id"`
	AppID               string `json:"app_id"`
	SourceConfigVersion string `json:"source_config_version"`
	TargetConfigVersion string `json:"target_config_version"`
	PublishedConfig     string `json:"published_config_version"`
	BindingID           string `json:"binding_id"`
	ExecutionRequestID  string `json:"execution_request_id"`
	ApprovalID          string `json:"approval_id"`
	MigrationID         string `json:"migration_id"`
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
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
	if adminToken == "" {
		return fmt.Errorf("%s is required", adminTokenEnv)
	}
	client := &http.Client{Timeout: 10 * time.Second}

	if status, err := getStatus(ctx, client, gatewayURL+"/readyz", ""); err != nil {
		return fmt.Errorf("check gateway readiness: %w", err)
	} else if status != http.StatusOK {
		return fmt.Errorf("gateway readiness status=%d", status)
	}
	if status, err := getStatus(ctx, client, gatewayURL+"/admin/v1/session", adminToken); err != nil {
		return fmt.Errorf("authenticate admin session: %w", err)
	} else if status != http.StatusOK {
		return fmt.Errorf("admin session status=%d", status)
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
	tenantID := "admin-ui-e2e-" + runID
	appID := "support"
	schema := "adminui_" + runID[:12]
	v1 := adminUIConfig(tenantID, appID, "v1", tenant.BackendRef{
		Kind: tenant.BackendRedis, Provider: "redis", Name: "admin-ui-e2e-redis",
	})
	v2 := adminUIConfig(tenantID, appID, "v2", tenant.BackendRef{
		Kind: tenant.BackendSQL, Provider: "postgres", Name: "admin-ui-e2e-postgres",
		Options: map[string]string{"schema": schema},
	})
	app := tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Admin UI E2E",
		ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}
	if err := postJSON(ctx, client, gatewayURL+"/admin/v1/tenants", adminToken, map[string]any{
		"tenant": tenant.Tenant{
			ID: tenantID, Name: "Admin UI E2E", Status: tenant.StatusActive,
			Audit: tenant.AuditPolicy{
				Enabled: true, RecordToolDecisions: true, RecordExecutions: true,
				RedactPII: true,
			},
		},
	}); err != nil {
		return fmt.Errorf("create tenant through admin HTTP: %w", err)
	}
	if err := postJSON(ctx, client, gatewayURL+"/admin/v1/apps", adminToken, map[string]any{
		"app": app, "initial_config": v1,
	}); err != nil {
		return fmt.Errorf("create app through admin HTTP: %w", err)
	}
	if err := postJSON(ctx, client, gatewayURL+"/admin/v1/configs", adminToken, map[string]any{
		"config": v2,
	}); err != nil {
		return fmt.Errorf("publish migration target through admin HTTP: %w", err)
	}

	bindingID := "binding-" + runID
	if err := postJSON(ctx, client, gatewayURL+"/admin/v1/channel-bindings", adminToken, map[string]any{
		"binding": channels.Binding{
			TenantID: tenantID, AppID: appID, BindingID: bindingID,
			Channel: channels.ChannelWeCom, ExternalAccount: "admin-ui-e2e-external",
			Secret: tenant.SecretRef{Name: "admin-ui-e2e-secret", Version: "v1"},
			Status: channels.BindingActive,
		},
	}); err != nil {
		return fmt.Errorf("create channel binding through admin HTTP: %w", err)
	}

	migrationBody, err := postJSONResponse(ctx, client, gatewayURL+"/admin/v1/data-migrations", adminToken, map[string]any{
		"tenant_id": tenantID, "app_id": appID, "domain": string(platformmigration.DomainSession),
		"source_config_version": v1.Version, "target_config_version": v2.Version,
	})
	if err != nil {
		return fmt.Errorf("create migration through admin HTTP: %w", err)
	}
	var dataMigration platformmigration.Record
	if err := json.Unmarshal(migrationBody, &dataMigration); err != nil {
		return fmt.Errorf("decode migration response: %w", err)
	}

	issued, err := (admin.API{Repository: store}).IssueCredential(
		ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, time.Time{},
	)
	if err != nil {
		return fmt.Errorf("issue data-plane credential: %w", err)
	}
	requestID := "admin-ui-e2e-" + runID
	// Approval keeps the original durable HTTP request open until its
	// continuation completes. The fixture only needs the admission and pending
	// approval record, so submit it asynchronously and cancel the disposable
	// client request after the durable state reaches WAITING_APPROVAL.
	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	responseCh := make(chan struct {
		status int
		body   []byte
		err    error
	}, 1)
	go func() {
		status, body, requestErr := postGatewayRequest(requestCtx, client, gatewayURL, issued.APIKey, requestID)
		responseCh <- struct {
			status int
			body   []byte
			err    error
		}{status: status, body: body, err: requestErr}
	}()
	if err := waitExecutionStatus(ctx, pool, tenantID, appID, requestID, "WAITING_APPROVAL"); err != nil {
		return fmt.Errorf("wait approval execution: %w", err)
	}
	cancelRequest()
	select {
	case response := <-responseCh:
		if response.err != nil && !errors.Is(response.err, context.Canceled) {
			return fmt.Errorf("submit approval execution: %w", response.err)
		}
		if response.err == nil {
			if response.status >= http.StatusOK && response.status < http.StatusMultipleChoices {
				projection, projectionErr := e2e.DecodeChatCompletion(response.body, true)
				if projectionErr != nil || projection.Kind != e2e.ProjectionToolCall {
					if projectionErr == nil {
						projectionErr = errors.New("response is not an approval tool-call projection")
					}
					return fmt.Errorf("gateway approval response projection: %w", projectionErr)
				}
			} else if response.status == http.StatusInternalServerError {
				if err := e2e.DecodeOpenAIError(response.body); err != nil {
					return fmt.Errorf("gateway approval failure response: %w", err)
				}
			} else {
				return fmt.Errorf("gateway approval request status=%d", response.status)
			}
		}
	case <-time.After(time.Second):
		return errors.New("approval request did not stop after durable approval wait")
	}
	approvalID, err := waitApproval(ctx, store, tenantID, appID, requestID)
	if err != nil {
		return fmt.Errorf("wait approval record: %w", err)
	}

	return writeFixture(fixture{
		TenantID: tenantID, AppID: appID,
		SourceConfigVersion: v1.Version, TargetConfigVersion: v2.Version,
		PublishedConfig: "v3", BindingID: bindingID,
		ExecutionRequestID: requestID, ApprovalID: approvalID,
		MigrationID: dataMigration.ID,
	})
}

func adminUIConfig(tenantID, appID, version string, session tenant.BackendRef) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:  tenant.ModelProviderOpenAI,
			Model:     "admin-ui-e2e-model",
			APIKeyRef: tenant.SecretRef{Name: "admin-ui-e2e-model-key", Version: "v1"},
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:        []string{"todo_write"},
			ExecutableTools:     []string{"todo_write"},
			ReviewRequiredTools: []string{"todo_write"},
		},
		BackendConfig: tenant.BackendConfig{
			Name:    "admin-ui-e2e-backends",
			Session: session,
		},
		Audit: tenant.AuditPolicy{
			Enabled: true, RecordToolDecisions: true, RecordExecutions: true,
			RedactPII: true,
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
	_, err := postJSONResponse(ctx, client, endpoint, token, value)
	return err
}

func postJSONResponse(ctx context.Context, client *http.Client, endpoint, token string, value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, readErr := e2e.ReadBody(response.Body)
	if readErr != nil {
		return nil, readErr
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("status=%d", response.StatusCode)
	}
	return body, nil
}

func postGatewayRequest(ctx context.Context, client *http.Client, gatewayURL, apiKey, requestID string) (int, []byte, error) {
	body := bytes.NewBufferString(`{"model":"ignored","messages":[{"role":"user","content":"admin ui approval"}]}`)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL+"/v1/chat/completions", body)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("X-Request-ID", requestID)
	request.Header.Set("Idempotency-Key", "idem-"+requestID)
	request.Header.Set("X-Session-ID", "admin-ui-session")
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

func waitExecutionStatus(ctx context.Context, pool *pgxpool.Pool, tenantID, appID, requestID, wanted string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status string
		err := pool.QueryRow(ctx, `
SELECT status
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`, tenantID, appID, requestID).Scan(&status)
		if err == nil {
			if status == wanted {
				return nil
			}
			if status == "FAILED" || status == "UNCERTAIN" || status == "CANCELED" || status == "SUCCEEDED" {
				return fmt.Errorf("status=%s, want=%s", status, wanted)
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitApproval(ctx context.Context, store *platformpostgres.Store, tenantID, appID, requestID string) (string, error) {
	api := admin.API{Repository: store}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		approvals, err := api.ListApprovalsForPrincipal(ctx, admin.AdminPrincipal{
			Role: admin.RoleSystemAdmin, ActorID: "admin-ui-e2e",
		}, platformapproval.Query{TenantID: tenantID, AppID: appID, Limit: 100})
		if err != nil {
			return "", err
		}
		for _, approval := range approvals {
			if approval.RequestID == requestID && approval.Status == platformapproval.StatusPending {
				return approval.ApprovalID, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

func writeFixture(value fixture) error {
	path := strings.TrimSpace(os.Getenv(fixturePathEnv))
	if path == "" {
		return fmt.Errorf("%s is required", fixturePathEnv)
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o600)
}

func valueOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
