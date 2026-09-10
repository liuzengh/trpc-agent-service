//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestExecutionQuotaAccountsBeforeCompletionAndEnforcesHardLimit(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metricsRecorder, err := platformmetrics.New(meterProvider, platformmetrics.PricingCatalog{})
	if err != nil {
		t.Fatalf("new metrics recorder: %v", err)
	}
	store, err := platformpostgres.New(pool, platformpostgres.WithMetrics(metricsRecorder))
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantID := fmt.Sprintf("quota-%d", time.Now().UnixNano())
	appID := "support"
	config := integrationAppConfig("v1", "quota-model")
	config.TenantID, config.AppID = tenantID, appID
	config.Budget = tenant.BudgetPolicy{
		MaxTokensPerExecution: 10,
		MaxCostPerExecution:   1,
		DailyTokenQuota:       20,
		DailyCostQuota:        2,
	}
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID: tenantID, Name: tenantID, Status: tenant.StatusActive,
		Quota: tenant.QuotaPolicy{DailyTokenQuota: 30, DailyCostQuota: 3},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Support",
		ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	digest, err := auth.DigestAPIKey("tas_quota_integration_key_" + fmt.Sprint(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("digest credential: %v", err)
	}
	credential := auth.Credential{
		ID:        "credential-" + tenantID,
		TenantID:  tenantID,
		AppID:     appID,
		KeyPrefix: "tas_quot",
		Status:    auth.CredentialActive,
	}
	if err := store.CreateCredential(ctx, digest, credential); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	t.Cleanup(func() {
		cleanupQuotaFixture(t, pool, tenantID, appID)
	})

	claim := admitQuotaExecution(t, ctx, store, tenantID, appID, credential, digest, "request-with-usage")
	if err := store.RecordExecutionUsage(ctx, claim, worker.RunResult{
		InputTokens:  3,
		OutputTokens: 2,
		TotalTokens:  5,
		Cost:         float64Pointer(.4),
	}, false); err != nil {
		t.Fatalf("record usage before completion: %v", err)
	}
	assertQuotaUsage(t, ctx, pool, tenantID, appID, 5, 10, .4, 1)
	if err := store.Complete(ctx, claim, queue.CompletionSucceeded); err != nil {
		t.Fatalf("complete execution: %v", err)
	}
	assertQuotaUsage(t, ctx, pool, tenantID, appID, 5, 0, .4, 0)

	overLimit := admitQuotaExecution(t, ctx, store, tenantID, appID, credential, digest, "request-over-limit")
	if _, err := store.Admit(ctx, quotaAdmissionRequest(tenantID, appID, credential, digest, "request-admission-rejected")); !errors.Is(err, gateway.ErrAdmissionQuotaExceeded) {
		t.Fatalf("quota admission error = %v, want ErrAdmissionQuotaExceeded", err)
	}
	assertQuotaRejectionMetric(t, reader)
	if err := store.RecordExecutionUsage(ctx, overLimit, worker.RunResult{
		TotalTokens: 17,
		Cost:        float64Pointer(.1),
	}, false); !errors.Is(err, tenant.ErrQuotaExceeded) {
		t.Fatalf("over-limit usage error = %v, want ErrQuotaExceeded", err)
	}
	// The failed accounting transaction must not consume usage or destroy the
	// reservation; the terminal transition still owns cleanup.
	assertQuotaUsage(t, ctx, pool, tenantID, appID, 5, 10, .4, 1)
	if err := store.CompleteUncertain(ctx, overLimit, errors.New("provider result unknown")); err != nil {
		t.Fatalf("complete over-limit execution uncertain: %v", err)
	}
	assertQuotaUsage(t, ctx, pool, tenantID, appID, 5, 0, .4, 0)
}

func admitQuotaExecution(
	t *testing.T,
	ctx context.Context,
	store *platformpostgres.Store,
	tenantID, appID string,
	credential auth.Credential,
	digest [32]byte,
	requestID string,
) queue.Claim {
	t.Helper()
	request := quotaAdmissionRequest(tenantID, appID, credential, digest, requestID)
	result, err := store.Admit(ctx, request)
	if err != nil {
		t.Fatalf("admit %s: %v", requestID, err)
	}
	dispatches, err := store.ClaimDispatches(ctx, "quota-relay-"+requestID, time.Minute, 1)
	if err != nil || len(dispatches) != 1 {
		t.Fatalf("claim dispatch %s: dispatches=%#v err=%v", requestID, dispatches, err)
	}
	claim, found, err := store.Claim(ctx, dispatches[0], queue.ClaimRequest{
		Owner:         "quota-worker-" + requestID,
		LeaseDuration: time.Minute,
	})
	if err != nil || !found {
		t.Fatalf("claim execution %s: claim=%#v found=%v err=%v", requestID, claim, found, err)
	}
	if claim.Job.RequestID() != result.RequestID {
		t.Fatalf("claim request id = %q, admitted %q", claim.Job.RequestID(), result.RequestID)
	}
	return claim
}

func quotaAdmissionRequest(
	tenantID, appID string,
	credential auth.Credential,
	digest [32]byte,
	requestID string,
) gateway.AdmissionRequest {
	return gateway.AdmissionRequest{
		RequestID:      requestID,
		IdempotencyKey: requestID,
		Identity: gateway.AdmissionIdentity{
			Tenant: tenant.RuntimeContext{
				TenantID:           tenantID,
				AppID:              appID,
				ConfigVersion:      "v1",
				SessionPrincipalID: "principal-quota",
				SessionID:          requestID,
				UserID:             "user-quota",
				TraceID:            requestID + "-trace",
			},
			Source:           gateway.TenantSourceAuthenticatedClaims,
			SourceID:         credential.ID,
			CredentialDigest: gateway.CredentialDigest(digest),
		},
		Message: gateway.Message{Text: "quota test"},
	}
}

func assertQuotaRejectionMetric(t *testing.T, reader *sdkmetric.ManualReader) {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect quota rejection metric: %v", err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, value := range scope.Metrics {
			if value.Name != "trpc_agent_service.governance.rejected" {
				continue
			}
			points, ok := value.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("governance rejection metric type = %T", value.Data)
			}
			for _, point := range points.DataPoints {
				if errorType, ok := point.Attributes.Value("error_type"); ok &&
					errorType.AsString() == "quota_exceeded" && point.Value > 0 {
					return
				}
			}
		}
	}
	t.Fatal("quota_exceeded governance rejection metric was not recorded")
}

func assertQuotaUsage(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	tenantID, appID string,
	wantUsedTokens, wantReservedTokens int64,
	wantUsedCost, wantReservedCost float64,
) {
	t.Helper()
	var usedTokens, reservedTokens int64
	var usedCost, reservedCost float64
	if err := pool.QueryRow(ctx, `
SELECT token_used, token_reserved, cost_used, cost_reserved
FROM platform.quota_usage
WHERE tenant_id=$1 AND app_id=$2 AND scope_kind='APP' AND period_kind='DAY'`, tenantID, appID).
		Scan(&usedTokens, &reservedTokens, &usedCost, &reservedCost); err != nil {
		t.Fatalf("read app quota usage: %v", err)
	}
	if usedTokens != wantUsedTokens || reservedTokens != wantReservedTokens ||
		usedCost != wantUsedCost || reservedCost != wantReservedCost {
		t.Fatalf("quota usage = tokens %d/%d cost %v/%v, want %d/%d %v/%v",
			usedTokens, reservedTokens, usedCost, reservedCost,
			wantUsedTokens, wantReservedTokens, wantUsedCost, wantReservedCost)
	}
}

func cleanupQuotaFixture(t *testing.T, pool *pgxpool.Pool, tenantID, appID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, query := range []string{
		`DELETE FROM platform.quota_usage_record WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.quota_reservation WHERE tenant_id=$1 AND app_id IN ('', $2)`,
		`DELETE FROM platform.quota_usage WHERE tenant_id=$1 AND app_id IN ('', $2)`,
		`DELETE FROM platform.dispatch_outbox WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.execution_event WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.execution WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.session_lane WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.api_credential WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.app_config_version WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.agent_app WHERE tenant_id=$1 AND app_id=$2`,
		`DELETE FROM platform.tenant WHERE tenant_id=$1`,
	} {
		if _, err := pool.Exec(ctx, query, tenantID, appID); err != nil {
			t.Logf("cleanup quota fixture query failed: %v", err)
		}
	}
}

func float64Pointer(value float64) *float64 { return &value }
