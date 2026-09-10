//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestChannelAdmissionPersistsInboxExecutionAndDispatchAtomically(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	request := newIM05Request(t, p.route, p.binding, "message-first", "request-first", channels.MessageTypeText, "hello")

	first, err := p.store.Admit(p.ctx, request)
	if err != nil {
		t.Fatalf("admit first channel message: %v", err)
	}
	if first.Status != gateway.AdmissionStatusAdmitted || first.Replayed || first.TurnSeq != 1 || first.ConfigVersion != "v1" {
		t.Fatalf("first channel admission result = %#v", first)
	}

	duplicate := request
	duplicate.RequestID = "request-duplicate"
	replayed, err := p.store.Admit(p.ctx, duplicate)
	if err != nil {
		t.Fatalf("admit duplicate channel message: %v", err)
	}
	if !replayed.Replayed || replayed.RequestID != first.RequestID || replayed.Status != gateway.AdmissionStatusAdmitted {
		t.Fatalf("duplicate channel admission result = %#v", replayed)
	}

	inboxCount, executionCount, dispatchCount, identityCount := im05Counts(t, p)
	if inboxCount != 1 || executionCount != 1 || dispatchCount != 1 || identityCount != 1 {
		t.Fatalf("channel admission counts = inbox:%d execution:%d dispatch:%d identity:%d", inboxCount, executionCount, dispatchCount, identityCount)
	}

	var userID, principalID, sessionID string
	if err := p.pool.QueryRow(p.ctx, `
SELECT user_id, session_principal_id, session_id
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, first.RequestID,
	).Scan(&userID, &principalID, &sessionID); err != nil {
		t.Fatalf("read admitted execution mapping: %v", err)
	}
	if userID == "" || principalID != userID || sessionID != channels.DefaultSessionID {
		t.Fatalf("admitted execution mapping = user:%q principal:%q session:%q", userID, principalID, sessionID)
	}
}

func TestConfigRollbackPinsAlreadyAdmittedExecutions(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	stable, err := p.store.ResolveAppConfig(p.ctx, p.scope.TenantID, p.scope.AppID, "v1")
	if err != nil {
		t.Fatalf("resolve stable config: %v", err)
	}
	canary := stable
	canary.Version = "v2"
	canary.Model.Model = "rollback-canary-model"
	if err := p.store.InsertAppConfigVersion(p.ctx, canary); err != nil {
		t.Fatalf("insert canary config: %v", err)
	}
	if err := p.store.ActivateAppConfig(p.ctx, p.scope.TenantID, p.scope.AppID, canary.Version); err != nil {
		t.Fatalf("activate canary config: %v", err)
	}

	first, err := p.store.Admit(p.ctx, newIM05Request(t, p.route, p.binding, "message-canary", "request-canary", channels.MessageTypeText, "canary"))
	if err != nil {
		t.Fatalf("admit canary execution: %v", err)
	}
	if first.ConfigVersion != canary.Version {
		t.Fatalf("canary admission config = %q, want %q", first.ConfigVersion, canary.Version)
	}

	if err := p.store.ActivateAppConfig(p.ctx, p.scope.TenantID, p.scope.AppID, stable.Version); err != nil {
		t.Fatalf("roll back active config: %v", err)
	}
	second, err := p.store.Admit(p.ctx, newIM05Request(t, p.route, p.binding, "message-stable", "request-stable", channels.MessageTypeText, "stable"))
	if err != nil {
		t.Fatalf("admit rolled-back execution: %v", err)
	}
	if second.ConfigVersion != stable.Version {
		t.Fatalf("rolled-back admission config = %q, want %q", second.ConfigVersion, stable.Version)
	}

	versions := map[string]string{}
	rows, err := p.pool.Query(p.ctx, `
SELECT request_id, config_version
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = ANY($3)`,
		p.scope.TenantID, p.scope.AppID, []string{first.RequestID, second.RequestID})
	if err != nil {
		t.Fatalf("read execution config versions: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var requestID, version string
		if err := rows.Scan(&requestID, &version); err != nil {
			t.Fatalf("scan execution config version: %v", err)
		}
		versions[requestID] = version
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate execution config versions: %v", err)
	}
	if versions[first.RequestID] != canary.Version || versions[second.RequestID] != stable.Version {
		t.Fatalf("pinned execution config versions = %#v", versions)
	}
}

func TestConfigCanaryEnablePausePromotePinsAdmissionVersion(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	stable, err := p.store.ResolveAppConfig(p.ctx, p.scope.TenantID, p.scope.AppID, "v1")
	if err != nil {
		t.Fatalf("resolve stable config: %v", err)
	}
	canary := stable
	canary.Version = "v2"
	canary.Model.Model = "canary-model"
	if err := p.store.InsertAppConfigVersion(p.ctx, canary); err != nil {
		t.Fatalf("insert canary config: %v", err)
	}
	app, err := p.store.EnableAppCanary(p.ctx, p.scope.TenantID, p.scope.AppID, canary.Version, 100)
	if err != nil {
		t.Fatalf("enable app canary: %v", err)
	}
	if app.CanaryConfigVersion != canary.Version || app.CanaryPercentage != 100 || app.CanaryStatus != tenant.CanaryEnabled {
		t.Fatalf("enabled app canary = %#v", app)
	}

	first, err := p.store.Admit(p.ctx, newIM05Request(t, p.route, p.binding, "message-canary-v2", "request-canary-v2", channels.MessageTypeText, "canary"))
	if err != nil {
		t.Fatalf("admit canary execution: %v", err)
	}
	if first.ConfigVersion != canary.Version {
		t.Fatalf("canary admission config = %q, want %q", first.ConfigVersion, canary.Version)
	}

	app, err = p.store.PauseAppCanary(p.ctx, p.scope.TenantID, p.scope.AppID)
	if err != nil {
		t.Fatalf("pause app canary: %v", err)
	}
	if app.CanaryStatus != tenant.CanaryPaused {
		t.Fatalf("paused app canary = %#v", app)
	}
	second, err := p.store.Admit(p.ctx, newIM05Request(t, p.route, p.binding, "message-canary-paused", "request-canary-paused", channels.MessageTypeText, "paused"))
	if err != nil {
		t.Fatalf("admit paused canary execution: %v", err)
	}
	if second.ConfigVersion != stable.Version {
		t.Fatalf("paused admission config = %q, want %q", second.ConfigVersion, stable.Version)
	}

	app, err = p.store.EnableAppCanary(p.ctx, p.scope.TenantID, p.scope.AppID, canary.Version, 100)
	if err != nil {
		t.Fatalf("re-enable app canary: %v", err)
	}
	app, err = p.store.PromoteAppCanary(p.ctx, p.scope.TenantID, p.scope.AppID)
	if err != nil {
		t.Fatalf("promote app canary: %v", err)
	}
	if app.ActiveConfigVersion != canary.Version || app.CanaryConfigVersion != "" || app.CanaryStatus != tenant.CanaryDisabled {
		t.Fatalf("promoted app = %#v", app)
	}
	third, err := p.store.Admit(p.ctx, newIM05Request(t, p.route, p.binding, "message-after-promote", "request-after-promote", channels.MessageTypeText, "promoted"))
	if err != nil {
		t.Fatalf("admit promoted execution: %v", err)
	}
	if third.ConfigVersion != canary.Version {
		t.Fatalf("promoted admission config = %q, want %q", third.ConfigVersion, canary.Version)
	}
}

func TestChannelAdmissionSealsMessageReplyTargetInInbox(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	request := newIM05Request(t, p.route, p.binding, "message-target", "request-target", channels.MessageTypeText, "hello")
	targetInput, err := channels.WithMessageReplyTarget(*request.ChannelInput, channels.MessageReplyTarget{
		ProviderTarget: "provider-message-target",
		ExpiresAt:      time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("attach message reply target: %v", err)
	}
	request.ChannelInput = &targetInput

	result, err := p.store.Admit(p.ctx, request)
	if err != nil {
		t.Fatalf("admit channel message with reply target: %v", err)
	}

	var targetJSON []byte
	var expiresAt time.Time
	var command []byte
	if err := p.pool.QueryRow(p.ctx, `
SELECT provider_reply_target_envelope, reply_target_expires_at, e.command
FROM platform.channel_inbox i
JOIN platform.execution e
  ON e.tenant_id = i.tenant_id
 AND e.app_id = i.app_id
 AND e.request_id = i.request_id
WHERE i.tenant_id = $1 AND i.app_id = $2 AND i.binding_id = $3
  AND i.external_message_id = $4`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, "message-target").Scan(&targetJSON, &expiresAt, &command); err != nil {
		t.Fatalf("read channel reply target: %v", err)
	}
	var envelope channels.TargetEnvelope
	if err := json.Unmarshal(targetJSON, &envelope); err != nil {
		t.Fatalf("decode channel reply target envelope: %v", err)
	}
	protector := newIntegrationTargetProtector(t, "v1")
	opened, err := protector.Open(p.ctx, channels.TargetContext{
		Scope:            p.scope,
		BindingID:        p.binding.BindingID,
		Channel:          p.binding.Channel,
		EntityType:       channels.TargetEntityInbox,
		InternalEntityID: result.RequestID,
	}, channels.TargetPurposeReplyMessage, envelope)
	if err != nil {
		t.Fatalf("open channel reply target: %v", err)
	}
	if opened.ProviderTarget != "provider-message-target" || !expiresAt.After(time.Now().UTC()) {
		t.Fatalf("stored channel reply target = %#v, expires_at=%s", opened, expiresAt)
	}
	if strings.Contains(string(command), "provider-message-target") {
		t.Fatal("provider reply target leaked into execution command")
	}
}

func TestChannelAdmissionDerivesIdempotencyFromExternalMessageID(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	first := newIM05Request(t, p.route, p.binding, "message-idempotency-one", "request-idempotency-one", channels.MessageTypeText, "one")
	second := newIM05Request(t, p.route, p.binding, "message-idempotency-two", "request-idempotency-two", channels.MessageTypeText, "two")
	first.IdempotencyKey = "caller-reused-key"
	second.IdempotencyKey = first.IdempotencyKey
	if _, err := p.store.Admit(p.ctx, first); err != nil {
		t.Fatalf("admit first reused-key message: %v", err)
	}
	if _, err := p.store.Admit(p.ctx, second); err != nil {
		t.Fatalf("admit second reused-key message: %v", err)
	}
	inboxCount, executionCount, dispatchCount, _ := im05Counts(t, p)
	if inboxCount != 2 || executionCount != 2 || dispatchCount != 2 {
		t.Fatalf("reused-key channel counts = inbox:%d execution:%d dispatch:%d", inboxCount, executionCount, dispatchCount)
	}
	var orphanCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*)
FROM platform.channel_inbox i
LEFT JOIN platform.execution e
  ON e.tenant_id = i.tenant_id
 AND e.app_id = i.app_id
 AND e.request_id = i.request_id
WHERE i.tenant_id = $1 AND i.app_id = $2 AND i.binding_id = $3
  AND e.request_id IS NULL`, p.scope.TenantID, p.scope.AppID, p.binding.BindingID).Scan(&orphanCount); err != nil {
		t.Fatalf("count orphan channel inbox rows: %v", err)
	}
	if orphanCount != 0 {
		t.Fatalf("orphan channel inbox rows = %d, want 0", orphanCount)
	}
}

func TestChannelAdmissionPayloadHashSurvivesExternalIDKeyRotation(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	request := newIM05Request(t, p.route, p.binding, "message-rotation", "request-rotation", channels.MessageTypeText, "hello")
	first, err := p.store.Admit(p.ctx, request)
	if err != nil {
		t.Fatalf("admit pre-rotation channel message: %v", err)
	}
	rotatedStore, err := platformpostgres.New(
		p.pool,
		platformpostgres.WithChannelIdentityMapping(
			integrationExternalIDHasher{activeKeyVersion: "v2"},
			newIntegrationTargetProtector(t, "v1"),
			[]string{"v2", "v1"},
		),
	)
	if err != nil {
		t.Fatalf("new rotated IM-05 store: %v", err)
	}
	duplicate := request
	duplicate.RequestID = "request-rotation-retry"
	replayed, err := rotatedStore.Admit(p.ctx, duplicate)
	if err != nil {
		t.Fatalf("replay post-rotation channel message: %v", err)
	}
	if !replayed.Replayed || replayed.RequestID != first.RequestID || replayed.Status != gateway.AdmissionStatusAdmitted {
		t.Fatalf("post-rotation replay result = %#v", replayed)
	}
}

func TestChannelAdmissionRequiresChannelInput(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	identityResolver, err := gateway.NewChannelBindingIdentityResolver(p.route, tenant.RuntimeContext{
		TenantID:           p.binding.TenantID,
		AppID:              p.binding.AppID,
		ConfigVersion:      "v1",
		Channel:            string(p.binding.Channel),
		BindingID:          p.binding.BindingID,
		SessionID:          channels.DefaultSessionID,
		SessionPrincipalID: "user-1",
		UserID:             "user-1",
		TraceID:            "trace-missing-input",
	})
	if err != nil {
		t.Fatalf("new complete channel identity resolver: %v", err)
	}
	identity, err := identityResolver.ResolveAdmissionIdentity(p.ctx)
	if err != nil {
		t.Fatalf("resolve complete channel identity: %v", err)
	}
	_, err = p.store.Admit(p.ctx, gateway.AdmissionRequest{
		RequestID:      "request-missing-input",
		IdempotencyKey: "message-missing-input",
		Identity:       identity,
		Message:        gateway.Message{Text: "hello"},
	})
	if !errors.Is(err, gateway.ErrChannelInputRequired) {
		t.Fatalf("missing channel input error = %v, want channel input required", err)
	}
}

func TestChannelAdmissionRejectsConflictingPayloadAndDoesNotRearmFailedExecution(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	request := newIM05Request(t, p.route, p.binding, "message-conflict", "request-conflict", channels.MessageTypeText, "hello")

	first, err := p.store.Admit(p.ctx, request)
	if err != nil {
		t.Fatalf("admit channel message: %v", err)
	}
	if _, err := p.pool.Exec(p.ctx, `
UPDATE platform.execution
SET status = 'FAILED'
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, first.RequestID,
	); err != nil {
		t.Fatalf("mark execution failed: %v", err)
	}

	failedReplay := request
	failedReplay.RequestID = "request-failed-replay"
	replayed, err := p.store.Admit(p.ctx, failedReplay)
	if err != nil {
		t.Fatalf("replay failed channel execution: %v", err)
	}
	if !replayed.Replayed || replayed.Status != gateway.AdmissionStatusAdmitted || replayed.RequestID != first.RequestID {
		t.Fatalf("failed channel replay result = %#v", replayed)
	}

	var status string
	if err := p.pool.QueryRow(p.ctx, `
SELECT status
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, first.RequestID).Scan(&status); err != nil {
		t.Fatalf("read failed execution status: %v", err)
	}
	if status != "FAILED" {
		t.Fatalf("failed channel replay status = %q, want FAILED", status)
	}

	conflicting := request
	conflicting.RequestID = "request-conflict-payload"
	conflictingInput := request.ChannelInput.Clone()
	conflictingInput.Text = "different payload"
	conflicting.ChannelInput = &conflictingInput
	conflicting.Message.Text = conflictingInput.Text
	if _, err := p.store.Admit(p.ctx, conflicting); !errors.Is(err, gateway.ErrIdempotencyConflict) {
		t.Fatalf("conflicting channel payload error = %v, want idempotency conflict", err)
	}

	inboxCount, executionCount, dispatchCount, _ := im05Counts(t, p)
	if inboxCount != 1 || executionCount != 1 || dispatchCount != 1 {
		t.Fatalf("conflicting channel counts = inbox:%d execution:%d dispatch:%d", inboxCount, executionCount, dispatchCount)
	}
}

func TestChannelAdmissionConcurrentDuplicateCreatesOneExecution(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	const workers = 8
	requests := make([]gateway.AdmissionRequest, workers)
	for i := range requests {
		requests[i] = newIM05Request(
			t,
			p.route,
			p.binding,
			"message-concurrent",
			fmt.Sprintf("request-concurrent-%d", i),
			channels.MessageTypeText,
			"hello",
		)
	}

	results := make(chan gateway.AdmissionResult, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for _, request := range requests {
		wg.Add(1)
		go func(request gateway.AdmissionRequest) {
			defer wg.Done()
			result, err := p.store.Admit(p.ctx, request)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(request)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent channel admission: %v", err)
	}

	var originalRequestID string
	for result := range results {
		if result.Status != gateway.AdmissionStatusAdmitted {
			t.Fatalf("concurrent channel result = %#v", result)
		}
		if originalRequestID == "" {
			originalRequestID = result.RequestID
		} else if result.RequestID != originalRequestID {
			t.Fatalf("concurrent channel request IDs = %q and %q", originalRequestID, result.RequestID)
		}
	}
	if originalRequestID == "" {
		t.Fatal("concurrent channel admission returned no result")
	}

	inboxCount, executionCount, dispatchCount, identityCount := im05Counts(t, p)
	if inboxCount != 1 || executionCount != 1 || dispatchCount != 1 || identityCount != 1 {
		t.Fatalf("concurrent channel counts = inbox:%d execution:%d dispatch:%d identity:%d", inboxCount, executionCount, dispatchCount, identityCount)
	}
}

func TestChannelAdmissionRejectedInputIsDurableWithoutExecution(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	request := newIM05Request(t, p.route, p.binding, "message-unsupported", "request-unsupported", channels.MessageTypeCard, "")

	first, err := p.store.Admit(p.ctx, request)
	if err != nil {
		t.Fatalf("admit unsupported channel message: %v", err)
	}
	if first.Status != gateway.AdmissionStatusRejected || first.RequestID != request.RequestID || first.ConfigVersion != "" || first.TurnSeq != 0 {
		t.Fatalf("rejected channel admission result = %#v", first)
	}

	duplicate := request
	duplicate.RequestID = "request-unsupported-retry"
	replayed, err := p.store.Admit(p.ctx, duplicate)
	if err != nil {
		t.Fatalf("replay unsupported channel message: %v", err)
	}
	if !replayed.Replayed || replayed.Status != gateway.AdmissionStatusRejected || replayed.RequestID != first.RequestID {
		t.Fatalf("replayed rejected channel result = %#v", replayed)
	}

	inboxCount, executionCount, dispatchCount, identityCount := im05Counts(t, p)
	if inboxCount != 1 || executionCount != 0 || dispatchCount != 0 || identityCount != 0 {
		t.Fatalf("rejected channel counts = inbox:%d execution:%d dispatch:%d identity:%d", inboxCount, executionCount, dispatchCount, identityCount)
	}
}

func TestChannelAdmissionRollsBackInboxAndMappingOnFailure(t *testing.T) {
	p := newIM05Fixture(t, failingTargetProtector{})
	request := newIM05Request(t, p.route, p.binding, "message-rollback", "request-rollback", channels.MessageTypeText, "hello")

	if _, err := p.store.Admit(p.ctx, request); err == nil {
		t.Fatal("channel admission succeeded with failing target protector")
	}
	inboxCount, executionCount, dispatchCount, identityCount := im05Counts(t, p)
	if inboxCount != 0 || executionCount != 0 || dispatchCount != 0 || identityCount != 0 {
		t.Fatalf("rolled back channel counts = inbox:%d execution:%d dispatch:%d identity:%d", inboxCount, executionCount, dispatchCount, identityCount)
	}
}

func TestRecordChannelFailurePersistsOneVisibleReply(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	request := newIM05Request(t, p.route, p.binding, "message-failed", "request-failed", channels.MessageTypeImage, "")
	input, err := channels.WithMessageReplyTarget(*request.ChannelInput, channels.MessageReplyTarget{
		ProviderTarget: "provider-message-failed",
		ExpiresAt:      time.Now().UTC().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("attach failure reply target: %v", err)
	}
	request.ChannelInput = &input

	if err := p.store.RecordChannelFailure(p.ctx, request); err != nil {
		t.Fatalf("record channel failure: %v", err)
	}
	if err := p.store.RecordChannelFailure(p.ctx, request); err != nil {
		t.Fatalf("replay channel failure: %v", err)
	}

	var status, reason, replyText string
	var targetEnvelope []byte
	var replyCount int
	if err := p.pool.QueryRow(p.ctx, `
SELECT status, COALESCE(reject_reason, ''), provider_reply_target_envelope
FROM platform.channel_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, request.IdempotencyKey,
	).Scan(&status, &reason, &targetEnvelope); err != nil {
		t.Fatalf("read failed channel inbox: %v", err)
	}
	if status != "REJECTED" || reason != "CHANNEL_PROCESSING_FAILED" || len(targetEnvelope) == 0 {
		t.Fatalf("failed channel inbox = status:%q reason:%q target:%d", status, reason, len(targetEnvelope))
	}
	if err := p.pool.QueryRow(p.ctx, `
SELECT count(*), min(payload->>'text')
FROM platform.reply_outbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND request_id = $4 AND source_kind = 'channel_failure'`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, request.RequestID,
	).Scan(&replyCount, &replyText); err != nil {
		t.Fatalf("read failed channel reply: %v", err)
	}
	if replyCount != 1 || replyText != channels.ChannelFailureReply {
		t.Fatalf("failed channel replies = count:%d text:%q", replyCount, replyText)
	}
	if _, err := p.pool.Exec(p.ctx, `
UPDATE platform.reply_outbox
SET status = 'SENT', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND request_id = $4`,
		p.scope.TenantID, p.scope.AppID, p.binding.BindingID, request.RequestID,
	); err != nil {
		t.Fatalf("retire failed channel reply fixture: %v", err)
	}
}

type im05Fixture struct {
	pool    *pgxpool.Pool
	ctx     context.Context
	store   *platformpostgres.Store
	scope   tenant.Scope
	binding channels.Binding
	route   gateway.LocatedChannelBinding
}

func newIM05Fixture(t *testing.T, protector channels.TargetProtector) im05Fixture {
	t.Helper()
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, err := platformpostgres.New(
		pool,
		platformpostgres.WithChannelIdentityMapping(integrationExternalIDHasher{}, protector, []string{"v1"}),
	)
	if err != nil {
		t.Fatalf("new IM-05 postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate IM-05 store: %v", err)
	}
	scope := tenant.Scope{TenantID: fmt.Sprintf("im05-%d", time.Now().UnixNano()), AppID: "support"}
	seedIdentityMappingScope(t, ctx, store, scope, "wecom-binding", channels.ChannelWeCom)
	binding, err := store.ResolveBinding(ctx, scope.TenantID, scope.AppID, "wecom-binding")
	if err != nil {
		t.Fatalf("resolve IM-05 binding: %v", err)
	}
	route, err := gateway.ResolveChannelBindingRoute(ctx, store, binding.Channel, binding.PublicRouteID)
	if err != nil {
		t.Fatalf("resolve IM-05 route: %v", err)
	}
	return im05Fixture{pool: pool, ctx: ctx, store: store, scope: scope, binding: binding, route: route}
}

func newIM05Request(
	t *testing.T,
	route gateway.LocatedChannelBinding,
	binding channels.Binding,
	externalMessageID, requestID string,
	messageType channels.MessageType,
	text string,
) gateway.AdmissionRequest {
	t.Helper()
	input, err := channels.NewChannelInput(
		channels.ChannelInput{
			TenantID:          binding.TenantID,
			AppID:             binding.AppID,
			Channel:           binding.Channel,
			BindingID:         binding.BindingID,
			BindingRevision:   binding.BindingRevision,
			ExternalMessageID: externalMessageID,
			Conversation: channels.ChannelConversation{
				Kind: channels.ConversationDirect,
			},
			MessageType: messageType,
			Text:        text,
		},
		channels.ChannelMappingInput{
			ExternalSenderID:     "user-1",
			ProviderSenderTarget: "user-target-1",
		},
	)
	if err != nil {
		t.Fatalf("new IM-05 channel input: %v", err)
	}
	identityResolver, err := gateway.NewChannelBindingInputIdentityResolver(route, tenant.RuntimeContext{
		TenantID:      binding.TenantID,
		AppID:         binding.AppID,
		ConfigVersion: "v1",
		Channel:       string(binding.Channel),
		BindingID:     binding.BindingID,
		TraceID:       requestID,
	})
	if err != nil {
		t.Fatalf("new IM-05 identity resolver: %v", err)
	}
	identity, err := identityResolver.ResolveAdmissionIdentity(context.Background())
	if err != nil {
		t.Fatalf("resolve IM-05 admission identity: %v", err)
	}
	return gateway.AdmissionRequest{
		RequestID:      requestID,
		IdempotencyKey: externalMessageID,
		Identity:       identity,
		Message: gateway.Message{
			Text: text,
		},
		ChannelInput: &input,
	}
}

func im05Counts(t *testing.T, fixture im05Fixture) (int, int, int, int) {
	t.Helper()
	var inboxCount, executionCount, dispatchCount, identityCount int
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT count(*)
FROM platform.channel_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		fixture.scope.TenantID, fixture.scope.AppID, fixture.binding.BindingID).Scan(&inboxCount); err != nil {
		t.Fatalf("count channel inbox: %v", err)
	}
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT count(*)
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2`, fixture.scope.TenantID, fixture.scope.AppID).Scan(&executionCount); err != nil {
		t.Fatalf("count channel executions: %v", err)
	}
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT count(*)
FROM platform.dispatch_outbox
WHERE tenant_id = $1 AND app_id = $2`, fixture.scope.TenantID, fixture.scope.AppID).Scan(&dispatchCount); err != nil {
		t.Fatalf("count channel dispatches: %v", err)
	}
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT count(*)
FROM platform.channel_identity
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		fixture.scope.TenantID, fixture.scope.AppID, fixture.binding.BindingID).Scan(&identityCount); err != nil {
		t.Fatalf("count channel identities: %v", err)
	}
	return inboxCount, executionCount, dispatchCount, identityCount
}

type failingTargetProtector struct{}

func (failingTargetProtector) Seal(context.Context, channels.TargetContext, channels.TargetPurpose, channels.TargetPlaintext) (channels.TargetEnvelope, error) {
	return channels.TargetEnvelope{}, errors.New("target protection is unavailable")
}

func (failingTargetProtector) Open(context.Context, channels.TargetContext, channels.TargetPurpose, channels.TargetEnvelope) (channels.TargetPlaintext, error) {
	return channels.TargetPlaintext{}, errors.New("target protection is unavailable")
}
