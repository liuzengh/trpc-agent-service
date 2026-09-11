package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding"
	channelhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/inbound/http"
	channelapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	channeldomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

// This is the real Session -> owner authorization -> PostgreSQL -> mTLS wire
// slice. It uses a random test schema created by the parent harness, not a live
// deployment, Telegram endpoint, webhook, NATS broker, or account credential.
func testTelegramPreflightHTTP(t *testing.T, ctx context.Context, router http.Handler, pool *pgxpool.Pool, module *channelbinding.Module, authenticate gin.HandlerFunc, tenant, ownerID, memberID string, owner, member, outsider *http.Cookie) {
	accountBase := "/v1/tenants/" + tenant + "/channel-accounts"
	var account channelapp.CommandResult
	deploymentRequest(t, router, "POST", accountBase, owner, "preflight-fixture-account", `{"provider":"telegram","config":{"receive_mode":"webhook"},"provider_account_id":"123456789","name":"Preflight disabled fixture","credentials":{"telegram.bot_token":{"action":"replace","value":"TEST_ONLY_PREFLIGHT_BOT_TOKEN"},"telegram.webhook_secret":{"action":"replace","value":"TEST_ONLY_PREFLIGHT_WEBHOOK_SECRET"}}}`, 201, &account)
	if account.Account == nil || account.Account.Enabled {
		t.Fatal("fixture must be a saved disabled account")
	}
	accountID := account.Account.ID
	accountPath := accountBase + "/" + accountID
	base := accountPath + "/preflights"
	body := `{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_token_version":1}`
	var bindingCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_bindings WHERE tenant_id=$1 AND account_id=$2`, tenant, accountID).Scan(&bindingCount); err != nil || bindingCount != 0 {
		t.Fatalf("unbound fixture count=%d err=%v", bindingCount, err)
	}
	// The same user has another visible tenant: malformed content must not reveal
	// a foreign account before account visibility returns 404.
	var otherTenant struct {
		ID string `json:"id"`
	}
	request(t, router, "POST", "/v1/admin/tenants", outsider, fmt.Sprintf(`{"slug":"preflight-other","name":"Preflight Other","owner_user_id":%q}`, ownerID), 201, &otherTenant)
	deploymentRequest(t, router, "POST", "/v1/tenants/"+otherTenant.ID+"/channel-accounts/"+accountID+"/preflights", owner, "foreign-account", "{malformed", 404, nil)
	deploymentRequest(t, router, "POST", base, nil, "anonymous", body, 401, nil)
	memberDenied := deploymentRequest(t, router, "POST", base, member, "member", body, 403, nil)
	if memberDenied.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("member denied preflight response is cacheable")
	}
	t.Run("OutsiderValidCreateHidden", func(t *testing.T) {
		w := deploymentRequest(t, router, "POST", base, outsider, "outsider", body, 404, nil)
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("outsider create response is cacheable")
		}
	})
	before := preflightUnchangedState(t, ctx, pool)
	var created channelv1.PreflightCreated
	first := deploymentRequest(t, router, "POST", base, owner, "preflight-first", body, 202, &created)
	if first.Header().Get("Cache-Control") != "no-store" || first.Header().Get("Location") != created.StatusURL || first.Header().Get("Retry-After") != "2" {
		t.Fatal("invalid creation headers")
	}
	if err := channelv1.Validate("preflight-created.schema.json", first.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
	replay := deploymentRequest(t, router, "POST", base, owner, "preflight-first", body, 202, nil)
	if first.Body.String() != replay.Body.String() {
		t.Fatal("creation replay changed the immutable receipt")
	}
	deploymentRequest(t, router, "POST", base, owner, "preflight-first", strings.Replace(body, `"expected_account_revision":1`, `"expected_account_revision":2`, 1), 409, nil)
	deploymentRequest(t, router, "POST", base, owner, "preflight-competing", body, 409, nil)
	view := preflightPublicView(t, router, created.StatusURL, member)
	if view.State != "QUEUED" || view.Freshness != "NOT_CHECKED" || view.GatewayConfigDigest != nil || view.GatewayConfigFreshness != nil || len(view.Checks) != 0 {
		t.Fatalf("queued view=%+v", view)
	}
	t.Run("OutsiderValidReadHidden", func(t *testing.T) {
		w := request(t, router, "GET", created.StatusURL, outsider, "", 404, nil)
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("outsider read response is cacheable")
		}
	})
	testPreflightUnauthenticatedNoStore(t, router, base, created.StatusURL, owner.Name)
	testPreflightInvisibleMalformedRequests(t, ctx, router, pool, base, created.StatusURL, outsider)
	request(t, router, "GET", base+"/cpf_not_found", member, "", 404, nil)
	serverTLS, clientTLS, _ := channelTLS(t)
	server := httptest.NewUnstartedServer(module.InternalHandler)
	server.TLS = serverTLS
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	transport := &http.Transport{TLSClientConfig: clientTLS}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	call := func(path string, body any, want int) []byte {
		return preflightInternalCall(t, client, server.URL, path, body, want)
	}
	origin := "https://gateway.example.com"
	digest, err := channelv1.PreflightConfigDigest("gateway_pool", channelEpoch, &origin, "PUBLIC_ORIGIN_STATIC_VALID")
	if err != nil {
		t.Fatal(err)
	}
	claim := channelv1.PreflightClaimRequest{DiagnosticPolicy: channelv1.PreflightReceiveModesPolicy, SchemaVersion: 1, ScopeID: "gateway_pool", SourceEpoch: channelEpoch, InstanceEpoch: "22222222-2222-4222-8222-222222222222", ClaimRequestID: "33333333-3333-4333-8333-333333333333", ClaimToken: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), GatewayConfigDigest: digest, ExpectedPublicOrigin: &origin, OriginStatus: "PUBLIC_ORIGIN_STATIC_VALID", Limit: 1}
	var grant channelv1.PreflightGrant
	raw := call("/internal/v1/channel-preflights:claim", claim, 200)
	if err := channelv1.Decode("preflight-grant.schema.json", raw, &grant); err != nil {
		t.Fatal(err)
	}
	if grant.PreflightID != created.PreflightID || grant.AccountID != accountID || grant.LeaseEpoch != 1 || grant.Credentials.Purpose != "telegram.bot_token" {
		t.Fatalf("grant did not identify exact task: %+v", grant)
	}
	var replayGrant channelv1.PreflightGrant
	raw = call("/internal/v1/channel-preflights:claim", claim, 200)
	if err := json.Unmarshal(raw, &replayGrant); err != nil {
		t.Fatal(err)
	}
	if !grant.LeaseExpiresAt.Equal(replayGrant.LeaseExpiresAt) || grant.LeaseEpoch != replayGrant.LeaseEpoch || grant.PreflightID != replayGrant.PreflightID || replayGrant.ServerTime.Before(grant.ServerTime) {
		t.Fatal("claim replay changed lease or used an old server time")
	}
	resolve := channelv1.PreflightResolveRequest{SchemaVersion: 1, ScopeID: claim.ScopeID, SourceEpoch: claim.SourceEpoch, InstanceEpoch: claim.InstanceEpoch, LeaseEpoch: grant.LeaseEpoch, ClaimToken: claim.ClaimToken}
	resolvePath := "/internal/v1/channel-preflights/" + grant.PreflightID + "/credentials:resolve"
	var resolved channelv1.PreflightResolveResponse
	raw = call(resolvePath, resolve, 200)
	if err := channelv1.Decode("preflight-resolved.schema.json", raw, &resolved); err != nil {
		t.Fatal(err)
	}
	if resolved.Value != "TEST_ONLY_PREFLIGHT_BOT_TOKEN" || resolved.Purpose != "telegram.bot_token" || resolved.CredentialID != grant.Credentials.CredentialID || strings.Contains(string(raw), "WEBHOOK_SECRET") {
		t.Fatal("diagnostic resolve was not the precise BotToken-only private result")
	}
	wrong := resolve
	wrong.ClaimToken = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	call(resolvePath, wrong, 409)
	// Normal runtime resolution still rejects this disabled account.
	epoch := int64(1)
	var credentials []channeldomain.CredentialUse
	rows, err := pool.Query(ctx, `SELECT purpose,id,credential_version FROM channel_account_credentials WHERE tenant_id=$1 AND account_id=$2 ORDER BY purpose`, tenant, accountID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var c channeldomain.CredentialUse
		if err := rows.Scan(&c.Purpose, &c.ID, &c.Version); err != nil {
			t.Fatal(err)
		}
		credentials = append(credentials, c)
	}
	rows.Close()
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	normal := channelapp.ResolveRequest{SchemaVersion: 1, ScopeID: "gateway_pool", SourceEpoch: channelEpoch, ConnectionRevision: 1, Consumer: channeldomain.Consumer{Kind: "telegram_registration", InstanceID: "gw-1", RegistrationEpoch: &epoch}, Uses: credentials}
	call("/internal/v1/tenants/"+tenant+"/channel-accounts/"+accountID+"/credentials:resolve", normal, 409)
	complete := preflightCompleteFixture(t, claim, grant)
	completePath := "/internal/v1/channel-preflights/" + grant.PreflightID + ":complete"
	call(completePath, complete, 204)
	completed := preflightPublicView(t, router, created.StatusURL, member)
	if completed.State != "COMPLETED" || completed.Outcome != "WARN" || completed.Freshness != "CURRENT" || completed.CheckedAt == nil || completed.GatewayConfigFreshness == nil || *completed.GatewayConfigFreshness != "UNCONFIRMED" || len(completed.Checks) != 8 {
		t.Fatalf("completed result=%+v", completed)
	}
	call(completePath, complete, 204)
	changedComplete := complete
	changedComplete.ObservedAt = complete.ObservedAt.Add(time.Second)
	call(completePath, changedComplete, 409)
	if again := preflightPublicView(t, router, created.StatusURL, member); !again.CheckedAt.Equal(*completed.CheckedAt) {
		t.Fatal("completion replay changed checked_at")
	}
	// Create another task and remove requester ownership after it was claimed.
	var second channelv1.PreflightCreated
	deploymentRequest(t, router, "POST", base, owner, "preflight-second", body, 202, &second)
	secondClaim := claim
	secondClaim.ClaimRequestID = "44444444-4444-4444-8444-444444444444"
	secondClaim.ClaimToken = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{3}, 32))
	var secondGrant channelv1.PreflightGrant
	raw = call("/internal/v1/channel-preflights:claim", secondClaim, 200)
	if err := json.Unmarshal(raw, &secondGrant); err != nil {
		t.Fatal(err)
	}
	if secondGrant.PreflightID != second.PreflightID {
		t.Fatal("second claim selected wrong task")
	}
	if _, err := pool.Exec(ctx, `UPDATE tenant_memberships SET role='MEMBER' WHERE tenant_id=$1 AND user_id=$2`, tenant, ownerID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := pool.Exec(context.Background(), `UPDATE tenant_memberships SET role='OWNER' WHERE tenant_id=$1 AND user_id=$2`, tenant, ownerID); err != nil {
			t.Error(err)
		}
	}()
	secondResolve := channelv1.PreflightResolveRequest{SchemaVersion: 1, ScopeID: claim.ScopeID, SourceEpoch: claim.SourceEpoch, InstanceEpoch: claim.InstanceEpoch, LeaseEpoch: secondGrant.LeaseEpoch, ClaimToken: secondClaim.ClaimToken}
	call("/internal/v1/channel-preflights/"+second.PreflightID+"/credentials:resolve", secondResolve, 409)
	call("/internal/v1/channel-preflights/"+second.PreflightID+":complete", preflightCompleteFixture(t, secondClaim, secondGrant), 409)
	stale := preflightPublicView(t, router, second.StatusURL, member)
	if stale.State != "STALE" || stale.ReasonCode != "CHANNEL_PREFLIGHT_REQUESTER_REVOKED" || len(stale.Checks) != 0 {
		t.Fatalf("revoked requester result=%+v", stale)
	}
	// An already accepted receipt remains acknowledgeable without issuing fresh
	// diagnostic authority, even after the creator loses OWNER privileges.
	call(completePath, complete, 204)
	deploymentRequest(t, router, "POST", base, owner, "preflight-first", body, 403, nil)
	if _, err := pool.Exec(ctx, `UPDATE tenant_memberships SET role='OWNER' WHERE tenant_id=$1 AND user_id=$2`, tenant, ownerID); err != nil {
		t.Fatal(err)
	}
	// Advance only this freshly created test task's stored deadline. No Gateway
	// claims it and no real-time sleep or maintenance worker is required: GET
	// must converge the expired queued task to a durable terminal fact itself.
	var unclaimed channelv1.PreflightCreated
	deploymentRequest(t, router, "POST", base, owner, "preflight-no-executor", body, 202, &unclaimed)
	preflightShiftUnclaimedDeadline(t, ctx, pool, unclaimed.PreflightID)
	timedOut := preflightPublicView(t, router, unclaimed.StatusURL, member)
	if timedOut.State != "TIMED_OUT" || timedOut.ReasonCode != "CHANNEL_PREFLIGHT_NO_EXECUTOR" || timedOut.StartedAt != nil || timedOut.CheckedAt != nil || timedOut.GatewayConfigFreshness != nil || len(timedOut.Checks) != 0 {
		t.Fatalf("unclaimed timeout result=%+v", timedOut)
	}
	var persistedState string
	if err := pool.QueryRow(ctx, `SELECT state FROM channel_preflights WHERE id=$1`, unclaimed.PreflightID).Scan(&persistedState); err != nil || persistedState != "TIMED_OUT" {
		t.Fatalf("timeout persistence=%s err=%v", persistedState, err)
	}
	if !reflect.DeepEqual(before, preflightUnchangedState(t, ctx, pool)) {
		t.Fatal("preflight mutated Account/Credential/Binding/Route/catalog/Observation/command receipt/Outbox")
	}
	var stored string
	if err := pool.QueryRow(ctx, `SELECT string_agg(document,'') FROM (SELECT record_jsonb::text AS document FROM channel_preflights WHERE tenant_id=$1 AND account_id=$2 UNION ALL SELECT record_jsonb::text FROM channel_preflight_requests WHERE tenant_id=$1 AND account_id=$2) records`, tenant, accountID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"TEST_ONLY_PREFLIGHT_BOT_TOKEN", "TEST_ONLY_PREFLIGHT_WEBHOOK_SECRET", claim.ClaimToken, secondClaim.ClaimToken} {
		if strings.Contains(stored, secret) {
			t.Fatal("preflight storage retained a plaintext secret or claim token")
		}
	}
	testPreflightConfigurationAndOwnerReplacement(t, ctx, router, pool, accountBase, tenant, ownerID, memberID, owner, member, claim, func(sub *testing.T, path string, body any, want int) []byte {
		return preflightInternalCall(sub, client, server.URL, path, body, want)
	})
	testPreflightRevokedSessionBeforeCommit(t, ctx, router, pool, module, authenticate, accountBase, tenant, ownerID, owner)
}

func preflightPublicView(t *testing.T, router http.Handler, path string, cookie *http.Cookie) channelv1.PreflightView {
	t.Helper()
	var view channelv1.PreflightView
	w := request(t, router, "GET", path, cookie, "", 200, &view)
	if err := channelv1.Validate("preflight-view.schema.json", w.Body.Bytes()); err != nil {
		t.Fatalf("public preflight schema: %v", err)
	}
	assertChannelPublic(t, w.Body.Bytes())
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("public result cacheable")
	}
	return view
}
func preflightInternalCall(t *testing.T, client *http.Client, base, path string, value any, want int) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	req, err := http.NewRequest("POST", base+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 32*1024))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != want {
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &envelope)
		t.Fatalf("private path=%s status=%d want=%d code=%s", path, res.StatusCode, want, envelope.Error.Code)
	}
	if res.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("private result cacheable")
	}
	if want == 200 && res.Header.Get("Content-Type") != "application/json" {
		t.Fatal("private 200 missing JSON type")
	}
	if want == 204 && len(data) != 0 {
		t.Fatal("private 204 has a body")
	}
	return data
}
func preflightCompleteFixture(t *testing.T, claim channelv1.PreflightClaimRequest, grant channelv1.PreflightGrant) channelv1.PreflightCompleteRequest {
	t.Helper()
	raw, err := os.ReadFile("../../../api/schemas/channel/v1/fixtures/preflight-complete-valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Document channelv1.PreflightCompleteRequest `json:"document"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	complete := fixture.Document
	complete.ScopeID = claim.ScopeID
	complete.SourceEpoch = claim.SourceEpoch
	complete.InstanceEpoch = claim.InstanceEpoch
	complete.LeaseEpoch = grant.LeaseEpoch
	complete.ClaimToken = claim.ClaimToken
	complete.GatewayConfigDigest = claim.GatewayConfigDigest
	complete.ExpectedPublicOrigin = claim.ExpectedPublicOrigin
	complete.ObservedAt = time.Now().UTC()
	complete.DiagnosticPolicy, complete.ReceiveMode, complete.ConnectionRevision, complete.OriginStatus = grant.DiagnosticPolicy, grant.ReceiveMode, grant.ConnectionRevision, claim.OriginStatus
	complete.EffectiveConfigDigest = grant.EffectiveConfigDigest
	return complete
}
func preflightUnchangedState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, table := range []string{"channel_accounts", "channel_account_credentials", "channel_bindings", "channel_account_route_states", "channel_account_catalog", "channel_account_observations", "channel_command_receipts", "control_outbox"} {
		var digest string
		query := `SELECT md5(COALESCE(string_agg(to_jsonb(row)::text,',' ORDER BY to_jsonb(row)::text),'')) FROM ` + pgx.Identifier{table}.Sanitize() + ` row`
		if err := pool.QueryRow(ctx, query).Scan(&digest); err != nil {
			t.Fatal(err)
		}
		out[table] = digest
	}
	return out
}

// The harness owns a newly created random schema. This fixture changes only a
// diagnostic task's non-secret timestamps, preserving column/JSON invariants.
func preflightShiftUnclaimedDeadline(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT record_jsonb FROM channel_preflights WHERE id=$1 AND state='QUEUED'`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var record channelapp.PreflightRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	record.View.RequestedAt = record.View.RequestedAt.Add(-3 * time.Minute)
	record.View.JobDeadlineAt = record.View.RequestedAt.Add(120 * time.Second)
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE channel_preflights SET requested_at=$2,job_deadline_at=$3,record_jsonb=$4 WHERE id=$1 AND state='QUEUED'`, id, record.View.RequestedAt, record.View.JobDeadlineAt, raw); err != nil {
		t.Fatal(err)
	}
}

func testPreflightConfigurationAndOwnerReplacement(t *testing.T, ctx context.Context, router http.Handler, pool *pgxpool.Pool, accountBase, tenant, ownerID, memberID string, owner, member *http.Cookie, template channelv1.PreflightClaimRequest, call func(*testing.T, string, any, int) []byte) {
	const body = `{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_token_version":1}`
	createAccount := func(t *testing.T, suffix, providerID string) string {
		t.Helper()
		var result channelapp.CommandResult
		input := fmt.Sprintf(`{"provider":"telegram","config":{"receive_mode":"webhook"},"provider_account_id":%q,"name":%q,"credentials":{"telegram.bot_token":{"action":"replace","value":"TEST_ONLY_PREFLIGHT_BOT_TOKEN"},"telegram.webhook_secret":{"action":"replace","value":"TEST_ONLY_PREFLIGHT_WEBHOOK_SECRET"}}}`, providerID, suffix)
		deploymentRequest(t, router, "POST", accountBase, owner, "preflight-account-"+suffix, input, 201, &result)
		if result.Account == nil || result.Account.Enabled {
			t.Fatal("fixture account is not disabled")
		}
		return accountBase + "/" + result.Account.ID + "/preflights"
	}
	t.Run("FirstCompleteChangedConfigurationPersistsStale", func(t *testing.T) {
		base := createAccount(t, "config-change", "223456789")
		before := preflightUnchangedState(t, ctx, pool)
		var created channelv1.PreflightCreated
		deploymentRequest(t, router, "POST", base, owner, "preflight-config-task", body, 202, &created)
		// A fresh poll respects the same instance's fixed two-claims/second limit.
		// This is rate-gate pacing, not a fabricated task deadline or provider wait.
		time.Sleep(1100 * time.Millisecond)
		claim := template
		claim.ClaimRequestID = "55555555-5555-4555-8555-555555555555"
		claim.ClaimToken = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{5}, 32))
		var grant channelv1.PreflightGrant
		raw := call(t, "/internal/v1/channel-preflights:claim", claim, 200)
		if err := channelv1.Decode("preflight-grant.schema.json", raw, &grant); err != nil {
			t.Fatal(err)
		}
		if grant.PreflightID != created.PreflightID {
			t.Fatal("claim did not identify configuration test task")
		}
		completeA := preflightCompleteFixture(t, claim, grant)
		completeB := completeA
		originB := "https://replacement.example.com"
		completeB.ExpectedPublicOrigin = &originB
		digestB, err := channelv1.PreflightConfigDigest(claim.ScopeID, claim.SourceEpoch, &originB, "PUBLIC_ORIGIN_STATIC_VALID")
		if err != nil {
			t.Fatal(err)
		}
		completeB.GatewayConfigDigest = digestB
		completeB.EffectiveConfigDigest, _ = channelv1.PreflightEffectiveConfigDigest(claim.ScopeID, claim.SourceEpoch, completeB.ReceiveMode, completeB.ConnectionRevision, &originB, claim.OriginStatus)
		payload, err := json.Marshal(completeB)
		if err != nil || channelv1.Validate("preflight-complete.schema.json", payload) != nil {
			t.Fatal("replacement configuration must be independently valid wire")
		}
		path := "/internal/v1/channel-preflights/" + created.PreflightID + ":complete"
		raw = call(t, path, completeB, 409)
		if !bytes.Contains(raw, []byte("CHANNEL_PREFLIGHT_STALE")) {
			t.Fatal("configuration change did not return STALE")
		}
		view := preflightPublicView(t, router, created.StatusURL, member)
		if view.State != "STALE" || view.ReasonCode != "CHANNEL_PREFLIGHT_GATEWAY_CONFIG_CHANGED" || view.GatewayConfigDigest == nil || *view.GatewayConfigDigest != claim.GatewayConfigDigest || view.ExpectedPublicOrigin == nil || *view.ExpectedPublicOrigin != *claim.ExpectedPublicOrigin || len(view.Checks) != 0 {
			t.Fatalf("changed configuration view=%+v", view)
		}
		call(t, path, completeA, 409)
		again := preflightPublicView(t, router, created.StatusURL, member)
		if !reflect.DeepEqual(view, again) {
			t.Fatal("late original configuration result rewrote stale task")
		}
		if !reflect.DeepEqual(before, preflightUnchangedState(t, ctx, pool)) {
			t.Fatal("configuration diagnostic changed runtime tables")
		}
	})
	t.Run("NewOwnerReconcilesPreviousRequesterBeforeCreate", func(t *testing.T) {
		base := createAccount(t, "owner-change", "323456789")
		before := preflightUnchangedState(t, ctx, pool)
		var previous channelv1.PreflightCreated
		deploymentRequest(t, router, "POST", base, owner, "preflight-previous-owner", body, 202, &previous)
		if _, err := pool.Exec(ctx, `UPDATE tenant_memberships SET role=CASE WHEN user_id=$2 THEN 'MEMBER' ELSE 'OWNER' END WHERE tenant_id=$1 AND user_id IN ($2,$3)`, tenant, ownerID, memberID); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := pool.Exec(context.Background(), `UPDATE tenant_memberships SET role=CASE WHEN user_id=$2 THEN 'OWNER' ELSE 'MEMBER' END WHERE tenant_id=$1 AND user_id IN ($2,$3)`, tenant, ownerID, memberID); err != nil {
				t.Error(err)
			}
		}()
		// No GET/Claim/Maintain runs after the role change before this request.
		var replacement channelv1.PreflightCreated
		deploymentRequest(t, router, "POST", base, member, "preflight-replacement-owner", body, 202, &replacement)
		if previous.PreflightID == replacement.PreflightID {
			t.Fatal("new owner did not get a new task")
		}
		oldView := preflightPublicView(t, router, previous.StatusURL, member)
		if oldView.State != "STALE" || oldView.ReasonCode != "CHANNEL_PREFLIGHT_REQUESTER_REVOKED" || oldView.RequestedBy != ownerID {
			t.Fatalf("previous requester task=%+v", oldView)
		}
		newView := preflightPublicView(t, router, replacement.StatusURL, member)
		if newView.State != "QUEUED" || newView.RequestedBy != memberID {
			t.Fatalf("new owner task=%+v", newView)
		}
		if !reflect.DeepEqual(before, preflightUnchangedState(t, ctx, pool)) {
			t.Fatal("owner reconciliation changed runtime tables")
		}
	})
}

func testPreflightRevokedSessionBeforeCommit(t *testing.T, ctx context.Context, router http.Handler, pool *pgxpool.Pool, module *channelbinding.Module, authenticate gin.HandlerFunc, accountBase, tenant, ownerID string, owner *http.Cookie) {
	t.Run("SessionRevokedAfterAuthenticationDoesNotMutateActiveTask", func(t *testing.T) {
		var account channelapp.CommandResult
		deploymentRequest(t, router, "POST", accountBase, owner, "preflight-session-fixture", `{"provider":"telegram","config":{"receive_mode":"webhook"},"provider_account_id":"423456789","name":"Submit Session fixture","credentials":{"telegram.bot_token":{"action":"replace","value":"TEST_ONLY_PREFLIGHT_BOT_TOKEN"},"telegram.webhook_secret":{"action":"replace","value":"TEST_ONLY_PREFLIGHT_WEBHOOK_SECRET"}}}`, 201, &account)
		if account.Account == nil {
			t.Fatal("missing session fixture account")
		}
		base := accountBase + "/" + account.Account.ID + "/preflights"
		const body = `{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_token_version":1}`
		var active channelv1.PreflightCreated
		deploymentRequest(t, router, "POST", base, owner, "preflight-session-active", body, 202, &active)
		// A distinct real login is revoked; the original owner's Session remains
		// available to the harness. No token or Session ID enters test output.
		submitCookie := login(t, router, "alice", "Alice-final-pass-5678", false)
		before := preflightTaskBytes(t, ctx, pool)
		runtimeBefore := preflightUnchangedState(t, ctx, pool)
		intercepted := 0
		boundary := gin.New()
		routes := boundary.Group("", authenticate, func(c *gin.Context) {
			identity, ok := identityapp.IdentityFromContext(c.Request.Context())
			if !ok || identity.UserID != ownerID || identity.SessionID == "" {
				t.Fatal("real middleware did not establish the expected Session identity")
			}
			tag, err := pool.Exec(ctx, `UPDATE user_sessions SET revoked_at=clock_timestamp() WHERE id=$1 AND user_id=$2 AND revoked_at IS NULL`, identity.SessionID, ownerID)
			if err != nil || tag.RowsAffected() != 1 {
				t.Fatalf("revoke authenticated test Session rows=%d err=%v", tag.RowsAffected(), err)
			}
			intercepted++
			c.Next()
		})
		channelhttp.NewPreflightHandler(module.Preflights, module.Queries).Register(routes)
		deploymentRequest(t, boundary, "POST", base, submitCookie, "preflight-revoked-before-commit", body, 403, nil)
		if intercepted != 1 {
			t.Fatalf("authenticated boundary intercepts=%d", intercepted)
		}
		if !reflect.DeepEqual(before, preflightTaskBytes(t, ctx, pool)) {
			t.Fatal("denied submit modified an existing task or idempotency receipt before Session reauthorization")
		}
		if !reflect.DeepEqual(runtimeBefore, preflightUnchangedState(t, ctx, pool)) {
			t.Fatal("denied submit changed normal runtime tables")
		}
		var state string
		if err := pool.QueryRow(ctx, `SELECT state FROM channel_preflights WHERE id=$1 AND tenant_id=$2`, active.PreflightID, tenant).Scan(&state); err != nil || state != "QUEUED" {
			t.Fatalf("active task state=%s err=%v", state, err)
		}
	})
}
func preflightTaskBytes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, table := range []string{"channel_preflights", "channel_preflight_requests"} {
		var document string
		query := `SELECT COALESCE(string_agg(to_jsonb(row)::text,E'\n' ORDER BY to_jsonb(row)::text),'') FROM ` + pgx.Identifier{table}.Sanitize() + ` row`
		if err := pool.QueryRow(ctx, query).Scan(&document); err != nil {
			t.Fatal(err)
		}
		out[table] = document
	}
	return out
}

// Visibility precedes parsing for authenticated users outside the tenant. Every
// case targets a real account and task, so a router-level missing path cannot
// accidentally satisfy the 404 invariant.
func testPreflightInvisibleMalformedRequests(t *testing.T, ctx context.Context, router http.Handler, pool *pgxpool.Pool, base, statusURL string, outsider *http.Cookie) {
	before := preflightTaskBytes(t, ctx, pool)
	const validBody = `{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_token_version":1}`
	for _, tc := range []struct {
		name, method, path, body, contentType string
		keys                                  []string
	}{
		{name: "OutsiderMalformedCreateBodyHidden", method: "POST", path: base, body: "{malformed", contentType: "application/json", keys: []string{"hidden-malformed"}},
		{name: "OutsiderWrongCreateContentTypeHidden", method: "POST", path: base, body: validBody, contentType: "text/plain", keys: []string{"hidden-content-type"}},
		{name: "OutsiderMissingCreateKeyHidden", method: "POST", path: base, body: validBody, contentType: "application/json"},
		{name: "OutsiderMalformedCreateKeyHidden", method: "POST", path: base, body: validBody, contentType: "application/json", keys: []string{"key contains spaces"}},
		{name: "OutsiderDuplicateCreateKeyHidden", method: "POST", path: base, body: validBody, contentType: "application/json", keys: []string{"first", "second"}},
		{name: "OutsiderCreateQueryHidden", method: "POST", path: base + "?unexpected=1", body: validBody, contentType: "application/json", keys: []string{"hidden-query"}},
		{name: "OutsiderMalformedTaskIDHidden", method: "GET", path: base + "/_invalid"},
		{name: "OutsiderReadBodyHidden", method: "GET", path: statusURL, body: "{malformed", contentType: "application/json"},
		{name: "OutsiderReadQueryHidden", method: "GET", path: statusURL + "?unexpected=1"},
		{name: "OutsiderMalformedReadCombinedHidden", method: "GET", path: base + "/_invalid?unexpected=1", body: "{malformed", contentType: "text/plain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.AddCookie(outsider)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			for _, key := range tc.keys {
				req.Header.Add("Idempotency-Key", key)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusNotFound {
				t.Fatalf("invisible %s request returned status=%d want=404 body=%s", tc.method, w.Code, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("invisible account response is cacheable")
			}
		})
	}
	if !reflect.DeepEqual(before, preflightTaskBytes(t, ctx, pool)) {
		t.Fatal("invisible request changed a diagnostic task or request receipt")
	}
}

// These requests go directly to the production Control router, not through the
// Web BFF. Cache headers must therefore exist even when Session middleware
// aborts before the preflight handler is reached.
func testPreflightUnauthenticatedNoStore(t *testing.T, router http.Handler, base, statusURL, cookieName string) {
	invalidCookie := &http.Cookie{Name: cookieName, Value: "TEST_ONLY_INVALID_PREFLIGHT_SESSION"}
	for _, tc := range []struct {
		name, method, path, body string
		cookie                   *http.Cookie
	}{
		{name: "AnonymousPreflightCreateIsNoStore", method: "POST", path: base, body: `{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_token_version":1}`},
		{name: "AnonymousPreflightReadIsNoStore", method: "GET", path: statusURL},
		{name: "InvalidSessionPreflightCreateIsNoStore", method: "POST", path: base, body: `{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_token_version":1}`, cookie: invalidCookie},
		{name: "InvalidSessionPreflightReadIsNoStore", method: "GET", path: statusURL, cookie: invalidCookie},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := deploymentRequest(t, router, tc.method, tc.path, tc.cookie, "preflight-auth-cache-test", tc.body, 401, nil)
			if got := w.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("authentication-aborted preflight %s Cache-Control=%q want=no-store", tc.method, got)
			}
		})
	}
}
