package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin"
	adminapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment"
	deploymentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	sharedpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/infra/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

func TestControlAPIV1AgainstPostgreSQL(t *testing.T) {
	databaseURL := os.Getenv("CONTROL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("CONTROL_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	adminPool, err := sharedpostgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminPool.Close)
	schema := fmt.Sprintf("control_v1_test_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := adminPool.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := sharedpostgres.Migrate(ctx, pool, migrations.Files); err != nil {
		t.Fatalf("migrate test schema: %v", err)
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	identityModule, err := identity.NewModule(identity.Dependencies{
		DB: pool, Routes: router, SessionLifetime: time.Hour,
		CookieName: "control_session", CookieSecure: false,
		CookieSameSite: http.SameSiteLaxMode,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantModule, err := tenant.NewModule(tenant.Dependencies{
		DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(),
		Accounts: accountLookup{accounts: identityModule.Accounts},
	})
	if err != nil {
		t.Fatal(err)
	}
	agentModule, err := agent.NewModule(agent.Dependencies{
		DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(),
		TenantAccess: tenantAccess{tenants: tenantModule.Service},
	})
	if err != nil {
		t.Fatal(err)
	}
	credentialKey := make([]byte, 32)
	if _, err := rand.Read(credentialKey); err != nil {
		t.Fatal(err)
	}
	runtimeProfileModule, err := runtimeprofile.NewModule(runtimeprofile.Dependencies{
		DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(),
		TenantAccess: tenantAccess{tenants: tenantModule.Service},
		OwnerAccess:  tenantAccess{tenants: tenantModule.Service}, CredentialKey: credentialKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	platform := deploymentdomain.DefaultPlatformExecutionContract()
	platform.Execution.AllowedEndpointHosts = []string{
		"api.openai.com", "mcp.example.com", "qdrant.internal", "state.example.test",
	}
	platform.Digest, err = platform.CalculateDigest()
	if err != nil {
		t.Fatal(err)
	}
	deploymentModule, err := deployment.NewModule(deployment.Dependencies{
		DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(),
		TenantAccess:  tenantAccess{tenants: tenantModule.Service},
		AgentVersions: agentModule.Service, ProfileRevisions: runtimeProfileModule.Service,
		ProfileCredentials: runtimeProfileModule.Service, Platform: platform,
	})
	if err != nil {
		t.Fatal(err)
	}
	channelModule := composeChannel(t, ctx, pool, router, identityModule, tenantModule, deploymentModule)
	adminModule, err := admin.NewModule(admin.Dependencies{
		DB: pool, Routes: router, Authenticate: identityModule.AuthenticationMiddleware(),
		Accounts: identityModule.Accounts, Tenants: tenantModule.Service,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminModule.Service.EnsureInitialOperator(ctx, adminapp.EnsureInitialOperatorCommand{
		Username: "platform-admin", DisplayName: "Platform Admin",
		TemporaryPassword: "Initial-pass-1234",
	}); err != nil {
		t.Fatalf("bootstrap operator: %v", err)
	}

	adminCookie := login(t, router, "platform-admin", "Initial-pass-1234", true)
	request(t, router, http.MethodPost, "/v1/me/change-password", adminCookie,
		`{"current_password":"Initial-pass-1234","new_password":"Final-admin-pass-5678"}`,
		http.StatusNoContent, nil)
	request(t, router, http.MethodGet, "/v1/admin/capabilities", adminCookie, "",
		http.StatusOK, nil)
	var user struct {
		ID string `json:"id"`
	}
	request(t, router, http.MethodPost, "/v1/admin/users", adminCookie,
		`{"username":"alice","display_name":"Alice","temporary_password":"Alice-temp-pass-1234"}`,
		http.StatusCreated, &user)
	var provisioned struct {
		ID string `json:"id"`
	}
	request(t, router, http.MethodPost, "/v1/admin/tenants", adminCookie,
		fmt.Sprintf(`{"slug":"team-a","name":"Team A","owner_user_id":%q}`, user.ID),
		http.StatusCreated, &provisioned)

	aliceCookie := login(t, router, "alice", "Alice-temp-pass-1234", true)
	request(t, router, http.MethodPost, "/v1/me/change-password", aliceCookie,
		`{"current_password":"Alice-temp-pass-1234","new_password":"Alice-final-pass-5678"}`,
		http.StatusNoContent, nil)
	var tenants struct {
		Tenants []struct {
			ID   string `json:"id"`
			Role string `json:"role"`
		} `json:"tenants"`
	}
	request(t, router, http.MethodGet, "/v1/me/tenants", aliceCookie, "",
		http.StatusOK, &tenants)
	if len(tenants.Tenants) != 1 || tenants.Tenants[0].ID != provisioned.ID ||
		tenants.Tenants[0].Role != "OWNER" {
		t.Fatalf("tenant memberships = %#v", tenants.Tenants)
	}

	var member struct {
		ID string `json:"id"`
	}
	request(t, router, http.MethodPost, "/v1/admin/users", adminCookie,
		`{"username":"bob","display_name":"Bob","temporary_password":"Bob-temp-pass-1234"}`,
		http.StatusCreated, &member)
	var disabledUser struct {
		ID string `json:"id"`
	}
	request(t, router, http.MethodPost, "/v1/admin/users", adminCookie,
		`{"username":"bobby-disabled","display_name":"Bobby Disabled","temporary_password":"Bobby-temp-pass-1234"}`,
		http.StatusCreated, &disabledUser)
	if _, err := pool.Exec(ctx, `UPDATE user_accounts SET status = 'DISABLED' WHERE id = $1`, disabledUser.ID); err != nil {
		t.Fatalf("disable candidate fixture: %v", err)
	}
	var displayCandidate struct {
		ID string `json:"id"`
	}
	request(t, router, http.MethodPost, "/v1/admin/users", adminCookie,
		`{"username":"charlie","display_name":"Human Label","temporary_password":"Charlie-temp-pass-1234"}`,
		http.StatusCreated, &displayCandidate)
	var isolatedTenant struct {
		ID string `json:"id"`
	}
	request(t, router, http.MethodPost, "/v1/admin/tenants", adminCookie,
		fmt.Sprintf(`{"slug":"team-b","name":"Team B","owner_user_id":%q}`, displayCandidate.ID),
		http.StatusCreated, &isolatedTenant)
	request(t, router, http.MethodGet, "/v1/tenants/"+isolatedTenant.ID+"/members", aliceCookie, "",
		http.StatusForbidden, nil)
	request(t, router, http.MethodGet, "/v1/tenants/"+isolatedTenant.ID+"/member-candidates?query=bob", aliceCookie, "",
		http.StatusForbidden, nil)
	request(t, router, http.MethodPost, "/v1/tenants/"+isolatedTenant.ID+"/members", aliceCookie,
		fmt.Sprintf(`{"user_id":%q}`, member.ID), http.StatusForbidden, nil)
	var candidates struct {
		Candidates []struct {
			UserID      string `json:"user_id"`
			Username    string `json:"username"`
			DisplayName string `json:"display_name"`
		} `json:"candidates"`
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
		Total  int `json:"total"`
	}
	candidatePath := "/v1/tenants/" + provisioned.ID + "/member-candidates"
	request(t, router, http.MethodGet, candidatePath+"?query=BoB&offset=0&limit=20", aliceCookie, "",
		http.StatusOK, &candidates)
	if candidates.Offset != 0 || candidates.Limit != 20 || candidates.Total != 1 ||
		len(candidates.Candidates) != 1 || candidates.Candidates[0].UserID != member.ID ||
		candidates.Candidates[0].Username != "bob" || candidates.Candidates[0].DisplayName != "Bob" {
		t.Fatalf("member candidates = %#v", candidates)
	}
	request(t, router, http.MethodGet, candidatePath+"?query=bob&offset=1&limit=20", aliceCookie, "",
		http.StatusOK, &candidates)
	if candidates.Offset != 1 || candidates.Total != 1 || len(candidates.Candidates) != 0 {
		t.Fatalf("member candidate page beyond end = %#v", candidates)
	}
	request(t, router, http.MethodGet, candidatePath+"?query=LABEL&offset=0&limit=20", aliceCookie, "",
		http.StatusOK, &candidates)
	if candidates.Total != 1 || len(candidates.Candidates) != 1 ||
		candidates.Candidates[0].UserID != displayCandidate.ID {
		t.Fatalf("display-name candidates = %#v", candidates)
	}
	request(t, router, http.MethodGet, candidatePath+"?query=%20%20", aliceCookie, "",
		http.StatusBadRequest, nil)
	request(t, router, http.MethodGet, candidatePath+"?query=bob&offset=0&limit=20", adminCookie, "",
		http.StatusForbidden, nil)
	request(t, router, http.MethodGet, candidatePath+"?query=alice&offset=0&limit=20", aliceCookie, "",
		http.StatusOK, &candidates)
	if candidates.Total != 0 || len(candidates.Candidates) != 0 {
		t.Fatalf("existing owner appeared as candidate = %#v", candidates)
	}
	request(t, router, http.MethodPost, "/v1/tenants/"+provisioned.ID+"/members", aliceCookie,
		fmt.Sprintf(`{"user_id":%q}`, member.ID), http.StatusCreated, nil)
	request(t, router, http.MethodPost, "/v1/tenants/"+provisioned.ID+"/members", aliceCookie,
		fmt.Sprintf(`{"user_id":%q}`, member.ID), http.StatusConflict, nil)
	request(t, router, http.MethodGet, candidatePath+"?query=bob&offset=0&limit=20", aliceCookie, "",
		http.StatusOK, &candidates)
	if candidates.Total != 0 || len(candidates.Candidates) != 0 {
		t.Fatalf("added member remained a candidate = %#v", candidates)
	}
	bobCookie := login(t, router, "bob", "Bob-temp-pass-1234", true)
	request(t, router, http.MethodPost, "/v1/me/change-password", bobCookie,
		`{"current_password":"Bob-temp-pass-1234","new_password":"Bob-final-pass-5678"}`,
		http.StatusNoContent, nil)
	var members struct {
		Members []json.RawMessage `json:"members"`
	}
	request(t, router, http.MethodGet, "/v1/tenants/"+provisioned.ID+"/members", aliceCookie, "",
		http.StatusOK, &members)
	if len(members.Members) != 2 {
		t.Fatalf("tenant members = %#v", members.Members)
	}
	request(t, router, http.MethodGet, "/v1/tenants/"+provisioned.ID+"/members", bobCookie, "",
		http.StatusForbidden, nil)
	request(t, router, http.MethodGet, candidatePath+"?query=alice", bobCookie, "",
		http.StatusForbidden, nil)
	request(t, router, http.MethodPost, "/v1/admin/tenants", aliceCookie,
		fmt.Sprintf(`{"slug":"forbidden-team","name":"Forbidden Team","owner_user_id":%q}`, user.ID),
		http.StatusForbidden, nil)

	testAgentV1Lifecycle(t, ctx, router, pool, provisioned.ID, aliceCookie, bobCookie, adminCookie)
	testRuntimeProfileV1Lifecycle(
		t, ctx, router, pool, provisioned.ID, aliceCookie, bobCookie, adminCookie,
	)
	testDeploymentV1Lifecycle(
		t, ctx, router, pool, provisioned.ID, aliceCookie, bobCookie, adminCookie,
	)
	t.Run("ChannelHTTPAndMTLS", func(t *testing.T) {
		testChannelHTTP(t, ctx, router, pool, channelModule, provisioned.ID, aliceCookie, bobCookie, adminCookie)
	})
	t.Run("TelegramPreflightHTTPAndMTLS", func(t *testing.T) {
		testTelegramPreflightHTTP(t, ctx, router, pool, channelModule, identityModule.AuthenticationMiddleware(), provisioned.ID, user.ID, member.ID, aliceCookie, bobCookie, adminCookie)
	})
	t.Run("ReceiveModeHTTPAndLegacyRecovery", func(t *testing.T) { testReceiveModeHTTP(t, ctx, router, pool, provisioned.ID, aliceCookie, bobCookie) })
}

func testAgentV1Lifecycle(
	t *testing.T,
	ctx context.Context,
	router http.Handler,
	pool *pgxpool.Pool,
	tenantID string,
	ownerCookie, memberCookie, outsiderCookie *http.Cookie,
) {
	t.Helper()
	base := "/v1/tenants/" + tenantID + "/agents"
	var created struct {
		Agent struct {
			ID                  string `json:"id"`
			Name                string `json:"name"`
			LatestVersionNumber *int64 `json:"latest_version_number"`
		} `json:"agent"`
		Draft struct {
			Revision int64           `json:"revision"`
			Spec     json.RawMessage `json:"spec"`
		} `json:"draft"`
	}
	request(t, router, http.MethodPost, base, ownerCookie,
		`{"name":"Support","description":"Answers users"}`,
		http.StatusCreated, &created)
	if created.Agent.ID == "" || created.Agent.Name != "Support" ||
		created.Agent.LatestVersionNumber != nil || created.Draft.Revision != 1 ||
		string(created.Draft.Spec) != `{}` {
		t.Fatalf("created Agent = %#v", created)
	}
	agentPath := base + "/" + created.Agent.ID

	var page struct {
		Agents []json.RawMessage `json:"agents"`
		Total  int               `json:"total"`
	}
	request(t, router, http.MethodGet, base+"?offset=0&limit=20", ownerCookie, "",
		http.StatusOK, &page)
	if page.Total != 1 || len(page.Agents) != 1 {
		t.Fatalf("Agent page = %#v", page)
	}
	request(t, router, http.MethodGet, agentPath, ownerCookie, "", http.StatusOK, nil)
	request(t, router, http.MethodGet, agentPath, memberCookie, "", http.StatusOK, nil)
	request(t, router, http.MethodPatch, agentPath, ownerCookie,
		`{"name":"Production Support"}`, http.StatusOK, nil)
	request(t, router, http.MethodGet, agentPath+"/draft", ownerCookie, "", http.StatusOK, nil)

	// A Draft may be incomplete after L0 safety validation.
	var incompleteDraft struct {
		Revision int64 `json:"revision"`
	}
	request(t, router, http.MethodPut, agentPath+"/draft", ownerCookie,
		`{"expected_revision":1,"spec":{"nodes":{}}}`,
		http.StatusOK, &incompleteDraft)
	if incompleteDraft.Revision != 2 {
		t.Fatalf("incomplete Draft revision = %d", incompleteDraft.Revision)
	}
	var invalidReport struct {
		Valid         bool  `json:"valid"`
		DraftRevision int64 `json:"draft_revision"`
	}
	request(t, router, http.MethodPost, agentPath+"/draft/validate", ownerCookie,
		`{"expected_revision":2}`, http.StatusOK, &invalidReport)
	if invalidReport.Valid || invalidReport.DraftRevision != 2 {
		t.Fatalf("invalid report = %#v", invalidReport)
	}
	request(t, router, http.MethodPost, agentPath+"/versions", ownerCookie,
		`{"expected_revision":2}`, http.StatusUnprocessableEntity, nil)

	spec := json.RawMessage(`{
		"schema_version":"v1",
		"root":"assistant",
		"requirements":{"models":{"primary":{"capabilities":["chat"]}},"tools":{},"knowledge":{}},
		"nodes":{"assistant":{"kind":"llm","instruction":"Answer accurately.","model_slot":"primary","tool_slots":[],"knowledge_slots":[]}}
	}`)
	saveBody, err := json.Marshal(map[string]any{"expected_revision": 2, "spec": spec})
	if err != nil {
		t.Fatal(err)
	}
	var validDraft struct {
		Revision int64 `json:"revision"`
	}
	request(t, router, http.MethodPut, agentPath+"/draft", ownerCookie,
		string(saveBody), http.StatusOK, &validDraft)
	if validDraft.Revision != 3 {
		t.Fatalf("valid Draft revision = %d", validDraft.Revision)
	}
	request(t, router, http.MethodPut, agentPath+"/draft", ownerCookie,
		string(saveBody), http.StatusConflict, nil)

	var validReport struct {
		Valid bool `json:"valid"`
	}
	request(t, router, http.MethodPost, agentPath+"/draft/validate", ownerCookie,
		`{"expected_revision":3}`, http.StatusOK, &validReport)
	if !validReport.Valid {
		t.Fatal("valid AgentSpec was rejected")
	}
	var firstPublish struct {
		Version struct {
			ID                  string          `json:"id"`
			VersionNumber       int64           `json:"version_number"`
			SourceDraftRevision int64           `json:"source_draft_revision"`
			Spec                json.RawMessage `json:"spec"`
			SpecDigest          string          `json:"spec_digest"`
		} `json:"version"`
	}
	request(t, router, http.MethodPost, agentPath+"/versions", ownerCookie,
		`{"expected_revision":3}`, http.StatusCreated, &firstPublish)
	if firstPublish.Version.ID == "" || firstPublish.Version.VersionNumber != 1 ||
		firstPublish.Version.SourceDraftRevision != 3 ||
		!strings.HasPrefix(firstPublish.Version.SpecDigest, "sha256:") {
		t.Fatalf("published Version = %#v", firstPublish.Version)
	}
	var repeatPublish struct {
		Version struct {
			ID string `json:"id"`
		} `json:"version"`
	}
	request(t, router, http.MethodPost, agentPath+"/versions", ownerCookie,
		`{"expected_revision":3}`, http.StatusOK, &repeatPublish)
	if repeatPublish.Version.ID != firstPublish.Version.ID {
		t.Fatalf("idempotent Version ID = %q, want %q", repeatPublish.Version.ID, firstPublish.Version.ID)
	}
	var versions struct {
		Versions []json.RawMessage `json:"versions"`
		Total    int               `json:"total"`
	}
	request(t, router, http.MethodGet, agentPath+"/versions", ownerCookie, "",
		http.StatusOK, &versions)
	if versions.Total != 1 || len(versions.Versions) != 1 {
		t.Fatalf("Version page = %#v", versions)
	}
	var fetched struct {
		Spec       json.RawMessage `json:"spec"`
		SpecDigest string          `json:"spec_digest"`
	}
	request(t, router, http.MethodGet, agentPath+"/versions/1", ownerCookie, "",
		http.StatusOK, &fetched)
	if fetched.SpecDigest != firstPublish.Version.SpecDigest ||
		string(fetched.Spec) != string(firstPublish.Version.Spec) {
		t.Fatalf("fetched immutable Version = %#v", fetched)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE agent_versions SET spec_jsonb = '{}'::jsonb
		WHERE tenant_id = $1 AND agent_id = $2 AND version_number = 1
	`, tenantID, created.Agent.ID); err == nil {
		t.Fatal("database allowed an AgentVersion update")
	}

	// Updating the Draft cannot alter the published snapshot.
	request(t, router, http.MethodPut, agentPath+"/draft", ownerCookie,
		`{"expected_revision":3,"spec":{"nodes":{}}}`,
		http.StatusOK, nil)
	var fetchedAgain struct {
		Spec       json.RawMessage `json:"spec"`
		SpecDigest string          `json:"spec_digest"`
	}
	request(t, router, http.MethodGet, agentPath+"/versions/1", ownerCookie, "",
		http.StatusOK, &fetchedAgain)
	if fetchedAgain.SpecDigest != fetched.SpecDigest || string(fetchedAgain.Spec) != string(fetched.Spec) {
		t.Fatal("published Version changed after Draft update")
	}

	// A platform operator who is not a Tenant member gets no Agent access.
	request(t, router, http.MethodGet, agentPath, outsiderCookie, "", http.StatusForbidden, nil)

	// A failed latest-version update must roll back the inserted Version.
	secondSaveBody, err := json.Marshal(map[string]any{"expected_revision": 4, "spec": spec})
	if err != nil {
		t.Fatal(err)
	}
	request(t, router, http.MethodPut, agentPath+"/draft", ownerCookie,
		string(secondSaveBody), http.StatusOK, nil)
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_second_agent_version() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.latest_version_number > 1 THEN
				RAISE EXCEPTION 'injected latest version update failure';
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER reject_second_agent_version
		BEFORE UPDATE ON agents
		FOR EACH ROW EXECUTE FUNCTION reject_second_agent_version();
	`); err != nil {
		t.Fatal(err)
	}
	request(t, router, http.MethodPost, agentPath+"/versions", ownerCookie,
		`{"expected_revision":5}`, http.StatusInternalServerError, nil)
	var versionCount int
	if err := pool.QueryRow(ctx, "SELECT count(*)::int FROM agent_versions WHERE tenant_id=$1 AND agent_id=$2", tenantID, created.Agent.ID).Scan(&versionCount); err != nil {
		t.Fatal(err)
	}
	if versionCount != 1 {
		t.Fatalf("failed publication left %d Versions, want 1", versionCount)
	}
	if _, err := pool.Exec(ctx, `
		DROP TRIGGER reject_second_agent_version ON agents;
		DROP FUNCTION reject_second_agent_version();
	`); err != nil {
		t.Fatal(err)
	}
	var secondPublish struct {
		Version struct {
			VersionNumber int64 `json:"version_number"`
		} `json:"version"`
	}
	request(t, router, http.MethodPost, agentPath+"/versions", ownerCookie,
		`{"expected_revision":5}`, http.StatusCreated, &secondPublish)
	if secondPublish.Version.VersionNumber != 2 {
		t.Fatalf("second Version number = %d", secondPublish.Version.VersionNumber)
	}

	// A failed initial-Draft insert must roll back the Agent row created by the CTE.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_agent_draft() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'injected Agent Draft failure';
		END
		$$;
		CREATE TRIGGER reject_agent_draft
		BEFORE INSERT ON agent_drafts
		FOR EACH ROW EXECUTE FUNCTION reject_agent_draft();
	`); err != nil {
		t.Fatal(err)
	}
	request(t, router, http.MethodPost, base, ownerCookie,
		`{"name":"Must Roll Back"}`, http.StatusInternalServerError, nil)
	if _, err := pool.Exec(ctx, `
		DROP TRIGGER reject_agent_draft ON agent_drafts;
		DROP FUNCTION reject_agent_draft();
	`); err != nil {
		t.Fatal(err)
	}

	var agentCount, draftCount int
	if err := pool.QueryRow(ctx, "SELECT count(*)::int FROM agents WHERE tenant_id=$1", tenantID).Scan(&agentCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*)::int FROM agent_drafts WHERE tenant_id=$1", tenantID).Scan(&draftCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*)::int FROM agent_versions WHERE tenant_id=$1 AND agent_id=$2", tenantID, created.Agent.ID).Scan(&versionCount); err != nil {
		t.Fatal(err)
	}
	if agentCount != 1 || draftCount != 1 || versionCount != 2 {
		t.Fatalf("persistence counts = agent:%d draft:%d version:%d", agentCount, draftCount, versionCount)
	}
}

type tenantAccess struct {
	tenants *tenantapp.Service
}

func (access tenantAccess) IsActiveMember(ctx context.Context, tenantID, userID string) (bool, error) {
	_, err := access.tenants.GetTenant(ctx, tenantID, userID)
	if errors.Is(err, tenantapp.ErrTenantForbidden) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (access tenantAccess) IsActiveOwner(ctx context.Context, tenantID, userID string) (bool, error) {
	membership, err := access.tenants.GetTenant(ctx, tenantID, userID)
	if errors.Is(err, tenantapp.ErrTenantForbidden) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return membership.Membership.Role == "OWNER", nil
}

type accountLookup struct {
	accounts *identityapp.AccountManagement
}

func (lookup accountLookup) IsActiveAccount(ctx context.Context, userID string) (bool, error) {
	account, err := lookup.accounts.GetAccount(ctx, userID)
	if err != nil {
		return false, err
	}
	return account.CanLogin(), nil
}

func login(
	t *testing.T,
	handler http.Handler,
	username, password string,
	wantRestricted bool,
) *http.Cookie {
	t.Helper()
	var response struct {
		PasswordChangeRequired bool `json:"password_change_required"`
	}
	recorder := request(t, handler, http.MethodPost, "/v1/auth/login", nil,
		fmt.Sprintf(`{"username":%q,"password":%q}`, username, password),
		http.StatusOK, &response)
	if response.PasswordChangeRequired != wantRestricted {
		t.Fatalf("password_change_required = %v", response.PasswordChangeRequired)
	}
	for _, cookie := range recorder.Result().Cookies() {
		if cookie.Name == "control_session" {
			return cookie
		}
	}
	t.Fatal("session cookie was not set")
	return nil
}

func request(
	t *testing.T,
	handler http.Handler,
	method, path string,
	cookie *http.Cookie,
	body string,
	wantStatus int,
	destination any,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if cookie != nil {
		request.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != wantStatus {
		t.Fatalf("%s %s: status = %d, body = %s", method, path, recorder.Code, recorder.Body.String())
	}
	if destination != nil {
		decoder := json.NewDecoder(bytes.NewReader(recorder.Body.Bytes()))
		if err := decoder.Decode(destination); err != nil {
			t.Fatalf("decode %s %s response: %v", method, path, err)
		}
	}
	return recorder
}
