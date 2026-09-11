package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testDeploymentV1Lifecycle(
	t *testing.T,
	ctx context.Context,
	router http.Handler,
	pool *pgxpool.Pool,
	tenantID string,
	ownerCookie, memberCookie, outsiderCookie *http.Cookie,
) {
	t.Helper()
	agentID := createDeploymentAgentVersion(t, router, tenantID, ownerCookie)
	profileID := createDeploymentProfileRevision(
		t, router, tenantID, ownerCookie, deploymentProfileConfig("gpt-deployment-v1", "matching"),
	)

	base := "/v1/tenants/" + tenantID + "/deployments"
	var created struct {
		Deployment deploymentHTTPView `json:"deployment"`
	}
	createBody := `{"name":"Support Production","description":"Fixed deployment inputs"}`
	createdRecorder := deploymentRequest(
		t, router, http.MethodPost, base, ownerCookie, "deployment-create-v1", createBody,
		http.StatusCreated, &created,
	)
	if created.Deployment.ID == "" || created.Deployment.MetadataRevision != 1 ||
		created.Deployment.LatestRevisionNumber != nil {
		t.Fatalf("created Deployment = %#v", created.Deployment)
	}
	replayedCreate := deploymentRequest(
		t, router, http.MethodPost, base, ownerCookie, "deployment-create-v1", createBody,
		http.StatusOK, nil,
	)
	if createdRecorder.Body.String() != replayedCreate.Body.String() {
		t.Fatalf("create receipt replay changed response:\ncreated=%s\nreplayed=%s",
			createdRecorder.Body.String(), replayedCreate.Body.String())
	}

	path := base + "/" + created.Deployment.ID
	request(t, router, http.MethodGet, path, memberCookie, "", http.StatusOK, nil)
	request(t, router, http.MethodGet, path, outsiderCookie, "", http.StatusForbidden, nil)
	var list struct {
		Deployments []deploymentHTTPView `json:"deployments"`
		Total       int                  `json:"total"`
	}
	request(t, router, http.MethodGet, base+"?offset=0&limit=20", memberCookie, "", http.StatusOK, &list)
	if list.Total < 1 || !containsDeployment(list.Deployments, created.Deployment.ID) {
		t.Fatalf("Deployment page does not contain %q: %#v", created.Deployment.ID, list)
	}
	var patched deploymentHTTPView
	request(t, router, http.MethodPatch, path, memberCookie,
		`{"expected_metadata_revision":1,"name":"Support Production Renamed"}`,
		http.StatusOK, &patched)
	if patched.Name != "Support Production Renamed" || patched.MetadataRevision != 2 {
		t.Fatalf("patched Deployment = %#v", patched)
	}
	request(t, router, http.MethodPatch, path, memberCookie,
		`{"expected_metadata_revision":1,"description":"stale"}`,
		http.StatusConflict, nil)

	input := deploymentInputJSON(t, agentID, 1, profileID, 1)
	var validation deploymentValidationHTTPView
	request(t, router, http.MethodPost, path+"/validate", memberCookie, input,
		http.StatusOK, &validation)
	if !validation.Valid || len(validation.Diagnostics) != 0 {
		t.Fatalf("valid Deployment input report = %#v", validation)
	}
	publishBody := fmt.Sprintf(`{"expected_latest_revision_number":null,"input":%s}`, input)
	deploymentRequest(t, router, http.MethodPost, path+"/revisions", memberCookie,
		"member-cannot-publish", publishBody, http.StatusForbidden, nil)

	var firstPublish deploymentPublishHTTPView
	firstRecorder := deploymentRequest(
		t, router, http.MethodPost, path+"/revisions", ownerCookie,
		"deployment-publish-v1", publishBody, http.StatusCreated, &firstPublish,
	)
	assertDeploymentPublication(t, firstPublish, 1, agentID, 1, profileID, 1)
	assertDeploymentPublicSafe(t, firstRecorder.Body.Bytes())
	assertNodeResourceIsolation(t, firstPublish.Revision.ManifestView)
	assertExtraProfileResourceExcluded(t, firstPublish.Revision.ManifestView)

	// AC-01: two independent deployments use the same AgentVersion with
	// distinct ProfileRevision identities, not an implicit environment overlay.
	testProfileID := createDeploymentProfileRevision(t, router, tenantID, ownerCookie,
		deploymentProfileConfig("gpt-deployment-test", "matching"))
	var testDeployment struct {
		Deployment deploymentHTTPView `json:"deployment"`
	}
	deploymentRequest(t, router, http.MethodPost, base, ownerCookie, "deployment-create-test",
		`{"name":"Support Test"}`, http.StatusCreated, &testDeployment)
	testInput := deploymentInputJSON(t, agentID, 1, testProfileID, 1)
	var testPublished deploymentPublishHTTPView
	deploymentRequest(t, router, http.MethodPost, base+"/"+testDeployment.Deployment.ID+"/revisions", ownerCookie,
		"deployment-publish-test", fmt.Sprintf(`{"expected_latest_revision_number":null,"input":%s}`, testInput),
		http.StatusCreated, &testPublished)
	assertDeploymentPublication(t, testPublished, 1, agentID, 1, testProfileID, 1)
	if testDeployment.Deployment.ID == created.Deployment.ID || testProfileID == profileID ||
		testPublished.Revision.ManifestDigest == firstPublish.Revision.ManifestDigest {
		t.Fatal("independent test and production publications share an identity or digest")
	}
	assertDeploymentModel(t, firstPublish.Revision.ManifestView, "gpt-deployment-v1")
	assertDeploymentModel(t, testPublished.Revision.ManifestView, "gpt-deployment-test")
	var distinctCredentials int
	if err := pool.QueryRow(ctx, `SELECT count(DISTINCT content_jsonb->'resources'->'models'->'primary'->'credential'->>'credential_id')
		FROM runtime_manifests m JOIN deployment_revisions r ON r.tenant_id=m.tenant_id AND r.id=m.deployment_revision_id
		WHERE m.tenant_id=$1 AND r.deployment_id IN ($2,$3)`,
		tenantID, created.Deployment.ID, testDeployment.Deployment.ID).Scan(&distinctCredentials); err != nil {
		t.Fatal(err)
	}
	if distinctCredentials != 2 {
		t.Fatalf("independent Profile credentials = %d, want 2", distinctCredentials)
	}

	var replayedPublish deploymentPublishHTTPView
	replayRecorder := deploymentRequest(
		t, router, http.MethodPost, path+"/revisions", ownerCookie,
		"deployment-publish-v1", publishBody, http.StatusOK, &replayedPublish,
	)
	if firstRecorder.Body.String() != replayRecorder.Body.String() {
		t.Fatalf("publish receipt replay changed response:\ncreated=%s\nreplayed=%s",
			firstRecorder.Body.String(), replayRecorder.Body.String())
	}

	var revisionPage struct {
		Revisions []json.RawMessage `json:"revisions"`
		Total     int               `json:"total"`
	}
	request(t, router, http.MethodGet, path+"/revisions?offset=0&limit=20", memberCookie,
		"", http.StatusOK, &revisionPage)
	if revisionPage.Total != 1 || len(revisionPage.Revisions) != 1 {
		t.Fatalf("Deployment revision page = %#v", revisionPage)
	}
	var summary map[string]json.RawMessage
	if err := json.Unmarshal(revisionPage.Revisions[0], &summary); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"input", "manifest_view", "content"} {
		if _, exists := summary[forbidden]; exists {
			t.Fatalf("summary contains %q: %s", forbidden, revisionPage.Revisions[0])
		}
	}

	var firstRead deploymentRevisionHTTPView
	firstReadRecorder := request(t, router, http.MethodGet, path+"/revisions/1", memberCookie,
		"", http.StatusOK, &firstRead)
	assertDeploymentPublicSafe(t, firstReadRecorder.Body.Bytes())
	if firstRead.ID != firstPublish.Revision.ID || firstRead.ManifestDigest != firstPublish.Revision.ManifestDigest {
		t.Fatalf("full immutable read = %#v, publish = %#v", firstRead, firstPublish.Revision)
	}

	var outboxStatus, outboxPayload string
	if err := pool.QueryRow(ctx, `
		SELECT status, payload_jsonb::text
		FROM control_outbox
		WHERE tenant_id=$1 AND aggregate_type='deployment' AND aggregate_id=$2
	`, tenantID, created.Deployment.ID).Scan(&outboxStatus, &outboxPayload); err != nil {
		t.Fatal(err)
	}
	if outboxStatus != "PENDING" || !strings.Contains(outboxPayload, "crd_") {
		t.Fatalf("Control Outbox status/internal manifest = %q/%s", outboxStatus, outboxPayload)
	}
	assertNoDeploymentCredentialValue(t, []byte(outboxPayload))
	var receiptResult string
	if err := pool.QueryRow(ctx, `
		SELECT result_jsonb::text
		FROM deployment_command_receipts
		WHERE tenant_id=$1 AND operation='publish' AND scope_id=$2
	`, tenantID, created.Deployment.ID).Scan(&receiptResult); err != nil {
		t.Fatal(err)
	}
	assertDeploymentPublicSafe(t, []byte(receiptResult))

	// Publishing a new ProfileRevision never mutates an existing
	// DeploymentRevision. A later deployment publication explicitly selects the
	// new revision while retaining the same AgentVersion.
	profilePath := "/v1/tenants/" + tenantID + "/runtime-profiles/" + profileID
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie,
		"deployment-profile-revision-2",
		runtimeProfileDraftBody(t, 2, deploymentProfileConfig("gpt-deployment-v1-updated", "matching"), ""),
		http.StatusOK, nil)
	var secondProfile runtimeProfilePublishResponse
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":3}`, http.StatusCreated, &secondProfile)
	if secondProfile.Revision.RevisionNumber != 2 {
		t.Fatalf("second ProfileRevision = %#v", secondProfile.Revision)
	}
	unchanged := request(t, router, http.MethodGet, path+"/revisions/1", memberCookie,
		"", http.StatusOK, nil)
	if firstReadRecorder.Body.String() != unchanged.Body.String() {
		t.Fatalf("existing DeploymentRevision changed after Profile publish:\nbefore=%s\nafter=%s",
			firstReadRecorder.Body.String(), unchanged.Body.String())
	}

	secondInput := deploymentInputJSON(t, agentID, 1, profileID, 2)
	secondPublishBody := fmt.Sprintf(`{"expected_latest_revision_number":1,"input":%s}`, secondInput)
	var secondPublish deploymentPublishHTTPView
	deploymentRequest(t, router, http.MethodPost, path+"/revisions", ownerCookie,
		"deployment-publish-v2", secondPublishBody, http.StatusCreated, &secondPublish)
	assertDeploymentPublication(t, secondPublish, 2, agentID, 1, profileID, 2)
	if secondPublish.Revision.ManifestDigest == firstPublish.Revision.ManifestDigest {
		t.Fatal("different ProfileRevision did not change the compiled Manifest digest")
	}

	// A ProfileRevision without the declared same-name tool produces a stable
	// validation diagnostic and cannot be published as a DeploymentRevision.
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie,
		"deployment-profile-missing-tool",
		runtimeProfileDraftBody(t, 3, deploymentProfileConfig("gpt-deployment-v1-updated", "missing"), ""),
		http.StatusOK, nil)
	var missingProfile runtimeProfilePublishResponse
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":4}`, http.StatusCreated, &missingProfile)
	missingInput := deploymentInputJSON(t, agentID, 1, profileID, 3)
	var missingReport deploymentValidationHTTPView
	request(t, router, http.MethodPost, path+"/validate", memberCookie, missingInput,
		http.StatusOK, &missingReport)
	assertDeploymentDiagnostic(t, missingReport, "DEPLOYMENT_RESOURCE_MISSING")
	missingPublishBody := fmt.Sprintf(`{"expected_latest_revision_number":2,"input":%s}`, missingInput)
	deploymentRequest(t, router, http.MethodPost, path+"/revisions", ownerCookie,
		"deployment-publish-missing", missingPublishBody, http.StatusUnprocessableEntity, nil)

	// Exact same-name matching does not accept a resource with the wrong
	// capability, even though a similarly configured resource exists.
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie,
		"deployment-profile-capability-mismatch",
		runtimeProfileDraftBody(t, 4, deploymentProfileConfig("gpt-deployment-v1-updated", "mismatch"), ""),
		http.StatusOK, nil)
	var mismatchProfile runtimeProfilePublishResponse
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":5}`, http.StatusCreated, &mismatchProfile)
	mismatchInput := deploymentInputJSON(t, agentID, 1, profileID, 4)
	var mismatchReport deploymentValidationHTTPView
	request(t, router, http.MethodPost, path+"/validate", memberCookie, mismatchInput,
		http.StatusOK, &mismatchReport)
	assertDeploymentDiagnostic(t, mismatchReport, "DEPLOYMENT_CAPABILITY_MISMATCH")
}

type deploymentHTTPView struct {
	ID                   string `json:"id"`
	Name                 string `json:"name"`
	MetadataRevision     int64  `json:"metadata_revision"`
	LatestRevisionNumber *int64 `json:"latest_revision_number"`
}

type deploymentValidationHTTPView struct {
	Valid       bool `json:"valid"`
	Diagnostics []struct {
		Code string `json:"code"`
	} `json:"diagnostics"`
}

type deploymentPublishHTTPView struct {
	Revision   deploymentRevisionHTTPView   `json:"revision"`
	Validation deploymentValidationHTTPView `json:"validation"`
}

type deploymentRevisionHTTPView struct {
	ID                    string          `json:"id"`
	RevisionNumber        int64           `json:"revision_number"`
	AgentID               string          `json:"agent_id"`
	AgentVersionNumber    int64           `json:"agent_version_number"`
	ProfileID             string          `json:"profile_id"`
	ProfileRevisionNumber int64           `json:"profile_revision_number"`
	ManifestDigest        string          `json:"manifest_digest"`
	ManifestView          json.RawMessage `json:"manifest_view"`
}

func createDeploymentAgentVersion(t *testing.T, router http.Handler, tenantID string, owner *http.Cookie) string {
	t.Helper()
	base := "/v1/tenants/" + tenantID + "/agents"
	var created struct {
		Agent struct {
			ID string `json:"id"`
		} `json:"agent"`
	}
	request(t, router, http.MethodPost, base, owner,
		`{"name":"Deployment Source Agent"}`, http.StatusCreated, &created)
	if created.Agent.ID == "" {
		t.Fatal("Deployment source Agent has no ID")
	}
	spec := json.RawMessage(`{
		"schema_version":"v1",
		"root":"pipeline",
		"requirements":{
			"models":{"primary":{"capabilities":["chat","tool_call"]}},
			"tools":{"search":{"capability":"web.search"}},
			"knowledge":{}
		},
		"nodes":{
			"pipeline":{"kind":"sequence","children":["with_search","without_search"]},
			"with_search":{"kind":"llm","instruction":"Search when needed.","model_slot":"primary","tool_slots":["search"],"knowledge_slots":[]},
			"without_search":{"kind":"llm","instruction":"Answer without tools.","model_slot":"primary","tool_slots":[],"knowledge_slots":[]}
		}
	}`)
	body, err := json.Marshal(map[string]any{"expected_revision": 1, "spec": spec})
	if err != nil {
		t.Fatal(err)
	}
	request(t, router, http.MethodPut, base+"/"+created.Agent.ID+"/draft", owner,
		string(body), http.StatusOK, nil)
	var published struct {
		Version struct {
			VersionNumber int64 `json:"version_number"`
		} `json:"version"`
	}
	request(t, router, http.MethodPost, base+"/"+created.Agent.ID+"/versions", owner,
		`{"expected_revision":2}`, http.StatusCreated, &published)
	if published.Version.VersionNumber != 1 {
		t.Fatalf("Deployment source AgentVersion = %#v", published.Version)
	}
	return created.Agent.ID
}

func createDeploymentProfileRevision(
	t *testing.T, router http.Handler, tenantID string, owner *http.Cookie, config string,
) string {
	t.Helper()
	base := "/v1/tenants/" + tenantID + "/runtime-profiles"
	var created struct {
		Profile struct {
			ID string `json:"id"`
		} `json:"profile"`
	}
	request(t, router, http.MethodPost, base, owner,
		`{"name":"Deployment Source Profile"}`, http.StatusCreated, &created)
	if created.Profile.ID == "" {
		t.Fatal("Deployment source Runtime Profile has no ID")
	}
	credentials := `{
		"models":{"primary":{"api_key":{"action":"replace","value":"deployment-test-model-credential"}}},
		"storage":{"session":{"dsn":{"action":"replace","value":"postgres://runtime_user:deployment-test-storage-credential@state.example.test:5432/agent_state?sslmode=require"}}}
	}`
	path := base + "/" + created.Profile.ID
	runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner,
		"deployment-profile-initial", runtimeProfileDraftBody(t, 1, config, credentials),
		http.StatusOK, nil)
	var published runtimeProfilePublishResponse
	request(t, router, http.MethodPost, path+"/revisions", owner,
		`{"expected_revision":2}`, http.StatusCreated, &published)
	if published.Revision.RevisionNumber != 1 {
		t.Fatalf("Deployment source ProfileRevision = %#v", published.Revision)
	}
	return created.Profile.ID
}

func deploymentProfileConfig(model, searchMode string) string {
	tools := map[string]any{
		"spare": map[string]any{
			"kind": "mcp_streamable_http", "server_url": "https://mcp.example.com/spare",
			"toolset_name": "web", "tool_name": "spare_search", "auth": map[string]any{"kind": "none"},
			"capability": "web.search",
		},
	}
	if searchMode != "missing" {
		tools["search"] = map[string]any{
			"kind": "mcp_streamable_http", "server_url": "https://mcp.example.com/rpc",
			"toolset_name": "web", "tool_name": "search_web", "auth": map[string]any{"kind": "none"},
			"capability": "web.search",
		}
	}
	modelCapabilities := []string{"chat", "tool_call"}
	if searchMode == "mismatch" {
		modelCapabilities = []string{"chat"}
	}
	config := map[string]any{
		"models": map[string]any{"primary": map[string]any{
			"kind": "openai_compatible", "model": model, "base_url": "https://api.openai.com/v1",
			"capabilities": modelCapabilities,
		}},
		"tools":     tools,
		"knowledge": map[string]any{},
		"storage": map[string]any{"session": map[string]any{
			"kind": "postgres_state", "destination": map[string]any{
				"host": "state.example.test", "port": 5432, "database": "agent_state",
				"username": "runtime_user", "sslmode": "require",
			},
		}},
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func deploymentInputJSON(t *testing.T, agentID string, agentVersion int64, profileID string, profileRevision int64) string {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"schema_version": "v1",
		"agent":          map[string]any{"agent_id": agentID, "version_number": agentVersion},
		"profile":        map[string]any{"profile_id": profileID, "revision_number": profileRevision},
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func deploymentRequest(
	t *testing.T,
	handler http.Handler,
	method, path string,
	cookie *http.Cookie,
	idempotencyKey, body string,
	wantStatus int,
	destination any,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != wantStatus {
		t.Fatalf("%s %s: status=%d want=%d body=%s headers=%v", method, path, recorder.Code, wantStatus, recorder.Body.String(), recorder.Header())
	}
	if destination != nil {
		if err := json.Unmarshal(recorder.Body.Bytes(), destination); err != nil {
			t.Fatalf("decode %s %s response: %v", method, path, err)
		}
	}
	assertNoDeploymentCredentialValue(t, recorder.Body.Bytes())
	return recorder
}

func containsDeployment(values []deploymentHTTPView, id string) bool {
	for _, value := range values {
		if value.ID == id {
			return true
		}
	}
	return false
}

func assertDeploymentPublication(
	t *testing.T,
	response deploymentPublishHTTPView,
	revision int64,
	agentID string,
	agentVersion int64,
	profileID string,
	profileRevision int64,
) {
	t.Helper()
	value := response.Revision
	if !response.Validation.Valid || value.ID == "" || value.RevisionNumber != revision ||
		value.AgentID != agentID || value.AgentVersionNumber != agentVersion ||
		value.ProfileID != profileID || value.ProfileRevisionNumber != profileRevision ||
		!strings.HasPrefix(value.ManifestDigest, "sha256:") || len(value.ManifestView) == 0 {
		t.Fatalf("Deployment publication = %#v", response)
	}
}

func assertDeploymentDiagnostic(t *testing.T, report deploymentValidationHTTPView, code string) {
	t.Helper()
	if report.Valid {
		t.Fatalf("validation unexpectedly succeeded: %#v", report)
	}
	for _, diagnostic := range report.Diagnostics {
		if diagnostic.Code == code {
			return
		}
	}
	t.Fatalf("validation diagnostics %#v do not contain %q", report.Diagnostics, code)
}

func assertNodeResourceIsolation(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var view struct {
		AgentPlan struct {
			Nodes map[string]struct {
				ToolResources   []string `json:"tool_resources"`
				CallableEntries []string `json:"callable_entries"`
			} `json:"nodes"`
		} `json:"agent_plan"`
	}
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	withTool := view.AgentPlan.Nodes["with_search"]
	withoutTool := view.AgentPlan.Nodes["without_search"]
	if len(withTool.ToolResources) != 1 || withTool.ToolResources[0] != "search" ||
		len(withTool.CallableEntries) != 1 || withTool.CallableEntries[0] != "tools/search" ||
		len(withoutTool.ToolResources) != 0 || len(withoutTool.CallableEntries) != 0 {
		t.Fatalf("node resource isolation = with:%#v without:%#v", withTool, withoutTool)
	}
}

func assertExtraProfileResourceExcluded(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var view struct {
		Resources struct {
			Tools map[string]json.RawMessage `json:"tools"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	if _, ok := view.Resources.Tools["search"]; !ok || len(view.Resources.Tools) != 1 {
		t.Fatalf("compiled tools include unselected Profile resources: %#v", view.Resources.Tools)
	}
}

func assertDeploymentPublicSafe(t *testing.T, data []byte) {
	t.Helper()
	assertNoDeploymentCredentialValue(t, data)
	lower := strings.ToLower(string(data))
	for _, forbidden := range []string{
		"crd_", "credential_id", "audience_digest", "\"purpose\"", "ciphertext", "nonce",
		"credential_revision", "association_token", "\"configured\"", "\"status\"",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("public Deployment response contains forbidden marker %q", forbidden)
		}
	}
}

func assertNoDeploymentCredentialValue(t *testing.T, data []byte) {
	t.Helper()
	for _, forbidden := range []string{
		"deployment-test-model-credential", "deployment-test-storage-credential",
	} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("Deployment data contains credential value marker %q", forbidden)
		}
	}
}

func assertDeploymentModel(t *testing.T, raw json.RawMessage, want string) {
	t.Helper()
	var view struct {
		Resources struct {
			Models map[string]struct {
				Model string `json:"model"`
			} `json:"models"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	if got := view.Resources.Models["primary"].Model; got != want {
		t.Fatalf("compiled model = %q, want %q", got, want)
	}
}
