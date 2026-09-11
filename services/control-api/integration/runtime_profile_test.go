package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const runtimeProfileConfigV1 = `{
  "models":{
    "primary":{
      "kind":"openai_compatible",
      "model":"gpt-4o-mini",
      "base_url":"https://api.openai.com/v1",
      "capabilities":["tool_call","chat"]
    }
  },
  "tools":{
    "search":{
      "kind":"mcp_streamable_http",
      "server_url":"https://mcp.example.com/rpc",
      "toolset_name":"web",
      "tool_name":"search_web",
      "auth":{"kind":"bearer"},
      "capability":"web.search"
    }
  },
  "knowledge":{
    "docs":{
      "kind":"qdrant_openai",
      "host":"qdrant.internal",
      "port":6334,
      "tls":true,
      "collection":"product_docs",
      "embedding":{
        "model":"text-embedding-3-small",
        "base_url":"https://api.openai.com/v1",
          "dimensions":1536
      }
    }
  },
  "storage":{
    "session":{"kind":"postgres_state","destination":{"host":"state.example.test","port":5432,"database":"agent_state","username":"runtime_user","sslmode":"require"}}
  }
}`

func testRuntimeProfileV1Lifecycle(
	t *testing.T,
	ctx context.Context,
	router http.Handler,
	pool *pgxpool.Pool,
	tenantID string,
	ownerCookie, memberCookie, outsiderCookie *http.Cookie,
) {
	t.Helper()
	base := "/v1/tenants/" + tenantID + "/runtime-profiles"
	var created struct {
		Profile struct {
			ID                   string `json:"id"`
			Name                 string `json:"name"`
			LatestRevisionNumber *int64 `json:"latest_revision_number"`
		} `json:"profile"`
		Draft struct {
			Revision int64                      `json:"draft_revision"`
			Config   map[string]json.RawMessage `json:"config"`
		} `json:"draft"`
	}
	request(t, router, http.MethodPost, base, ownerCookie,
		`{"name":"Production Runtime","description":"Reusable runtime resources"}`,
		http.StatusCreated, &created)
	if created.Profile.ID == "" || created.Profile.Name != "Production Runtime" ||
		created.Profile.LatestRevisionNumber != nil || created.Draft.Revision != 1 ||
		len(created.Draft.Config) != 4 {
		t.Fatalf("created Runtime Profile = %#v", created)
	}
	profilePath := base + "/" + created.Profile.ID
	var emptyReport struct {
		Valid bool `json:"valid"`
	}
	request(t, router, http.MethodPost, profilePath+"/draft/validate", ownerCookie,
		`{"expected_revision":1}`, http.StatusOK, &emptyReport)
	if emptyReport.Valid {
		t.Fatal("new empty Profile Draft was reported publishable")
	}
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":1}`, http.StatusUnprocessableEntity, nil)

	var page struct {
		RuntimeProfiles []json.RawMessage `json:"runtime_profiles"`
		Total           int               `json:"total"`
	}
	request(t, router, http.MethodGet, base+"?offset=0&limit=20", ownerCookie, "",
		http.StatusOK, &page)
	if page.Total != 1 || len(page.RuntimeProfiles) != 1 {
		t.Fatalf("Runtime Profile page = %#v", page)
	}
	request(t, router, http.MethodGet, profilePath, ownerCookie, "", http.StatusOK, nil)
	request(t, router, http.MethodGet, profilePath, memberCookie, "", http.StatusOK, nil)
	request(t, router, http.MethodPatch, profilePath, ownerCookie,
		`{"name":"Production Runtime V1"}`, http.StatusOK, nil)
	request(t, router, http.MethodGet, profilePath+"/draft", ownerCookie, "", http.StatusOK, nil)

	var incompleteDraft struct {
		Revision int64 `json:"draft_revision"`
	}
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie, "incomplete",
		runtimeProfileDraftBody(t, 1, runtimeProfileConfigV1, ""),
		http.StatusOK, &incompleteDraft)
	if incompleteDraft.Revision != 2 {
		t.Fatalf("incomplete Profile Draft revision = %d", incompleteDraft.Revision)
	}
	var invalidReport struct {
		Valid         bool  `json:"valid"`
		DraftRevision int64 `json:"draft_revision"`
	}
	request(t, router, http.MethodPost, profilePath+"/draft/validate", ownerCookie,
		`{"expected_revision":2}`, http.StatusOK, &invalidReport)
	if invalidReport.Valid || invalidReport.DraftRevision != 2 {
		t.Fatalf("invalid Runtime Profile report = %#v", invalidReport)
	}
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":2}`, http.StatusUnprocessableEntity, nil)

	saveBody := runtimeProfileDraftBody(t, 2, runtimeProfileConfigV1, runtimeProfileCredentialsV1)
	var validDraft struct {
		Revision int64 `json:"draft_revision"`
	}
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie, "initial-values",
		saveBody, http.StatusOK, &validDraft)
	if validDraft.Revision != 3 {
		t.Fatalf("valid Profile Draft revision = %d", validDraft.Revision)
	}
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie, "stale-save",
		saveBody, http.StatusConflict, nil)
	var validReport struct {
		Valid bool `json:"valid"`
	}
	request(t, router, http.MethodPost, profilePath+"/draft/validate", ownerCookie,
		`{"expected_revision":3}`, http.StatusOK, &validReport)
	if !validReport.Valid {
		t.Fatal("valid RuntimeProfileSpec was rejected")
	}

	var firstPublish runtimeProfilePublishResponse
	firstRecorder := request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":3}`, http.StatusCreated, &firstPublish)
	assertRuntimeProfileRevision(t, firstPublish, 1, 3)
	assertPublishHasNoValidation(t, firstRecorder)

	var immediateRetry runtimeProfilePublishResponse
	retryRecorder := request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":3}`, http.StatusOK, &immediateRetry)
	if immediateRetry.Revision.ID != firstPublish.Revision.ID {
		t.Fatalf("immediate idempotent Revision ID = %q, want %q",
			immediateRetry.Revision.ID, firstPublish.Revision.ID)
	}
	assertPublishHasNoValidation(t, retryRecorder)
	if firstRecorder.Body.String() != retryRecorder.Body.String() {
		t.Fatalf("idempotent publication changed complete response:\nfirst=%s\nretry=%s", firstRecorder.Body.String(), retryRecorder.Body.String())
	}

	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie, "advance-draft",
		runtimeProfileDraftBody(t, 3, strings.Replace(runtimeProfileConfigV1, "gpt-4o-mini", "changed-model", 1), ""), http.StatusOK, nil)
	var delayedRetry runtimeProfilePublishResponse
	delayedRecorder := request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":3}`, http.StatusOK, &delayedRetry)
	if firstRecorder.Body.String() != delayedRecorder.Body.String() {
		t.Fatalf("delayed idempotent publication changed complete response:\nfirst=%s\nretry=%s", firstRecorder.Body.String(), delayedRecorder.Body.String())
	}
	if delayedRetry.Revision.ID != firstPublish.Revision.ID {
		t.Fatalf("delayed idempotent Revision ID = %q, want %q",
			delayedRetry.Revision.ID, firstPublish.Revision.ID)
	}
	// Source Draft 2 failed validation and was never published, so it is stale.
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":2}`, http.StatusConflict, nil)

	var listed struct {
		Revisions []json.RawMessage `json:"revisions"`
		Total     int               `json:"total"`
	}
	request(t, router, http.MethodGet, profilePath+"/revisions", ownerCookie, "",
		http.StatusOK, &listed)
	if listed.Total != 1 || len(listed.Revisions) != 1 {
		t.Fatalf("Profile Revision page = %#v", listed)
	}
	var listedFields map[string]json.RawMessage
	if err := json.Unmarshal(listed.Revisions[0], &listedFields); err != nil {
		t.Fatal(err)
	}
	if _, exists := listedFields["spec"]; exists {
		t.Fatalf("Profile Revision list exposed canonical spec: %s", listed.Revisions[0])
	}
	var listedSummary struct {
		ID                  string `json:"id"`
		RevisionNumber      int64  `json:"revision_number"`
		SourceDraftRevision int64  `json:"source_draft_revision"`
		SchemaVersion       string `json:"schema_version"`
		SpecDigest          string `json:"spec_digest"`
	}
	if err := json.Unmarshal(listed.Revisions[0], &listedSummary); err != nil {
		t.Fatal(err)
	}
	if listedSummary.ID != firstPublish.Revision.ID || listedSummary.RevisionNumber != 1 ||
		listedSummary.SourceDraftRevision != 3 || listedSummary.SchemaVersion != "v1" ||
		listedSummary.SpecDigest != firstPublish.Revision.SpecDigest {
		t.Fatalf("Profile Revision summary = %#v", listedSummary)
	}
	var fetched runtimeProfileRevisionView
	request(t, router, http.MethodGet, profilePath+"/revisions/1", ownerCookie, "",
		http.StatusOK, &fetched)
	if fetched.SpecDigest != firstPublish.Revision.SpecDigest ||
		string(fetched.Config) != string(firstPublish.Revision.Config) {
		t.Fatalf("fetched immutable Profile Revision = %#v", fetched)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE runtime_profile_revisions SET spec_jsonb = '{}'::jsonb
		WHERE tenant_id = $1 AND profile_id = $2 AND revision_number = 1
	`, tenantID, created.Profile.ID); err == nil {
		t.Fatal("database allowed a ProfileRevision update")
	}
	if _, err := pool.Exec(ctx, `
		DELETE FROM runtime_profile_revisions
		WHERE tenant_id = $1 AND profile_id = $2 AND revision_number = 1
	`, tenantID, created.Profile.ID); err == nil {
		t.Fatal("database allowed a ProfileRevision delete")
	}

	// Pure Platform Operator identity has no implicit Tenant access.
	request(t, router, http.MethodGet, profilePath, outsiderCookie, "",
		http.StatusForbidden, nil)

	// A restricted Tenant member must rotate the temporary password first.
	var restrictedUser struct {
		ID string `json:"id"`
	}
	request(t, router, http.MethodPost, "/v1/admin/users", outsiderCookie,
		`{"username":"runtime-reviewer","display_name":"Runtime Reviewer","temporary_password":"Runtime-temp-pass-1234"}`,
		http.StatusCreated, &restrictedUser)
	request(t, router, http.MethodPost, "/v1/tenants/"+tenantID+"/members", ownerCookie,
		fmt.Sprintf(`{"user_id":%q}`, restrictedUser.ID), http.StatusCreated, nil)
	restrictedCookie := login(t, router, "runtime-reviewer", "Runtime-temp-pass-1234", true)
	request(t, router, http.MethodGet, profilePath, restrictedCookie, "",
		http.StatusForbidden, nil)

	// A failed latest-revision update must roll back the inserted Revision.
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie, "restore-config",
		runtimeProfileDraftBody(t, 4, runtimeProfileConfigV1, ""), http.StatusOK, nil)
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_second_profile_revision() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.latest_revision_number > 1 THEN
				RAISE EXCEPTION 'injected latest revision update failure';
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER reject_second_profile_revision
		BEFORE UPDATE ON runtime_profiles
		FOR EACH ROW EXECUTE FUNCTION reject_second_profile_revision();
	`); err != nil {
		t.Fatal(err)
	}
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":5}`, http.StatusInternalServerError, nil)
	var revisionCount int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::int FROM runtime_profile_revisions
		WHERE tenant_id=$1 AND profile_id=$2
	`, tenantID, created.Profile.ID).Scan(&revisionCount); err != nil {
		t.Fatal(err)
	}
	if revisionCount != 1 {
		t.Fatalf("failed publication left %d Profile Revisions, want 1", revisionCount)
	}
	if _, err := pool.Exec(ctx, `
		DROP TRIGGER reject_second_profile_revision ON runtime_profiles;
		DROP FUNCTION reject_second_profile_revision();
	`); err != nil {
		t.Fatal(err)
	}
	var secondPublish runtimeProfilePublishResponse
	request(t, router, http.MethodPost, profilePath+"/revisions", ownerCookie,
		`{"expected_revision":5}`, http.StatusCreated, &secondPublish)
	assertRuntimeProfileRevision(t, secondPublish, 2, 5)
	if secondPublish.Revision.SpecDigest != firstPublish.Revision.SpecDigest ||
		secondPublish.Revision.ID == firstPublish.Revision.ID {
		t.Fatal("same canonical content from a new Source Draft did not create a distinct Revision")
	}

	// Concurrent publication of one Source Draft creates exactly one row and ID.
	runtimeProfileRequest(t, router, http.MethodPut, profilePath+"/draft", ownerCookie, "concurrent-publish",
		runtimeProfileDraftBody(t, 5, runtimeProfileConfigV1, ""), http.StatusOK, nil)
	concurrent := publishRuntimeProfileConcurrently(
		t, router, profilePath+"/revisions", ownerCookie, 6, 8,
	)
	createdResponses := 0
	var concurrentID string
	for _, result := range concurrent {
		if result.Status == http.StatusCreated {
			createdResponses++
		} else if result.Status != http.StatusOK {
			t.Fatalf("concurrent publication status/body = %d/%s", result.Status, result.Body)
		}
		if result.ID == "" {
			t.Fatalf("concurrent publication body = %s", result.Body)
		}
		if concurrentID == "" {
			concurrentID = result.ID
		} else if result.ID != concurrentID {
			t.Fatalf("concurrent publication IDs differ: %q != %q", result.ID, concurrentID)
		}
	}
	if createdResponses != 1 {
		t.Fatalf("concurrent created responses = %d, want 1", createdResponses)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*)::int FROM runtime_profile_revisions
		WHERE tenant_id=$1 AND profile_id=$2
	`, tenantID, created.Profile.ID).Scan(&revisionCount); err != nil {
		t.Fatal(err)
	}
	if revisionCount != 3 {
		t.Fatalf("Profile Revision count = %d, want 3", revisionCount)
	}

	testRuntimeProfileCredentials(t, ctx, router, pool, tenantID, created.Profile.ID, profilePath, ownerCookie, memberCookie, firstPublish)

	// A failed initial Draft insert must roll back the Runtime Profile row.
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION reject_runtime_profile_draft() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'injected Runtime Profile Draft failure';
		END
		$$;
		CREATE TRIGGER reject_runtime_profile_draft
		BEFORE INSERT ON runtime_profile_drafts
		FOR EACH ROW EXECUTE FUNCTION reject_runtime_profile_draft();
	`); err != nil {
		t.Fatal(err)
	}
	request(t, router, http.MethodPost, base, ownerCookie,
		`{"name":"Must Roll Back"}`, http.StatusInternalServerError, nil)
	if _, err := pool.Exec(ctx, `
		DROP TRIGGER reject_runtime_profile_draft ON runtime_profile_drafts;
		DROP FUNCTION reject_runtime_profile_draft();
	`); err != nil {
		t.Fatal(err)
	}
	var profileCount, draftCount int
	if err := pool.QueryRow(ctx,
		"SELECT count(*)::int FROM runtime_profiles WHERE tenant_id=$1", tenantID,
	).Scan(&profileCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT count(*)::int FROM runtime_profile_drafts WHERE tenant_id=$1", tenantID,
	).Scan(&draftCount); err != nil {
		t.Fatal(err)
	}
	if profileCount != 1 || draftCount != 1 {
		t.Fatalf("Runtime Profile persistence counts = profile:%d draft:%d", profileCount, draftCount)
	}
}

type runtimeProfilePublishResponse struct {
	Revision runtimeProfileRevisionView `json:"revision"`
}

type runtimeProfileRevisionView struct {
	ID                  string                                                  `json:"id"`
	RevisionNumber      int64                                                   `json:"revision_number"`
	SourceDraftRevision int64                                                   `json:"source_draft_revision"`
	Config              json.RawMessage                                         `json:"config"`
	CredentialStates    map[string]map[string]map[string]runtimeCredentialState `json:"credential_states"`
	SpecDigest          string                                                  `json:"spec_digest"`
}

func assertRuntimeProfileRevision(
	t *testing.T, response runtimeProfilePublishResponse, number, source int64,
) {
	t.Helper()
	if response.Revision.ID == "" || response.Revision.RevisionNumber != number ||
		response.Revision.SourceDraftRevision != source ||
		!strings.HasPrefix(response.Revision.SpecDigest, "sha256:") {
		t.Fatalf("published Profile Revision = %#v", response.Revision)
	}
}

func assertPublishHasNoValidation(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	assertRuntimeProfileNoSecrets(t, recorder.Body.Bytes())
	var body map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, exists := body["validation"]; exists {
		t.Fatalf("successful publish leaked validation report: %s", recorder.Body.String())
	}
}

func runtimeProfileDraftBody(t *testing.T, expected int64, config, credentials string) string {
	t.Helper()
	body := map[string]any{"expected_draft_revision": expected, "credential_protocol_version": "v1", "config": json.RawMessage(config)}
	if credentials != "" {
		body["credentials"] = json.RawMessage(credentials)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

type concurrentPublishResult struct {
	Status int
	ID     string
	Body   string
}

func publishRuntimeProfileConcurrently(
	t *testing.T,
	handler http.Handler,
	path string,
	cookie *http.Cookie,
	revision int64,
	workers int,
) []concurrentPublishResult {
	t.Helper()
	results := make(chan concurrentPublishResult, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			request := httptest.NewRequest(
				http.MethodPost, path,
				strings.NewReader(fmt.Sprintf(`{"expected_revision":%d}`, revision)),
			)
			request.Header.Set("Content-Type", "application/json")
			request.AddCookie(cookie)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			var response runtimeProfilePublishResponse
			_ = json.Unmarshal(recorder.Body.Bytes(), &response)
			results <- concurrentPublishResult{
				Status: recorder.Code,
				ID:     response.Revision.ID,
				Body:   recorder.Body.String(),
			}
		}()
	}
	group.Wait()
	close(results)
	collected := make([]concurrentPublishResult, 0, workers)
	for result := range results {
		collected = append(collected, result)
	}
	return collected
}

const runtimeProfileCredentialsV1 = `{"models":{"primary":{"api_key":{"action":"replace","value":"fixture-model-initial-2397"}}},"tools":{"search":{"bearer_token":{"action":"replace","value":"fixture-mcp-initial-8371"}}},"knowledge":{"docs":{"qdrant_api_key":{"action":"replace","value":"fixture-qdrant-initial-4492"},"embedding_api_key":{"action":"replace","value":"fixture-embedding-initial-3291"}}},"storage":{"session":{"dsn":{"action":"replace","value":"postgres://runtime_user:fixture-storage-password-2239@state.example.test:5432/agent_state?sslmode=require"}}}}`

type runtimeCredentialState struct {
	Configured       bool   `json:"configured"`
	Status           string `json:"status"`
	Revision         int64  `json:"credential_revision"`
	AssociationToken string `json:"association_token"`
}

func runtimeProfileRequest(t *testing.T, handler http.Handler, method, path string, cookie *http.Cookie, key, body string, status int, target any) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, req)
	if out.Code != status {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, out.Code, status, out.Body.String())
	}
	if target != nil {
		if err := json.Unmarshal(out.Body.Bytes(), target); err != nil {
			t.Fatal(err)
		}
	}
	assertRuntimeProfileNoSecrets(t, out.Body.Bytes())
	return out
}

func assertRuntimeProfileNoSecrets(t *testing.T, data []byte) {
	t.Helper()
	for _, forbidden := range []string{"fixture-model-", "fixture-mcp-", "fixture-qdrant-", "fixture-embedding-", "fixture-storage-", "crd_", "ciphertext", "api_key_ref", "dsn_ref"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("public or non-secret storage contains forbidden marker %q", forbidden)
		}
	}
}

func testRuntimeProfileCredentials(t *testing.T, ctx context.Context, router http.Handler, pool *pgxpool.Pool, tenantID, profileID, path string, owner, member *http.Cookie, published runtimeProfilePublishResponse) {
	t.Helper()
	var revision runtimeProfileRevisionView
	out := request(t, router, http.MethodGet, path+"/revisions/1", owner, "", http.StatusOK, &revision)
	assertRuntimeProfileNoSecrets(t, out.Body.Bytes())
	initial := revision.CredentialStates["models"]["primary"]["api_key"]
	if !initial.Configured || initial.Status != "active" || initial.Revision != 1 || len(initial.AssociationToken) != 64 {
		t.Fatalf("credential state=%#v", initial)
	}
	var originalID string
	if err := pool.QueryRow(ctx, `SELECT spec_jsonb #>> '{models,primary,api_key_credential_id}' FROM runtime_profile_revisions WHERE tenant_id=$1 AND profile_id=$2 AND revision_number=1`, tenantID, profileID).Scan(&originalID); err != nil {
		t.Fatal(err)
	}
	countCredentials := func() int {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM runtime_profile_credentials WHERE tenant_id=$1 AND profile_id=$2`, tenantID, profileID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if countCredentials() != 5 {
		t.Fatalf("initial encrypted credential count=%d", countCredentials())
	}
	// URL userinfo/query/fragment are not alternate credential input fields.
	for index, endpoint := range []string{
		"https://user:fixture-model-url-canary@api.example.test/v1",
		"https://api.example.test/v1?api_key=fixture-model-url-canary",
		"https://api.example.test/v1#fixture-model-url-canary",
	} {
		config := strings.Replace(runtimeProfileConfigV1, "https://api.openai.com/v1", endpoint, 1)
		runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, fmt.Sprintf("reject-url-%d", index),
			runtimeProfileDraftBody(t, 6, config, ""), http.StatusBadRequest, nil)
	}
	var afterUnsafeEndpoint struct {
		DraftRevision int64 `json:"draft_revision"`
	}
	request(t, router, http.MethodGet, path+"/draft", owner, "", http.StatusOK, &afterUnsafeEndpoint)
	if afterUnsafeEndpoint.DraftRevision != 6 || countCredentials() != 5 {
		t.Fatal("rejected credential-bearing endpoint changed persistent state")
	}

	cowActions := `{"models":{"primary":{"api_key":{"action":"replace","value":"fixture-model-cow-6271"}}}}`
	cowBody := runtimeProfileDraftBody(t, 6, runtimeProfileConfigV1, cowActions)
	runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", member, "member-cow", cowBody, http.StatusForbidden, nil)
	runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, "", cowBody, http.StatusBadRequest, nil)
	runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, "reject-old-wire", `{"expected_revision":6,"spec":{"api_key_ref":"old-ref"}}`, http.StatusBadRequest, nil)
	var saved struct {
		DraftRevision int64 `json:"draft_revision"`
	}
	cowResponse := runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, "cow-save", cowBody, http.StatusOK, &saved)
	if saved.DraftRevision != 7 {
		t.Fatalf("COW draft revision=%d", saved.DraftRevision)
	}
	cowRetry := runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, "cow-save", cowBody, http.StatusOK, nil)
	if cowResponse.Body.String() != cowRetry.Body.String() || countCredentials() != 6 {
		t.Fatal("COW retry created duplicate value or changed receipt")
	}
	runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, "cow-save", strings.Replace(cowBody, "6271", "another", 1), http.StatusConflict, nil)
	var cowID string
	if err := pool.QueryRow(ctx, `SELECT spec_jsonb #>> '{models,primary,api_key_credential_id}' FROM runtime_profile_drafts WHERE tenant_id=$1 AND profile_id=$2`, tenantID, profileID).Scan(&cowID); err != nil {
		t.Fatal(err)
	}
	if cowID == originalID || !strings.HasPrefix(cowID, "crd_") {
		t.Fatal("COW reused published credential identity")
	}
	runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, "keep-save", runtimeProfileDraftBody(t, 7, runtimeProfileConfigV1, ""), http.StatusOK, nil)
	var keptID string
	if err := pool.QueryRow(ctx, `SELECT spec_jsonb #>> '{models,primary,api_key_credential_id}' FROM runtime_profile_drafts WHERE tenant_id=$1 AND profile_id=$2`, tenantID, profileID).Scan(&keptID); err != nil {
		t.Fatal(err)
	}
	if keptID != cowID || countCredentials() != 6 {
		t.Fatal("keep mutated credential identity/value set")
	}

	updatePath := path + "/credentials/update"
	rotateBody := runtimeProfileUpdateBody(t, initial.AssociationToken, "replace", 1, "fixture-model-live-3288")
	runtimeProfileRequest(t, router, http.MethodPost, updatePath, member, "member-live", rotateBody, http.StatusForbidden, nil)
	var rotated runtimeCredentialState
	rotateResponse := runtimeProfileRequest(t, router, http.MethodPost, updatePath, owner, "live-rotate", rotateBody, http.StatusOK, &rotated)
	if rotated.Revision != 2 || rotated.Status != "active" {
		t.Fatalf("rotation result=%#v", rotated)
	}
	runtimeProfileRequest(t, router, http.MethodPost, updatePath, owner, "stale-live", rotateBody, http.StatusConflict, nil)
	runtimeProfileRequest(t, router, http.MethodPost, updatePath, owner, "bad-association", runtimeProfileUpdateBody(t, strings.Repeat("0", 64), "clear", 2, ""), http.StatusConflict, nil)
	var afterRotate runtimeProfileRevisionView
	request(t, router, http.MethodGet, path+"/revisions/1", owner, "", http.StatusOK, &afterRotate)
	if afterRotate.SpecDigest != published.Revision.SpecDigest || afterRotate.CredentialStates["models"]["primary"]["api_key"].Revision != 2 {
		t.Fatal("live rotation changed published snapshot or did not update state")
	}
	immutableRetry := request(t, router, http.MethodPost, path+"/revisions", owner, `{"expected_revision":3}`, http.StatusOK, nil)
	assertPublishHasNoValidation(t, immutableRetry)
	if strings.Contains(immutableRetry.Body.String(), "credential_states") {
		t.Fatal("publish retry contains mutable credential state")
	}

	clearBody := runtimeProfileUpdateBody(t, initial.AssociationToken, "clear", 2, "")
	var cleared runtimeCredentialState
	runtimeProfileRequest(t, router, http.MethodPost, updatePath, owner, "live-clear", clearBody, http.StatusOK, &cleared)
	if cleared.Revision != 3 || cleared.Status != "cleared" {
		t.Fatalf("clear result=%#v", cleared)
	}
	runtimeProfileRequest(t, router, http.MethodPost, updatePath, owner, "no-resurrection", runtimeProfileUpdateBody(t, initial.AssociationToken, "replace", 3, "fixture-model-resurrection"), http.StatusConflict, nil)
	oldReceipt := runtimeProfileRequest(t, router, http.MethodPost, updatePath, owner, "live-rotate", rotateBody, http.StatusOK, nil)
	if oldReceipt.Body.String() != rotateResponse.Body.String() {
		t.Fatal("live request receipt changed after clear")
	}
	var clearedView runtimeProfileRevisionView
	request(t, router, http.MethodGet, path+"/revisions/1", owner, "", http.StatusOK, &clearedView)
	oldState := clearedView.CredentialStates["models"]["primary"]["api_key"]
	if oldState.Configured || oldState.Status != "cleared" || clearedView.SpecDigest != published.Revision.SpecDigest {
		t.Fatal("clear changed immutable snapshot or reports configured")
	}
	var draftView struct {
		DraftRevision    int64                                                   `json:"draft_revision"`
		CredentialStates map[string]map[string]map[string]runtimeCredentialState `json:"credential_states"`
	}
	draftResponse := request(t, router, http.MethodGet, path+"/draft", owner, "", http.StatusOK, &draftView)
	assertRuntimeProfileNoSecrets(t, draftResponse.Body.Bytes())
	if draftView.DraftRevision != 8 || !draftView.CredentialStates["models"]["primary"]["api_key"].Configured || draftView.CredentialStates["models"]["primary"]["api_key"].Revision != 1 {
		t.Fatal("live mutation of old publication changed new COW Draft")
	}

	// Force the final receipt write to fail: credentials and Draft must both roll back.
	if _, err := pool.Exec(ctx, `CREATE FUNCTION reject_profile_credential_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected receipt failure'; END $$; CREATE TRIGGER reject_profile_credential_receipt BEFORE INSERT ON runtime_profile_credential_receipts FOR EACH ROW EXECUTE FUNCTION reject_profile_credential_receipt();`); err != nil {
		t.Fatal(err)
	}
	runtimeProfileRequest(t, router, http.MethodPut, path+"/draft", owner, "atomic-failure", runtimeProfileDraftBody(t, 8, runtimeProfileConfigV1, cowActions), http.StatusInternalServerError, nil)
	if _, err := pool.Exec(ctx, `DROP TRIGGER reject_profile_credential_receipt ON runtime_profile_credential_receipts; DROP FUNCTION reject_profile_credential_receipt();`); err != nil {
		t.Fatal(err)
	}
	var afterFailure struct {
		DraftRevision int64 `json:"draft_revision"`
	}
	request(t, router, http.MethodGet, path+"/draft", owner, "", http.StatusOK, &afterFailure)
	if afterFailure.DraftRevision != 8 || countCredentials() != 6 {
		t.Fatal("failed receipt write left a partial credential save")
	}

	var storedSpec, receipts string
	if err := pool.QueryRow(ctx, `SELECT string_agg(spec_jsonb::text, E'\n') FROM (SELECT spec_jsonb FROM runtime_profile_drafts WHERE tenant_id=$1 AND profile_id=$2 UNION ALL SELECT spec_jsonb FROM runtime_profile_revisions WHERE tenant_id=$1 AND profile_id=$2) AS snapshots`, tenantID, profileID).Scan(&storedSpec); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT coalesce(string_agg(result_jsonb::text, E'\n'),'') FROM runtime_profile_credential_receipts WHERE tenant_id=$1 AND profile_id=$2`, tenantID, profileID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	assertRuntimeProfileNoValues(t, []byte(storedSpec))
	assertRuntimeProfileNoSecrets(t, []byte(receipts))
	if strings.Contains(storedSpec, "ciphertext") || strings.Contains(storedSpec, "api_key_ref") {
		t.Fatal("stored Canonical Spec has an excluded field")
	}
	rows, err := pool.Query(ctx, `SELECT status,ciphertext FROM runtime_profile_credentials WHERE tenant_id=$1 AND profile_id=$2`, tenantID, profileID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	activeCount := 0
	for rows.Next() {
		var status string
		var encrypted []byte
		if err := rows.Scan(&status, &encrypted); err != nil {
			t.Fatal(err)
		}
		if status == "cleared" {
			if len(encrypted) != 0 {
				t.Fatal("cleared credential retained ciphertext")
			}
			continue
		}
		activeCount++
		if len(encrypted) < 32 {
			t.Fatal("encrypted value missing")
		}
		assertRuntimeProfileNoValues(t, encrypted)
		for _, representation := range []string{base64.StdEncoding.EncodeToString(encrypted), hex.EncodeToString(encrypted)} {
			if strings.Contains(storedSpec, representation) || strings.Contains(receipts, representation) {
				t.Fatal("ciphertext leaked into snapshot or receipt")
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if activeCount != 5 {
		t.Fatalf("active encrypted credentials=%d", activeCount)
	}
}

func runtimeProfileUpdateBody(t *testing.T, token, action string, revision int64, value string) string {
	t.Helper()
	body := map[string]any{"target": map[string]any{"profile_revision_number": 1, "category": "models", "resource_name": "primary", "purpose_field": "api_key", "association_token": token}, "action": action, "expected_credential_revision": revision}
	if action == "replace" {
		body["value"] = value
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func assertRuntimeProfileNoValues(t *testing.T, data []byte) {
	t.Helper()
	for _, marker := range []string{"fixture-model-", "fixture-mcp-", "fixture-qdrant-", "fixture-embedding-", "fixture-storage-"} {
		if strings.Contains(string(data), marker) {
			t.Fatalf("secret marker escaped write boundary: %s", marker)
		}
	}
}
