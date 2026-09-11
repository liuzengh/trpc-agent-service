package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestConsoleReadsAndRollsBackImmutableApplicationVersion(t *testing.T) {
	handler := testConsoleHandler(t)
	update := `{"status":"disabled","instruction":"changed","model":{"provider_id":"primary","name":"support-v2"},"channels":[{"type":"wecom","binding_id":"wecom-support"}]}`
	if response := handler.request(t, http.MethodPut, "/api/v1/apps/example/support", update); response.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200: %s", response.Code, response.Body.String())
	}

	versionOne := handler.request(t, http.MethodGet, "/api/v1/apps/example/support/versions/1", "")
	if versionOne.Code != http.StatusOK {
		t.Fatalf("get version status = %d, want 200: %s", versionOne.Code, versionOne.Body.String())
	}
	var historical struct {
		Application tenant.Snapshot `json:"application"`
	}
	if err := json.Unmarshal(versionOne.Body.Bytes(), &historical); err != nil {
		t.Fatalf("decode historical version: %v", err)
	}
	if historical.Application.Config.ConfigVersion != 1 || historical.Application.Config.Status != config.AgentActive {
		t.Fatalf("unexpected historical snapshot: %+v", historical.Application)
	}

	rollback := handler.request(t, http.MethodPost, "/api/v1/apps/example/support/rollback/1", "")
	if rollback.Code != http.StatusOK {
		t.Fatalf("rollback status = %d, want 200: %s", rollback.Code, rollback.Body.String())
	}
	var restored struct {
		Application tenant.Snapshot `json:"application"`
	}
	if err := json.Unmarshal(rollback.Body.Bytes(), &restored); err != nil {
		t.Fatalf("decode rollback response: %v", err)
	}
	if restored.Application.Config.ConfigVersion != 3 || restored.Application.Config.Status != config.AgentActive {
		t.Fatalf("rollback must publish a fresh active version: %+v", restored.Application)
	}

	member := identity.SessionUser{PlatformUserID: "member", Tenants: []identity.TenantRole{{TenantID: "example", Role: identity.RoleMember}}}
	if response := handler.requestAs(t, member, http.MethodGet, "/api/v1/apps/example/support/versions/2", ""); response.Code != http.StatusOK {
		t.Fatalf("member version read status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if response := handler.request(t, http.MethodGet, "/api/v1/apps/example/support/versions/not-a-number", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("invalid version status = %d, want 400: %s", response.Code, response.Body.String())
	}
}

func TestConsoleCandidateRolloutHasOneExplicitCandidateLifecycle(t *testing.T) {
	var testUserID string
	handler := testConsoleHandler(t, func(dependencies *ConsoleDependencies) {
		store := dependencies.Identities.(*identity.MemoryIdentityStore)
		user := resolveTestWeComUser(t, store, "corp-example", "rollout-tester")
		testUserID = user.PlatformUserID
		if err := store.GrantMembership(context.Background(), "example", testUserID, identity.RoleMember); err != nil {
			t.Fatal(err)
		}
	})
	stage := func(instruction string) *httptest.ResponseRecorder {
		body := `{"status":"active","instruction":"` + instruction + `","model":{"provider_id":"primary","name":"support"},` +
			`"storage":{"session":{"profile_id":"platform-postgres"},"memory":{"profile_id":"platform-postgres"},"knowledge":{"profile_id":"platform-pgvector"},"artifact":{"profile_id":"platform-postgres"}},` +
			`"channels":[{"type":"telegram","binding_id":"example-support-bot"}]}`
		return handler.request(t, http.MethodPost, "/api/v1/apps/example/support/versions", body)
	}
	if response := stage("candidate-two"); response.Code != http.StatusCreated {
		t.Fatalf("stage v2 = %d: %s", response.Code, response.Body.String())
	}
	if response := stage("candidate-three"); response.Code != http.StatusCreated {
		t.Fatalf("replace candidate with v3 = %d: %s", response.Code, response.Body.String())
	}
	versions := handler.request(t, http.MethodGet, "/api/v1/apps/example/support/versions", "")
	if versions.Code != http.StatusOK || !strings.Contains(versions.Body.String(), `"config_version":1`) || !strings.Contains(versions.Body.String(), `"config_version":3`) {
		t.Fatalf("list versions = %d: %s", versions.Code, versions.Body.String())
	}

	candidate := handler.request(t, http.MethodGet, "/api/v1/apps/example/support/candidate", "")
	if candidate.Code != http.StatusOK {
		t.Fatalf("get candidate = %d: %s", candidate.Code, candidate.Body.String())
	}
	var candidateBody struct {
		Candidate *tenant.Snapshot `json:"candidate"`
	}
	if err := json.Unmarshal(candidate.Body.Bytes(), &candidateBody); err != nil {
		t.Fatalf("decode candidate: %v", err)
	}
	if candidateBody.Candidate == nil || candidateBody.Candidate.Config.ConfigVersion != 3 {
		t.Fatalf("candidate = %#v, want v3 only", candidateBody.Candidate)
	}

	rollout := handler.request(t, http.MethodPut, "/api/v1/apps/example/support/rollout",
		fmt.Sprintf(`{"expected_generation":0,"basis_points":1000,"test_user_ids":[%q],"ingresses":["web"]}`, testUserID))
	if rollout.Code != http.StatusOK {
		t.Fatalf("start rollout = %d: %s", rollout.Code, rollout.Body.String())
	}
	var rolloutBody struct {
		Rollout tenant.RolloutPolicy `json:"rollout"`
	}
	if err := json.Unmarshal(rollout.Body.Bytes(), &rolloutBody); err != nil {
		t.Fatalf("decode rollout: %v", err)
	}
	if rolloutBody.Rollout.CandidateVersion != 3 || rolloutBody.Rollout.StableVersion != 1 || rolloutBody.Rollout.BasisPoints != 1000 {
		t.Fatalf("rollout = %+v, want stable v1 -> candidate v3 at 10%%", rolloutBody.Rollout)
	}
	currentRollout := handler.request(t, http.MethodGet, "/api/v1/apps/example/support/rollout", "")
	if currentRollout.Code != http.StatusOK || !strings.Contains(currentRollout.Body.String(), `"candidate_version":3`) || !strings.Contains(currentRollout.Body.String(), `"basis_points":1000`) {
		t.Fatalf("get rollout = %d: %s", currentRollout.Code, currentRollout.Body.String())
	}

	if response := handler.request(t, http.MethodPut, "/api/v1/apps/example/support", `{"status":"active","instruction":"must-not-publish","model":{"provider_id":"primary","name":"support"},"storage":{"session":{"profile_id":"platform-postgres"},"memory":{"profile_id":"platform-postgres"},"knowledge":{"profile_id":"platform-pgvector"},"artifact":{"profile_id":"platform-postgres"}},"channels":[{"type":"telegram","binding_id":"example-support-bot"}]}`); response.Code != http.StatusConflict {
		t.Fatalf("direct publish during rollout = %d, want 409: %s", response.Code, response.Body.String())
	}
	if response := handler.request(t, http.MethodDelete, "/api/v1/apps/example/support/candidate", `{"expected_version":3}`); response.Code != http.StatusConflict {
		t.Fatalf("discard during rollout = %d, want 409: %s", response.Code, response.Body.String())
	}
	if response := handler.request(t, http.MethodDelete, "/api/v1/apps/example/support/rollout", fmt.Sprintf(`{"expected_generation":%d}`, rolloutBody.Rollout.Generation)); response.Code != http.StatusNoContent {
		t.Fatalf("stop rollout = %d: %s", response.Code, response.Body.String())
	}
	candidate = handler.request(t, http.MethodGet, "/api/v1/apps/example/support/candidate", "")
	if candidate.Code != http.StatusOK || !strings.Contains(candidate.Body.String(), `"config_version":3`) {
		t.Fatalf("candidate after stop = %d: %s", candidate.Code, candidate.Body.String())
	}
	if response := handler.request(t, http.MethodPost, "/api/v1/apps/example/support/candidate/promote", `{"expected_version":3,"expected_generation":0}`); response.Code != http.StatusOK {
		t.Fatalf("promote stopped candidate = %d: %s", response.Code, response.Body.String())
	}
	candidate = handler.request(t, http.MethodGet, "/api/v1/apps/example/support/candidate", "")
	if candidate.Code != http.StatusOK || !strings.Contains(candidate.Body.String(), `"candidate":null`) {
		t.Fatalf("candidate after promotion = %d: %s", candidate.Code, candidate.Body.String())
	}

	if response := stage("candidate-four"); response.Code != http.StatusCreated {
		t.Fatalf("stage v4 = %d: %s", response.Code, response.Body.String())
	}
	if response := handler.request(t, http.MethodDelete, "/api/v1/apps/example/support/candidate", `{"expected_version":4}`); response.Code != http.StatusNoContent {
		t.Fatalf("discard v4 candidate = %d: %s", response.Code, response.Body.String())
	}
	candidate = handler.request(t, http.MethodGet, "/api/v1/apps/example/support/candidate", "")
	if candidate.Code != http.StatusOK || !strings.Contains(candidate.Body.String(), `"candidate":null`) {
		t.Fatalf("candidate after discard = %d: %s", candidate.Code, candidate.Body.String())
	}
}
