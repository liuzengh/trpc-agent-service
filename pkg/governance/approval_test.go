package governance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func approvalRequest() ApprovalRequest {
	return ApprovalRequest{
		TenantID:       "tenant-a",
		UserID:         "alice",
		SessionOwnerID: "group_owner",
		SessionID:      "session-a",
		ToolName:       "delete_file",
		ArgsHash:       "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		InvocationID:   "inbox:42",
	}
}

func TestCanonicalArgsHashIgnoresObjectOrderButPreservesArrays(t *testing.T) {
	left, err := CanonicalArgsHash([]byte(`{"a":1,"b":[1,2]}`))
	if err != nil {
		t.Fatal(err)
	}
	right, err := CanonicalArgsHash([]byte(` {"b":[1,2],"a":1} `))
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("object order changed hash: %q != %q", left, right)
	}
	changed, err := CanonicalArgsHash([]byte(`{"a":1,"b":[2,1]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed == left {
		t.Fatal("array order was ignored")
	}
}

func TestCanonicalArgsHashRejectsDuplicateObjectKeys(t *testing.T) {
	for _, raw := range []string{
		`{"a":1,"a":2}`,
		`{"outer":{"a":1,"a":2}}`,
		`[{"a":1,"a":2}]`,
	} {
		if _, err := CanonicalArgsHash([]byte(raw)); err == nil {
			t.Fatalf("duplicate JSON key accepted: %s", raw)
		}
	}
}

func TestCanonicalArgsHashRejectsInvalidUTF8InsteadOfReplacingIt(t *testing.T) {
	invalid := []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}
	if _, err := CanonicalArgsHash(invalid); err == nil {
		t.Fatal("invalid UTF-8 arguments were accepted")
	}
	valid, err := CanonicalArgsHash([]byte(`{"x":"\ufffd"}`))
	if err != nil {
		t.Fatal(err)
	}
	if valid == "" {
		t.Fatal("valid replacement-character arguments returned an empty hash")
	}
}

func TestCanonicalArgsHashBoundsInputSize(t *testing.T) {
	raw := append([]byte(`{"value":"`), []byte(strings.Repeat("x", maxApprovalArgsBytes))...)
	raw = append(raw, []byte(`"}`)...)
	if _, err := CanonicalArgsHash(raw); err == nil {
		t.Fatal("oversized tool arguments were accepted")
	}
}

func TestCanonicalArgsHashBoundsJSONDepth(t *testing.T) {
	tooDeep := strings.Repeat("[", maxApprovalJSONDepth+1) + strings.Repeat("]", maxApprovalJSONDepth+1)
	if _, err := CanonicalArgsHash([]byte(tooDeep)); err == nil ||
		!strings.Contains(err.Error(), "maximum JSON depth") {
		t.Fatalf("deep tool arguments were accepted: %v", err)
	}

	atLimit := strings.Repeat("[", maxApprovalJSONDepth) + "null" + strings.Repeat("]", maxApprovalJSONDepth)
	if _, err := CanonicalArgsHash([]byte(atLimit)); err != nil {
		t.Fatalf("arguments at the configured depth were rejected: %v", err)
	}
}

func TestMemoryApprovalStoreGrantAndConsumeIsOneTimeAndBound(t *testing.T) {
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	challenge, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := store.Grant(context.Background(), challenge.ChallengeID, "operator-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Consume(context.Background(), request, grant.Token); err != nil {
		t.Fatal(err)
	}
	if err := store.Consume(context.Background(), request, grant.Token); !errors.Is(err, ErrApprovalNotFound) && !errors.Is(err, ErrApprovalInvalid) {
		t.Fatalf("second consume error=%v, want one-time rejection", err)
	}

	other := request
	other.ToolName = "different_tool"
	challenge, err = store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	grant, err = store.Grant(context.Background(), challenge.ChallengeID, "operator-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Consume(context.Background(), other, grant.Token); err == nil {
		t.Fatal("approval token was accepted for a different tool")
	}
}

func TestPluginCreatesChallengeThenConsumesExactToken(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{ID: "tenant-a", ToolPolicy: tenant.ToolPolicy{
		Mode: "whitelist", Allowed: []string{"delete_file"}, RequireConfirmation: []string{"delete_file"},
	}})
	store := NewMemoryApprovalStore()
	plugin := NewPluginWithApprovalStore(filter, "approval", store)
	state := NewApprovalState()
	ctx := ContextWithApprovalState(context.Background(), state)
	ctx = ContextWithInvocationAudit(ctx, InvocationAuditContext{
		UserID: "alice", SessionOwnerID: "group_owner", SessionID: "session-a", InvocationID: "inbox:42",
	})
	args := &tool.BeforeToolArgs{ToolName: "delete_file", Arguments: []byte(`{"path":"/tmp/a"}`)}
	if _, err := plugin.beforeTool(ctx, args); err == nil {
		t.Fatal("dangerous tool ran without approval")
	} else {
		var required *ApprovalRequiredError
		if !errors.As(err, &required) {
			t.Fatalf("first error=%v, want ApprovalRequiredError", err)
		}
	}
	challenge, ok := state.Challenge()
	if !ok {
		t.Fatal("approval challenge was not exposed to caller")
	}
	grant, err := store.Grant(context.Background(), challenge.ChallengeID, "operator-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx = ContextWithInvocationAudit(ctx, InvocationAuditContext{
		UserID: "alice", SessionOwnerID: "group_owner", SessionID: "session-a", InvocationID: "inbox:42", ApprovalToken: grant.Token,
	})
	if _, err := plugin.beforeTool(ctx, args); err != nil {
		t.Fatalf("approved exact retry rejected: %v", err)
	}
	if _, err := plugin.beforeTool(ctx, args); err == nil {
		t.Fatal("one-time approval was reusable")
	}
}

func TestMemoryApprovalStoreConsumesGrantedRetryWithoutRawToken(t *testing.T) {
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	challenge, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(context.Background(), challenge.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeGranted(context.Background(), request); err != nil {
		t.Fatalf("ConsumeGranted: %v", err)
	}
	if err := store.ConsumeGranted(context.Background(), request); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("second ConsumeGranted error=%v, want ErrApprovalNotFound", err)
	}
}

func TestMemoryApprovalStoreExactChallengeConsumptionRejectsReplacement(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	first, err := store.CreateChallenge(ctx, request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(ctx, first.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeGrantedForChallenge(ctx, request, first.ChallengeID); err != nil {
		t.Fatalf("consume first challenge: %v", err)
	}
	second, err := store.CreateChallenge(ctx, request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.ChallengeID == first.ChallengeID {
		t.Fatal("replacement challenge reused the consumed challenge id")
	}
	if _, err := store.Grant(ctx, second.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeGrantedForChallenge(ctx, request, first.ChallengeID); !errors.Is(err, ErrApprovalNotFound) {
		t.Fatalf("stale challenge consume error=%v, want ErrApprovalNotFound", err)
	}
	if err := store.ConsumeGrantedForChallenge(ctx, request, second.ChallengeID); err != nil {
		t.Fatalf("consume replacement challenge: %v", err)
	}
}

func TestPluginResumeRejectsConsumedChallengeWithoutCreatingReplacement(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{ID: "tenant-a", ToolPolicy: tenant.ToolPolicy{
		Mode: "whitelist", Allowed: []string{"delete_file"}, RequireConfirmation: []string{"delete_file"},
	}})
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	first, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(context.Background(), first.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.ConsumeGrantedForChallenge(context.Background(), request, first.ChallengeID); err != nil {
		t.Fatal(err)
	}
	second, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(context.Background(), second.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}
	state := NewApprovalState()
	state.SetResumeChallengeID(first.ChallengeID)
	ctx := ContextWithApprovalState(context.Background(), state)
	ctx = ContextWithInvocationAudit(ctx, InvocationAuditContext{
		UserID:         request.UserID,
		SessionOwnerID: request.SessionOwnerID,
		SessionID:      request.SessionID,
		InvocationID:   request.InvocationID,
	})
	args := &tool.BeforeToolArgs{ToolName: request.ToolName, Arguments: []byte(`{"path":"/tmp/a"}`)}
	_, err = NewPluginWithApprovalStore(filter, "approval", store).beforeTool(ctx, args)
	if !errors.Is(err, ErrApprovalResumeInvalid) {
		t.Fatalf("stale resume error=%v, want ErrApprovalResumeInvalid", err)
	}
	if err := store.ConsumeGrantedForChallenge(context.Background(), request, second.ChallengeID); err != nil {
		t.Fatalf("replacement grant was consumed by stale resume: %v", err)
	}
}

func TestMemoryApprovalStoreConcurrentConsumeGrantedIsOneTime(t *testing.T) {
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	challenge, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(context.Background(), challenge.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}
	const workers = 16
	results := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer group.Done()
			results <- store.ConsumeGranted(context.Background(), request)
		}()
	}
	group.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		if !errors.Is(err, ErrApprovalNotFound) && !errors.Is(err, ErrApprovalNotGranted) && !errors.Is(err, ErrApprovalInvalid) {
			t.Fatalf("unexpected concurrent consume error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent consume successes=%d, want exactly one", successes)
	}
}

func TestMemoryApprovalStoreDoesNotCreateDuplicateChallengeForRetry(t *testing.T) {
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	first, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if first.ChallengeID != second.ChallengeID {
		t.Fatalf("retry created duplicate challenge: %q != %q", first.ChallengeID, second.ChallengeID)
	}
}

func TestMemoryApprovalStoreFindActiveApprovalIsExactAndFailsClosedOnAmbiguity(t *testing.T) {
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	challenge, err := store.CreateChallenge(context.Background(), request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	scope := ApprovalInvocationScope{
		TenantID: request.TenantID, UserID: request.UserID,
		SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
		InvocationID: request.InvocationID,
	}
	found, err := store.FindActiveApproval(context.Background(), scope)
	if err != nil || found.ChallengeID != challenge.ChallengeID {
		t.Fatalf("FindActiveApproval=%#v err=%v, want %q", found, err, challenge.ChallengeID)
	}
	other := request
	other.ToolName = "delete_directory"
	if _, err := store.CreateChallenge(context.Background(), other, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FindActiveApproval(context.Background(), scope); !errors.Is(err, ErrApprovalAmbiguous) {
		t.Fatalf("ambiguous approval error=%v, want ErrApprovalAmbiguous", err)
	}
}

func TestMemoryApprovalStoreInspectApprovalResumeCapturesGrantAtomically(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryApprovalStore()
	request := approvalRequest()
	challenge, err := store.CreateChallenge(ctx, request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.InspectApprovalResume(ctx, ApprovalInvocationScope{
		TenantID: request.TenantID, UserID: request.UserID,
		SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
		InvocationID: request.InvocationID,
	})
	if err != nil {
		t.Fatalf("pending inspection: %v", err)
	}
	if state.Granted || state.Challenge.ChallengeID != challenge.ChallengeID {
		t.Fatalf("pending state=%#v, want ungranted challenge", state)
	}
	if _, err := store.Grant(ctx, challenge.ChallengeID, "operator"); err != nil {
		t.Fatal(err)
	}
	state, err = store.InspectApprovalResume(ctx, ApprovalInvocationScope{
		TenantID: request.TenantID, UserID: request.UserID,
		SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
		InvocationID: request.InvocationID,
	})
	if err != nil {
		t.Fatalf("granted inspection: %v", err)
	}
	if !state.Granted || state.Challenge.ChallengeID != challenge.ChallengeID {
		t.Fatalf("granted state=%#v, want granted challenge", state)
	}
}

func TestValidateApprovalChallengeRejectsMalformedMetadata(t *testing.T) {
	request := approvalRequest()
	tests := []ApprovalChallenge{
		{ChallengeID: "bad id", Request: request, ExpiresAt: time.Now().Add(time.Minute)},
		{ChallengeID: "challenge", Request: request},
		{ChallengeID: "challenge", ExpiresAt: time.Now().Add(time.Minute),
			Request: ApprovalRequest{TenantID: request.TenantID, UserID: request.UserID,
				SessionOwnerID: request.SessionOwnerID, SessionID: request.SessionID,
				ToolName: request.ToolName, ArgsHash: "sha256:not-hex", InvocationID: request.InvocationID}},
	}
	for i, challenge := range tests {
		if err := ValidateApprovalChallenge(challenge); !errors.Is(err, ErrApprovalInvalid) {
			t.Errorf("case %d error=%v, want ErrApprovalInvalid", i, err)
		}
	}
}

func TestMemoryApprovalStoreListsOnlyPendingTenantChallenges(t *testing.T) {
	store := NewMemoryApprovalStore()
	first := approvalRequest()
	if _, err := store.CreateChallenge(context.Background(), first, time.Minute); err != nil {
		t.Fatal(err)
	}
	other := first
	other.TenantID = "tenant-b"
	if _, err := store.CreateChallenge(context.Background(), other, time.Minute); err != nil {
		t.Fatal(err)
	}
	items, err := store.ListChallenges(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Request.TenantID != "tenant-a" {
		t.Fatalf("tenant challenge list=%#v", items)
	}
}
