package governance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestApprovalPolicyBindsEveryAuthorizationScope(t *testing.T) {
	rc := RequestContext{
		TenantID: "tenant", ConfigVersion: "v1", AppNamespace: "app", UserID: "user",
		SenderID: "sender", SessionID: "session", Channel: "wecom", BindingID: "binding",
	}
	changes := map[string]func(*RequestContext){
		"tenant":  func(r *RequestContext) { r.TenantID = "other" },
		"version": func(r *RequestContext) { r.ConfigVersion = "v2" },
		"app":     func(r *RequestContext) { r.AppNamespace = "other" },
		"user":    func(r *RequestContext) { r.UserID = "other" },
		"sender":  func(r *RequestContext) { r.SenderID = "other" },
		"session": func(r *RequestContext) { r.SessionID = "other" },
		"channel": func(r *RequestContext) { r.Channel = "telegram" },
		"binding": func(r *RequestContext) { r.BindingID = "other" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			store := NewApprovalStore()
			policy := PermissionPolicy(config.ToolPolicy{Allow: []string{"danger"}, RequireConfirm: []string{"danger"}}, store)
			req := &tool.PermissionRequest{ToolName: "danger", Arguments: []byte(`{"target":"a","amount":1}`)}
			decision, err := policy(WithRequestContext(context.Background(), rc), req)
			if err != nil || decision.Action != tool.PermissionActionAsk {
				t.Fatalf("issue approval = %+v, %v", decision, err)
			}
			_, nonce := ExtractApproval(decision.Reason)
			if nonce == "" {
				t.Fatalf("missing nonce: %+v", decision)
			}
			changed := rc
			changed.ApprovalNonce = nonce
			change(&changed)
			decision, err = policy(WithRequestContext(context.Background(), changed), req)
			if err != nil || decision.Action != tool.PermissionActionAsk {
				t.Fatalf("changed scope must not grant approval: %+v, %v", decision, err)
			}
			original := rc
			original.ApprovalNonce = nonce
			// Object field order has no semantic impact on argument identity.
			req.Arguments = []byte(`{ "amount": 1, "target": "a" }`)
			decision, err = policy(WithRequestContext(context.Background(), original), req)
			if err != nil || decision.Action != tool.PermissionActionAllow {
				t.Fatalf("scope mismatch consumed original nonce: %+v, %v", decision, err)
			}
		})
	}
}

func TestApprovalScopeCannotCollideAtFieldSeparator(t *testing.T) {
	a := RequestContext{TenantID: "tenant\x1fv1", ConfigVersion: "v2"}
	b := RequestContext{TenantID: "tenant", ConfigVersion: "v1\x1fv2"}
	if approvalScope(a, "danger", []byte(`{}`)) == approvalScope(b, "danger", []byte(`{}`)) {
		t.Fatal("scope field boundaries are ambiguous")
	}
}

func TestApprovalArgumentNormalizationPreservesLargeIntegers(t *testing.T) {
	const reordered = `{ "items": [9007199254740995], "nested": { "amount": 9007199254740993 } }`
	const original = `{"nested":{"amount":9007199254740993},"items":[9007199254740995]}`
	const rounded = `{"nested":{"amount":9007199254740992},"items":[9007199254740995]}`

	if got, want := ArgumentsHash([]byte(reordered)), ArgumentsHash([]byte(original)); got != want {
		t.Fatalf("equivalent object arguments have different hashes: %s != %s", got, want)
	}
	if ArgumentsHash([]byte(original)) == ArgumentsHash([]byte(rounded)) {
		t.Fatal("large integer arguments collided after normalization")
	}

	store := NewApprovalStore()
	policy := PermissionPolicy(config.ToolPolicy{Allow: []string{"danger"}, RequireConfirm: []string{"danger"}}, store)
	rc := RequestContext{TenantID: "tenant", ConfigVersion: "v1", AppNamespace: "app", UserID: "user", SessionID: "session"}
	req := &tool.PermissionRequest{ToolName: "danger", Arguments: []byte(original)}
	decision, err := policy(WithRequestContext(context.Background(), rc), req)
	if err != nil || decision.Action != tool.PermissionActionAsk {
		t.Fatalf("issue approval = %+v, %v", decision, err)
	}
	_, nonce := ExtractApproval(decision.Reason)
	if nonce == "" {
		t.Fatal("missing approval nonce")
	}

	changed := rc
	changed.ApprovalNonce = nonce
	req.Arguments = []byte(rounded)
	decision, err = policy(WithRequestContext(context.Background(), changed), req)
	if err != nil || decision.Action != tool.PermissionActionAsk {
		t.Fatalf("rounded large integer unexpectedly consumed approval: %+v, %v", decision, err)
	}

	req.Arguments = []byte(reordered)
	decision, err = policy(WithRequestContext(context.Background(), changed), req)
	if err != nil || decision.Action != tool.PermissionActionAllow {
		t.Fatalf("equivalent reordered large integer did not consume approval: %+v, %v", decision, err)
	}
}

func TestApprovalArgumentNormalizationRejectsTrailingJSONValue(t *testing.T) {
	raw := []byte(`{"value":1} {"value":2}`)
	if got := string(normalizeJSON(raw)); got != string(raw) {
		t.Fatalf("trailing JSON value was normalized: %q", got)
	}
}

type failingApprovalBackend struct {
	issueErr   error
	consumeErr error
	issueCalls int
	consumeOK  bool
}

func (s *failingApprovalBackend) IssueApproval(context.Context, string, time.Duration) (string, error) {
	s.issueCalls++
	return "", s.issueErr
}

func (s *failingApprovalBackend) ConsumeApproval(context.Context, string, string) (bool, error) {
	return s.consumeOK, s.consumeErr
}

func TestApprovalPolicyStorageFailureAlwaysDeniesWithoutLeakingDetails(t *testing.T) {
	secretErr := errors.New("redis://alice:private-secret@private-host scope=confidential")
	for _, tc := range []struct {
		name           string
		store          *failingApprovalBackend
		wantIssueCalls int
	}{
		{"consume", &failingApprovalBackend{consumeErr: secretErr}, 0},
		{"uncertain-consume", &failingApprovalBackend{consumeErr: secretErr, consumeOK: true}, 0},
		{"issue", &failingApprovalBackend{issueErr: secretErr}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var observed ToolDecision
			policy := PermissionPolicyObserved(config.ToolPolicy{Allow: []string{"danger"}, RequireConfirm: []string{"danger"}}, tc.store,
				func(_ context.Context, decision ToolDecision) { observed = decision })
			decision, err := policy(WithRequestContext(context.Background(), RequestContext{TenantID: "tenant"}), &tool.PermissionRequest{ToolName: "danger"})
			if err != nil || decision.Action != tool.PermissionActionDeny || observed.Decision != string(tool.PermissionActionDeny) {
				t.Fatalf("storage failure must deny: %+v, %+v, %v", decision, observed, err)
			}
			for _, secret := range []string{"private-secret", "private-host", "confidential"} {
				if strings.Contains(decision.Reason, secret) || strings.Contains(observed.Reason, secret) {
					t.Fatal("backend details leaked in permission decision or observer")
				}
			}
			if tc.store.issueCalls != tc.wantIssueCalls {
				t.Fatalf("issue calls = %d, want %d", tc.store.issueCalls, tc.wantIssueCalls)
			}
		})
	}
}

func TestMemoryApprovalRejectsCanceledContextWithoutMutation(t *testing.T) {
	store := NewApprovalStore()
	nonce, err := store.Issue("scope", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.IssueApproval(ctx, "other", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("issue error = %v", err)
	}
	if ok, err := store.ConsumeApproval(ctx, nonce, "scope"); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("consume = %v, %v", ok, err)
	}
	if !store.Consume(nonce, "scope") {
		t.Fatal("canceled consume burned token")
	}
}

func TestMemoryApprovalCollisionNeverOverwritesScope(t *testing.T) {
	store := NewApprovalStore()
	store.newNonce = func() (string, error) { return "collision", nil }
	if _, err := store.Issue("original", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Issue("replacement", time.Minute); err == nil {
		t.Fatal("collision unexpectedly succeeded")
	}
	if !store.Consume("collision", "original") {
		t.Fatal("collision replaced original scope")
	}
}

func TestApprovalBackendTypedNilFailsClosed(t *testing.T) {
	for _, backend := range []ApprovalBackend{(*ApprovalStore)(nil), (*RedisApprovalStore)(nil)} {
		policy := PermissionPolicy(config.ToolPolicy{Allow: []string{"danger"}, RequireConfirm: []string{"danger"}}, backend)
		decision, err := policy(WithRequestContext(context.Background(), RequestContext{TenantID: "tenant"}), &tool.PermissionRequest{ToolName: "danger"})
		if err != nil || decision.Action != tool.PermissionActionDeny {
			t.Fatalf("nil backend = %+v, %v", decision, err)
		}
	}
}
