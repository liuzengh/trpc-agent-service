package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

const preflightArgsHash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func preflightRequest() worker.Request {
	return worker.Request{
		TenantID:       "tenant-a",
		UserID:         "alice",
		SessionOwnerID: "group-owner",
		SessionID:      "session-a",
		IdempotencyKey: "inbox:42",
	}
}

func preflightApprovalRequest(req worker.Request) governance.ApprovalRequest {
	return governance.ApprovalRequest{
		TenantID:       req.TenantID,
		UserID:         req.UserID,
		SessionOwnerID: req.SessionOwnerID,
		SessionID:      req.SessionID,
		ToolName:       "delete_file",
		ArgsHash:       preflightArgsHash,
		InvocationID:   req.IdempotencyKey,
	}
}

func TestPreflightApprovalResumePausesBeforeExecutionWhenGrantIsPending(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	req := preflightRequest()
	want, err := store.CreateChallenge(context.Background(), preflightApprovalRequest(req), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	got, waiting, err := preflightApprovalResume(context.Background(), req, store)
	if err != nil {
		t.Fatalf("preflightApprovalResume: %v", err)
	}
	if !waiting {
		t.Fatal("ungranted challenge was admitted for execution")
	}
	if got.ChallengeID != want.ChallengeID {
		t.Fatalf("challenge=%q, want %q", got.ChallengeID, want.ChallengeID)
	}
	granted, err := store.IsApprovalGranted(context.Background(), want.ChallengeID)
	if err != nil {
		t.Fatal(err)
	}
	if granted {
		t.Fatal("preflight changed an ungranted challenge")
	}
}

func TestAdmitExecutionWithApprovalGateDoesNotStartForRepeatedPendingPolls(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	req := preflightRequest()
	challenge, err := store.CreateChallenge(context.Background(), preflightApprovalRequest(req), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var starts int
	for i := 0; i < 3; i++ {
		handle, got, waiting, err := admitExecutionWithApprovalGate(
			context.Background(), &req, store, func() (controlplane.ExecutionHandle, error) {
				starts++
				return controlplane.ExecutionHandle{ID: 1}, nil
			},
		)
		if err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
		if !waiting || got.ChallengeID != challenge.ChallengeID || handle.ID != 0 {
			t.Fatalf("poll %d result=(%#v,%#v,%v), want waiting without handle", i, handle, got, waiting)
		}
	}
	if starts != 0 {
		t.Fatalf("execution admission callback called %d times for pending polls, want 0", starts)
	}
}

func TestAdmitExecutionWithApprovalGateMarksExpectedGrantedChallenge(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	req := preflightRequest()
	challenge, err := store.CreateChallenge(context.Background(), preflightApprovalRequest(req), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(context.Background(), challenge.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}
	var starts int
	handle, got, waiting, err := admitExecutionWithApprovalGate(
		context.Background(), &req, store, func() (controlplane.ExecutionHandle, error) {
			starts++
			return controlplane.ExecutionHandle{ID: 7}, nil
		},
	)
	if err != nil || waiting || handle.ID != 7 || got.ChallengeID != challenge.ChallengeID {
		t.Fatalf("granted result=(%#v,%#v,%v) err=%v", handle, got, waiting, err)
	}
	if starts != 1 || req.ApprovalResumeChallengeID != challenge.ChallengeID {
		t.Fatalf("starts=%d marker=%q, want one start and challenge marker", starts, req.ApprovalResumeChallengeID)
	}
}

func TestPreflightApprovalResumeDoesNotConsumeGrantedApproval(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	req := preflightRequest()
	approval := preflightApprovalRequest(req)
	challenge, err := store.CreateChallenge(context.Background(), approval, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Grant(context.Background(), challenge.ChallengeID, "operator-1"); err != nil {
		t.Fatal(err)
	}

	got, waiting, err := preflightApprovalResume(context.Background(), req, store)
	if err != nil {
		t.Fatalf("preflightApprovalResume: %v", err)
	}
	if waiting || got.ChallengeID != challenge.ChallengeID {
		t.Fatalf("preflight result=(%#v, %v), want granted challenge and no pause", got, waiting)
	}
	// The actual Worker consumes the one-time grant after it has acquired its
	// execution fence. A read-only preflight must leave that capability intact.
	if err := store.ConsumeGranted(context.Background(), approval); err != nil {
		t.Fatalf("ConsumeGranted after preflight: %v", err)
	}
}

func TestPreflightApprovalResumeNoChallengeAllowsAdmission(t *testing.T) {
	got, waiting, err := preflightApprovalResume(context.Background(), preflightRequest(), governance.NewMemoryApprovalStore())
	if err != nil {
		t.Fatalf("preflightApprovalResume: %v", err)
	}
	if waiting || got != (governance.ApprovalChallenge{}) {
		t.Fatalf("preflight result=(%#v, %v), want no challenge", got, waiting)
	}
}

func TestPreflightApprovalResumeFailsClosedWithoutAtomicInspector(t *testing.T) {
	store := approvalStoreWithoutAtomicInspector{}
	if _, waiting, err := preflightApprovalResume(context.Background(), preflightRequest(), store); err == nil || waiting {
		t.Fatalf("result waiting=%v err=%v, want unavailable atomic inspector", waiting, err)
	}
}

func TestPreflightApprovalResumeHonorsCanceledContextBeforeStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := staticPreflightApprovalStore{challenge: governance.ApprovalChallenge{}}
	if _, waiting, err := preflightApprovalResume(ctx, preflightRequest(), store); !errors.Is(err, context.Canceled) || waiting {
		t.Fatalf("result waiting=%v err=%v, want context.Canceled", waiting, err)
	}
}

func TestPreflightApprovalResumeFailsClosedOnAmbiguousChallenge(t *testing.T) {
	store := governance.NewMemoryApprovalStore()
	req := preflightRequest()
	approval := preflightApprovalRequest(req)
	if _, err := store.CreateChallenge(context.Background(), approval, time.Minute); err != nil {
		t.Fatal(err)
	}
	approval.ToolName = "delete_directory"
	if _, err := store.CreateChallenge(context.Background(), approval, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, waiting, err := preflightApprovalResume(context.Background(), req, store); !errors.Is(err, governance.ErrApprovalAmbiguous) || waiting {
		t.Fatalf("result waiting=%v err=%v, want fail-closed ambiguity", waiting, err)
	}
}

func TestPreflightApprovalResumeUsesStoreAuthorityForExpiry(t *testing.T) {
	req := preflightRequest()
	challenge := governance.ApprovalChallenge{
		ChallengeID: "challenge-clock",
		Request:     preflightApprovalRequest(req),
		// A real FindActiveApproval implementation has already checked this
		// against its database clock. The local value is deliberately stale to
		// prove that node clock skew cannot override that decision.
		ExpiresAt: time.Now().Add(-time.Hour),
	}
	store := staticPreflightApprovalStore{challenge: challenge}
	got, waiting, err := preflightApprovalResume(context.Background(), req, store)
	if err != nil || !waiting || got.ChallengeID != challenge.ChallengeID {
		t.Fatalf("result=(%#v, %v) err=%v, want store-authoritative pause", got, waiting, err)
	}
}

func TestPreflightApprovalResumeRejectsScopeMismatch(t *testing.T) {
	req := preflightRequest()
	challenge := governance.ApprovalChallenge{
		ChallengeID: "challenge-mismatch",
		Request:     preflightApprovalRequest(req),
		ExpiresAt:   time.Now().Add(time.Minute),
	}
	challenge.Request.UserID = "bob"
	store := staticPreflightApprovalStore{challenge: challenge}
	if _, waiting, err := preflightApprovalResume(context.Background(), req, store); err == nil || waiting {
		t.Fatalf("result waiting=%v err=%v, want scope mismatch rejection", waiting, err)
	}
}

func TestWriteApprovalRequiredResponseIsBoundedAndSecretFree(t *testing.T) {
	challenge := governance.ApprovalChallenge{
		ChallengeID: "challenge-response",
		Request:     preflightApprovalRequest(preflightRequest()),
		ExpiresAt:   time.Now().Add(time.Minute),
	}
	response := httptest.NewRecorder()
	writeApprovalRequiredResponse(response, challenge)
	if response.Code != http.StatusPreconditionRequired {
		t.Fatalf("status=%d, want %d", response.Code, http.StatusPreconditionRequired)
	}
	if response.Header().Get("Retry-After") != "5" {
		t.Fatalf("Retry-After=%q, want 5", response.Header().Get("Retry-After"))
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", response.Header().Get("Cache-Control"))
	}
	if !strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("Content-Type=%q, want JSON", response.Header().Get("Content-Type"))
	}
	var body map[string]interface{}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["challenge_id"] != challenge.ChallengeID || body["error"] != "tool_approval_required" {
		t.Fatalf("response body=%v", body)
	}
	if strings.Contains(response.Body.String(), "delete_file") || strings.Contains(response.Body.String(), preflightArgsHash) {
		t.Fatal("approval response leaked tool scope material")
	}
}

func TestWriteApprovalRequiredResponseRejectsMalformedChallenge(t *testing.T) {
	response := httptest.NewRecorder()
	writeApprovalRequiredResponse(response, governance.ApprovalChallenge{
		ChallengeID: strings.Repeat("x", 129), ExpiresAt: time.Now().Add(time.Minute),
	})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if strings.Contains(response.Body.String(), "challenge_id") {
		t.Fatal("malformed challenge ID was emitted")
	}
}

// staticPreflightApprovalStore models the durable store boundary. The
// preflight is deliberately independent of a concrete backend so tests can
// exercise clock and corruption behavior without an external database.
type staticPreflightApprovalStore struct {
	governance.ApprovalStore
	challenge governance.ApprovalChallenge
	findErr   error
	grantErr  error
}

type approvalStoreWithoutAtomicInspector struct{}

func (approvalStoreWithoutAtomicInspector) CreateChallenge(context.Context, governance.ApprovalRequest, time.Duration) (governance.ApprovalChallenge, error) {
	return governance.ApprovalChallenge{}, governance.ErrApprovalStoreUnavailable
}

func (approvalStoreWithoutAtomicInspector) Grant(context.Context, string, string) (governance.ApprovalGrant, error) {
	return governance.ApprovalGrant{}, governance.ErrApprovalStoreUnavailable
}

func (approvalStoreWithoutAtomicInspector) Consume(context.Context, governance.ApprovalRequest, string) error {
	return governance.ErrApprovalStoreUnavailable
}

func (s staticPreflightApprovalStore) InspectApprovalResume(context.Context, governance.ApprovalInvocationScope) (governance.ApprovalResumeState, error) {
	if s.findErr != nil {
		return governance.ApprovalResumeState{}, s.findErr
	}
	return governance.ApprovalResumeState{Challenge: s.challenge}, nil
}

func (s staticPreflightApprovalStore) FindActiveApproval(context.Context, governance.ApprovalInvocationScope) (governance.ApprovalChallenge, error) {
	if s.findErr != nil {
		return governance.ApprovalChallenge{}, s.findErr
	}
	return s.challenge, nil
}

func (s staticPreflightApprovalStore) IsApprovalGranted(context.Context, string) (bool, error) {
	if s.grantErr != nil {
		return false, s.grantErr
	}
	return false, nil
}
