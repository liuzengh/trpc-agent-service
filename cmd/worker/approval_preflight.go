//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func writeHTTPApprovalPause(w http.ResponseWriter, err error, markPaused func()) bool {
	if _, paused := worker.AsApprovalPause(err); !paused {
		return false
	}
	var approvalErr *governance.ApprovalRequiredError
	if !errors.As(err, &approvalErr) || approvalErr == nil {
		return false
	}
	if markPaused != nil {
		markPaused()
	}
	writeApprovalRequiredResponse(w, approvalErr.Challenge)
	return true
}

// preflightApprovalResume checks a durable retry before the caller creates an
// execution record. A pending challenge is only claimable after its grant is
// visible in the same durable approval store; all inspection failures fail
// closed so an unavailable control plane cannot turn a paused tool call into a
// fresh model invocation.
func preflightApprovalResume(
	ctx context.Context,
	req worker.Request,
	store governance.ApprovalStore,
) (governance.ApprovalChallenge, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return governance.ApprovalChallenge{}, false, fmt.Errorf("inspect active approval: %w", err)
	}
	if isNilApprovalStore(store) {
		return governance.ApprovalChallenge{}, false, fmt.Errorf("approval resume inspection is unavailable")
	}
	resumeInspector, ok := store.(governance.ApprovalResumeStateInspector)
	if !ok {
		return governance.ApprovalChallenge{}, false, fmt.Errorf("atomic approval resume inspection is unavailable")
	}
	scope := governance.ApprovalInvocationScope{
		TenantID:       req.TenantID,
		UserID:         req.UserID,
		SessionOwnerID: req.SessionOwnerID,
		SessionID:      req.SessionID,
		InvocationID:   req.IdempotencyKey,
	}
	state, err := resumeInspector.InspectApprovalResume(ctx, scope)
	if errors.Is(err, governance.ErrApprovalNotFound) {
		return governance.ApprovalChallenge{}, false, nil
	}
	if err != nil {
		return governance.ApprovalChallenge{}, false, fmt.Errorf("inspect active approval atomically: %w", err)
	}
	challenge := state.Challenge
	if err := governance.ValidateApprovalChallenge(challenge); err != nil {
		return governance.ApprovalChallenge{}, false, fmt.Errorf("active approval metadata is invalid: %w", err)
	}
	if !approvalChallengeMatchesScope(challenge, scope) {
		return governance.ApprovalChallenge{}, false, fmt.Errorf("active approval scope is invalid")
	}
	if !state.Granted {
		return challenge, true, nil
	}
	return challenge, false, nil
}

// admitExecutionWithApprovalGate is the sole execution-record admission
// helper used by the HTTP boundary. A pending, ungranted challenge returns
// before start is called; a granted challenge carries an internal expected ID
// so Worker can reject a grant consumed by a concurrent retry instead of
// treating the request as a fresh user turn.
func admitExecutionWithApprovalGate(
	ctx context.Context,
	req *worker.Request,
	store governance.ApprovalStore,
	start func() (controlplane.ExecutionHandle, error),
) (controlplane.ExecutionHandle, governance.ApprovalChallenge, bool, error) {
	if req == nil {
		return controlplane.ExecutionHandle{}, governance.ApprovalChallenge{}, false, fmt.Errorf("worker request is required")
	}
	if start == nil {
		return controlplane.ExecutionHandle{}, governance.ApprovalChallenge{}, false, fmt.Errorf("execution admission callback is required")
	}
	challenge, waiting, err := preflightApprovalResume(ctx, *req, store)
	if err != nil {
		return controlplane.ExecutionHandle{}, governance.ApprovalChallenge{}, false, err
	}
	if waiting {
		return controlplane.ExecutionHandle{}, challenge, true, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return controlplane.ExecutionHandle{}, governance.ApprovalChallenge{}, false, fmt.Errorf("admit execution: %w", err)
		}
	}
	if challenge.ChallengeID != "" {
		// This marker is not accepted from JSON and is validated again by the
		// Worker against the durable Session transcript and challenge row.
		req.ApprovalResumeChallengeID = challenge.ChallengeID
	}
	handle, err := start()
	return handle, challenge, false, err
}

func admitHTTPExecutionWithApprovalGate(
	w http.ResponseWriter,
	ctx context.Context,
	req *worker.Request,
	store governance.ApprovalStore,
	start func() (controlplane.ExecutionHandle, error),
) (controlplane.ExecutionHandle, bool) {
	startAttempted := false
	if start != nil {
		startExecution := start
		start = func() (controlplane.ExecutionHandle, error) {
			startAttempted = true
			return startExecution()
		}
	}
	handle, challenge, waiting, err := admitExecutionWithApprovalGate(ctx, req, store, start)
	if err != nil {
		// Execution conflicts retain their retry policy after the approval
		// gate. Only failures before start belong to approval inspection.
		if startAttempted {
			writeExecutionStartError(w, err)
		} else if errors.Is(err, governance.ErrApprovalAmbiguous) {
			log.Printf("approval resume state is ambiguous: error=%s", telemetry.StableErrorCode(err))
			http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
		} else {
			log.Printf("approval resume inspection failed: error=%s", telemetry.StableErrorCode(err))
			http.Error(w, "Approval state unavailable", http.StatusServiceUnavailable)
		}
		return controlplane.ExecutionHandle{}, false
	}
	if waiting {
		writeApprovalRequiredResponse(w, challenge)
		return controlplane.ExecutionHandle{}, false
	}
	return handle, true
}

func isNilApprovalStore(store governance.ApprovalStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func approvalChallengeMatchesScope(
	challenge governance.ApprovalChallenge,
	scope governance.ApprovalInvocationScope,
) bool {
	request := challenge.Request
	return governance.ValidateApprovalChallenge(challenge) == nil &&
		request.TenantID == scope.TenantID &&
		request.UserID == scope.UserID &&
		request.SessionOwnerID == scope.SessionOwnerID &&
		request.SessionID == scope.SessionID &&
		request.InvocationID == scope.InvocationID &&
		request.ToolName != "" && request.ArgsHash != ""
}

// writeApprovalRequiredResponse emits the bounded control response shared by
// the pre-execution gate and the normal Worker approval path. It never emits
// raw arguments or a grant token.
func writeApprovalRequiredResponse(w http.ResponseWriter, challenge governance.ApprovalChallenge) {
	w.Header().Set("Cache-Control", "no-store")
	if err := governance.ValidateApprovalChallenge(challenge); err != nil {
		http.Error(w, "Approval state unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Retry-After", "5")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusPreconditionRequired)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error":        "tool_approval_required",
		"challenge_id": challenge.ChallengeID,
		"expires_at":   challenge.ExpiresAt.UTC(),
	})
}
