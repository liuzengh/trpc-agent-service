//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

// workerProcessFailure is the durable execution policy decision returned to
// the HTTP adapter. It keeps retry safety, durable error code, and status code
// together so callers cannot accidentally diverge those contracts.
type workerProcessFailure struct {
	code        string
	safeToRetry bool
	statusCode  int
	message     string
}

func classifyWorkerInitializationFailure(ctx context.Context, err error) workerProcessFailure {
	if errors.Is(err, context.DeadlineExceeded) ||
		(ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded)) {
		return workerProcessFailure{code: "execution_preflight_timeout", safeToRetry: true, statusCode: http.StatusServiceUnavailable, message: "Execution admission timed out"}
	}
	if errors.Is(err, worker.ErrCacheSaturated) {
		return workerProcessFailure{code: "worker_capacity_exhausted", safeToRetry: true, statusCode: http.StatusServiceUnavailable, message: "Worker capacity temporarily exhausted"}
	}
	return workerProcessFailure{code: "worker_initialization_failed", safeToRetry: true, statusCode: http.StatusInternalServerError, message: "Worker initialization failed"}
}

// classifyWorkerProcessFailure keeps durable execution state and HTTP
// semantics aligned. A preflight timeout is retryable; failures after Runner
// entry may have side effects and therefore require reconciliation.
func classifyWorkerProcessFailure(err error) workerProcessFailure {
	if errors.Is(err, worker.ErrWorkerExecutionOutcomeUnknown) {
		code := "execution_outcome_unknown"
		if errors.Is(err, worker.ErrExecutionTimedOut) {
			code = "execution_timeout"
		}
		return classifyWorkerPostRunnerFailure(code)
	}
	switch {
	case errors.Is(err, worker.ErrExecutionPreflightPermanent):
		return workerProcessFailure{code: "execution_preflight_rejected", safeToRetry: false, statusCode: http.StatusBadRequest, message: "Execution request was rejected"}
	case errors.Is(err, worker.ErrExecutionPreflightTimedOut):
		return workerProcessFailure{code: "execution_preflight_timeout", safeToRetry: true, statusCode: http.StatusServiceUnavailable, message: "Execution admission timed out"}
	case errors.Is(err, worker.ErrExecutionPreflight):
		return workerProcessFailure{code: "execution_preflight_failed", safeToRetry: true, statusCode: http.StatusServiceUnavailable, message: "Execution admission temporarily unavailable"}
	case errors.Is(err, worker.ErrExecutionTimedOut):
		return workerProcessFailure{code: "execution_timeout", safeToRetry: false, statusCode: http.StatusLocked, message: "Session requires operator reconciliation"}
	default:
		return classifyWorkerPostRunnerFailure("execution_outcome_unknown")
	}
}

// classifyWorkerPostRunnerFailure is the policy seam for failures after
// Runner.Run has started. Such attempts are never retry-safe.
func classifyWorkerPostRunnerFailure(code string) workerProcessFailure {
	code = strings.TrimSpace(code)
	if code == "" {
		code = "execution_outcome_unknown"
	}
	return workerProcessFailure{code: code, safeToRetry: false, statusCode: http.StatusLocked, message: "Session requires operator reconciliation"}
}

// writeExecutionStartError maps control-plane state conflicts to the stable
// HTTP contract consumed by the FIFO consumer.
func writeExecutionStartError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, controlplane.ErrSessionExecutionInProgress), errors.Is(err, controlplane.ErrExecutionInProgress):
		w.Header().Set("Retry-After", "2")
		http.Error(w, "Execution is already in progress", http.StatusConflict)
	case errors.Is(err, controlplane.ErrSessionReconciliationRequired), errors.Is(err, controlplane.ErrExecutionRetryUnsafe), errors.Is(err, controlplane.ErrExecutionOutcomeUnknown):
		http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
	case errors.Is(err, controlplane.ErrExecutionAlreadySucceeded):
		http.Error(w, "Execution succeeded but its cached result is unavailable", http.StatusGone)
	case errors.Is(err, controlplane.ErrPayloadConflict), errors.Is(err, controlplane.ErrRequestIdentityConflict), errors.Is(err, controlplane.ErrVersionBindingConflict):
		http.Error(w, "Idempotency key conflicts with an earlier request", http.StatusConflict)
	default:
		log.Printf("failed to record resolved execution: error=%s", telemetry.StableErrorCode(err))
		http.Error(w, "Execution audit unavailable", http.StatusServiceUnavailable)
	}
}

func detachedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}
