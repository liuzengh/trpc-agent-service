//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"time"
	"unicode"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/resultcache"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

const maxWorkerRequestBytes = 1 << 20

func decodeWorkerRequest(writer http.ResponseWriter, request *http.Request, target *worker.Request) bool {
	if request == nil || request.Body == nil || target == nil {
		http.Error(writer, "Invalid request", http.StatusBadRequest)
		return false
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		http.Error(writer, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return false
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxWorkerRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			http.Error(writer, "Request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(writer, "Invalid request", http.StatusBadRequest)
		}
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(writer, "Request must contain exactly one JSON value", http.StatusBadRequest)
		return false
	}
	return true
}

func validateWorkerExecutionContract(writer http.ResponseWriter, request *worker.Request, executionTimeout time.Duration) bool {
	if request == nil {
		http.Error(writer, "Worker execution contract unavailable", http.StatusServiceUnavailable)
		return false
	}
	if err := worker.ValidateExecutionContract(request.ExecutionContract, executionTimeout); err != nil {
		writer.Header().Set("Retry-After", "2")
		http.Error(writer, "Worker execution contract unavailable", http.StatusServiceUnavailable)
		return false
	}
	return true
}

func writeLegacyProcessUnavailable(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Retry-After", "2")
	http.Error(writer, "Versioned Worker execution contract required", http.StatusServiceUnavailable)
}

func registerWorkerProcessRoutes(router *http.ServeMux, protected http.Handler) {
	if router == nil || protected == nil {
		return
	}
	router.Handle("/process", protected)
	router.Handle(worker.ExecutionContractProcessPath, protected)
}

func validateWorkerRequest(request worker.Request) error {
	fields := []struct {
		name  string
		value string
		max   int
	}{
		{"tenant", request.TenantID, 64}, {"channel", request.ChannelType, 32},
		{"conversation", request.ConversationID, 256}, {"message", request.MessageID, 256},
		{"agent app", request.AgentApp, 128}, {"idempotency key", request.IdempotencyKey, 256},
		{"user", request.UserID, 255}, {"session", request.SessionID, 255},
	}
	for _, field := range fields {
		if !validWorkerField(field.value, field.max) {
			return fmt.Errorf("%s is invalid", field.name)
		}
	}
	if err := tenant.ValidateTenantID(request.TenantID); err != nil {
		return err
	}
	if err := tenant.ValidateAgentAppName(request.AgentApp); err != nil {
		return err
	}
	if len(request.PayloadHash) != sha256.Size*2 {
		return fmt.Errorf("payload hash is invalid")
	}
	if _, err := hex.DecodeString(request.PayloadHash); err != nil {
		return fmt.Errorf("payload hash is invalid")
	}
	return nil
}

func validWorkerField(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Cf) {
			return false
		}
	}
	return true
}

func resultIdentity(req worker.Request, resolved *controlplane.ResolvedDeployment) resultcache.Identity {
	return resultcache.Identity{TenantID: req.TenantID, IdempotencyKey: req.IdempotencyKey, PayloadHash: req.PayloadHash, SessionID: req.SessionID, AgentAppID: resolved.AgentAppID, AgentVersionID: resolved.VersionID, DeploymentID: resolved.DeploymentID}
}

func failExecution(parent context.Context, recorder *controlplane.ExecutionRecorder, handle controlplane.ExecutionHandle, errorType string, safeToRetry bool) {
	ctx, cancel := detachedContext(parent, 5*time.Second)
	defer cancel()
	if err := recorder.Fail(ctx, handle, controlplane.Failure{Code: errorType, SafeToRetry: safeToRetry}); err != nil {
		log.Printf("failed to finalize execution record %d: error=%s", handle.ID, telemetry.StableErrorCode(err))
	}
}
