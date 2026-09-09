//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Operation identifies an allowlisted runtime operation safe to expose in a
// trace. It deliberately excludes tenant, user, session, tool, model, and
// backend identifiers because those values may be sensitive or unbounded.
type Operation string

const (
	// OperationWorkerProcess covers the Worker-owned request lifecycle.
	OperationWorkerProcess Operation = "worker.process"
	// OperationRunnerRun covers one Runner invocation inside a Worker.
	OperationRunnerRun Operation = "runner.run"
	// OperationInboxProcess covers one durable Inbox processing attempt.
	OperationInboxProcess Operation = "inbox.process"
	// OperationOutboxDeliver covers one durable Outbox delivery attempt.
	OperationOutboxDeliver Operation = "outbox.deliver"
	// OperationToolInvoke covers a governed tool invocation.
	OperationToolInvoke Operation = "tool.invoke"
	// OperationSessionRead covers a fenced Session read.
	OperationSessionRead Operation = "session.read"
	// OperationSessionWrite covers a fenced Session write.
	OperationSessionWrite Operation = "session.write"
	// OperationMemoryRead covers a fenced Memory read.
	OperationMemoryRead Operation = "memory.read"
	// OperationMemoryWrite covers a fenced Memory write.
	OperationMemoryWrite Operation = "memory.write"

	operationUnknown Operation = "operation.unknown"
)

// StartOperation starts a span whose name is drawn from the fixed runtime
// operation allowlist. Unknown values are normalized so a future caller cannot
// accidentally turn an untrusted identifier into telemetry data.
func StartOperation(ctx context.Context, operation Operation) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	return otel.Tracer("enterprise/runtime").Start(ctx, string(allowlistedOperation(operation)))
}

// EndOperation records only a credential-free stable error code before ending
// span. It intentionally does not call RecordError because error text can
// contain request data, credentials, or provider response bodies.
func EndOperation(span trace.Span, err error) {
	if span == nil {
		return
	}
	code := StableErrorCode(err)
	span.SetAttributes(attribute.String("error.code", code))
	if err == nil {
		span.SetStatus(codes.Ok, "")
	} else {
		span.SetStatus(codes.Error, code)
	}
	span.End()
}

func allowlistedOperation(operation Operation) Operation {
	switch operation {
	case OperationWorkerProcess,
		OperationRunnerRun,
		OperationInboxProcess,
		OperationOutboxDeliver,
		OperationToolInvoke,
		OperationSessionRead,
		OperationSessionWrite,
		OperationMemoryRead,
		OperationMemoryWrite:
		return operation
	default:
		return operationUnknown
	}
}
