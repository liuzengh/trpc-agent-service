//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package tenant

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAuditActorContextFailsClosed(t *testing.T) {
	if _, err := auditActorFromContext(context.Background()); !errors.Is(err, ErrAuditActorRequired) {
		t.Fatalf("missing actor error = %v", err)
	}
	ctx := ContextWithAuditActor(context.Background(), "  operator@example.com  ")
	actor, err := auditActorFromContext(ctx)
	if err != nil || actor != "operator@example.com" {
		t.Fatalf("actor = %q, err = %v", actor, err)
	}
	tooLong := ContextWithAuditActor(context.Background(), strings.Repeat("x", maxAuditActorLength+1))
	if _, err := auditActorFromContext(tooLong); !errors.Is(err, ErrAuditActorRequired) {
		t.Fatalf("oversized actor error = %v", err)
	}
}

func TestAuditActorContextHandlesNilContext(t *testing.T) {
	if _, err := auditActorFromContext(nil); !errors.Is(err, ErrAuditActorRequired) {
		t.Fatalf("nil context error = %v", err)
	}
	if ctx := ContextWithAuditActor(nil, "operator"); ctx == nil {
		t.Fatal("ContextWithAuditActor returned a nil context")
	}
}
