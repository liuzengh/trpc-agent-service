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
)

const maxAuditActorLength = 256

type auditActorContextKey struct{}

// ErrAuditActorRequired means a mutating SQL repository operation did not
// carry a verified control-plane identity. Repository enforcement prevents a
// caller from bypassing the HTTP handler and creating an unaudited change.
var ErrAuditActorRequired = errors.New("control-plane audit actor is required")

// ContextWithAuditActor associates the identity authenticated by the admin
// boundary with a tenant mutation. The value is consumed only by the SQL
// repository, where the mutation and audit record share one transaction.
func ContextWithAuditActor(ctx context.Context, actor string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, auditActorContextKey{}, strings.TrimSpace(actor))
}

func auditActorFromContext(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", ErrAuditActorRequired
	}
	actor, _ := ctx.Value(auditActorContextKey{}).(string)
	actor = strings.TrimSpace(actor)
	if actor == "" || len(actor) > maxAuditActorLength {
		return "", ErrAuditActorRequired
	}
	return actor, nil
}
