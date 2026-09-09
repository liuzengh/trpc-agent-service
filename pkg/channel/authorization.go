// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package channel

import (
	"errors"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// ErrInboundIdentityDenied is intentionally opaque: provider identifiers and
// tenant policy contents must not be copied into HTTP responses or logs.
var ErrInboundIdentityDenied = errors.New("inbound channel identity is not authorized")

// AuthorizeInbound applies the binding's fail-closed provider identity policy.
func AuthorizeInbound(binding *tenant.ChannelBinding, msg *InboundMessage) error {
	if binding == nil || msg == nil || msg.ExternalUserID == "" || msg.ConversationID == "" {
		return ErrInboundIdentityDenied
	}
	policy := binding.AccessPolicy
	if !containsExact(policy.AllowedUsers, msg.ExternalUserID) {
		return ErrInboundIdentityDenied
	}
	if msg.IsGroupChat {
		if !policy.AllowGroupMessages || !containsExact(policy.AllowedGroups, msg.ConversationID) {
			return ErrInboundIdentityDenied
		}
		return nil
	}
	if !policy.AllowDirectMessages {
		return ErrInboundIdentityDenied
	}
	return nil
}

func containsExact(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
