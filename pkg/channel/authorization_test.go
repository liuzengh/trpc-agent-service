// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package channel

import (
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestAuthorizeInboundRequiresExplicitUserAndConversation(t *testing.T) {
	binding := &tenant.ChannelBinding{AccessPolicy: tenant.ChannelAccessPolicy{
		AllowDirectMessages: true,
		AllowGroupMessages:  true,
		AllowedUsers:        []string{"user-1"},
		AllowedGroups:       []string{"group-1"},
	}}
	tests := []struct {
		name string
		msg  *InboundMessage
		want error
	}{
		{
			name: "allowed direct user",
			msg:  &InboundMessage{ExternalUserID: "user-1", ConversationID: "direct-1"},
		},
		{
			name: "unknown direct user",
			msg:  &InboundMessage{ExternalUserID: "user-2", ConversationID: "direct-2"},
			want: ErrInboundIdentityDenied,
		},
		{
			name: "allowed user in allowed group",
			msg: &InboundMessage{
				ExternalUserID: "user-1", ConversationID: "group-1", IsGroupChat: true,
			},
		},
		{
			name: "allowed user in unknown group",
			msg: &InboundMessage{
				ExternalUserID: "user-1", ConversationID: "group-2", IsGroupChat: true,
			},
			want: ErrInboundIdentityDenied,
		},
		{
			name: "unknown user in allowed group",
			msg: &InboundMessage{
				ExternalUserID: "user-2", ConversationID: "group-1", IsGroupChat: true,
			},
			want: ErrInboundIdentityDenied,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := AuthorizeInbound(binding, test.msg)
			if !errors.Is(err, test.want) {
				t.Fatalf("AuthorizeInbound error=%v, want %v", err, test.want)
			}
		})
	}
}

func TestAuthorizeInboundFailsClosedOnMissingPolicy(t *testing.T) {
	if err := AuthorizeInbound(&tenant.ChannelBinding{}, &InboundMessage{
		ExternalUserID: "user-1", ConversationID: "direct-1",
	}); !errors.Is(err, ErrInboundIdentityDenied) {
		t.Fatalf("missing policy error=%v", err)
	}
}
