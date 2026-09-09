//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package worker

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// approvalPauseSessionService preserves the exact resume point for a
// confirmation-gated tool. The upstream Runner deliberately turns ordinary
// tool errors into model-visible tool results. That is useful for recoverable
// tool failures, but an approval pause must not let the model consume a
// synthetic denial and produce a later response: doing so destroys the
// pending tool-call state needed for a one-time approved resume.
//
// The wrapper is inert for all ordinary invocations. Once the governance
// plugin has created a challenge, it permits only the matching assistant
// tool-call event to reach Session and discards later events in that aborted
// attempt. The Worker then returns the typed approval requirement; a later
// approved retry starts with WithResume(true) and persists its real tool
// result and final model response normally.
type approvalPauseSessionService struct {
	session.Service
}

func newApprovalPauseSessionService(delegate session.Service) session.Service {
	if delegate == nil {
		return nil
	}
	return &approvalPauseSessionService{Service: delegate}
}

func (s *approvalPauseSessionService) AppendEvent(
	ctx context.Context,
	sess *session.Session,
	evt *event.Event,
	options ...session.Option,
) error {
	if s == nil || s.Service == nil {
		return session.ErrNilSession
	}
	if state, ok := governance.ApprovalCapabilityFromContext(ctx); ok {
		if challenge, pending := state.Challenge(); pending && !approvalToolCallEventMatches(evt, challenge) {
			return nil
		}
	}
	return s.Service.AppendEvent(ctx, sess, evt, options...)
}

func approvalToolCallEventMatches(evt *event.Event, challenge governance.ApprovalChallenge) bool {
	if evt == nil || evt.RequestID != challenge.Request.InvocationID || evt.Response == nil ||
		evt.IsPartial || !evt.IsValidContent() || !evt.Response.IsToolCallResponse() ||
		len(evt.Response.Choices) != 1 {
		return false
	}
	calls := evt.Response.Choices[0].Message.ToolCalls
	if len(calls) != 1 || calls[0].Function.Name != challenge.Request.ToolName {
		return false
	}
	argsHash, err := governance.CanonicalArgsHash(calls[0].Function.Arguments)
	return err == nil && argsHash == challenge.Request.ArgsHash
}
