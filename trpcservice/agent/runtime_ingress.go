package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// prepareInboundExecution normalizes the actor/subject metadata and resolves
// the channel conversation to the canonical platform Session before any
// execution lease is acquired.
func (r *Runtime) prepareInboundExecution(
	ctx context.Context,
	snapshot tenant.Snapshot,
	externalBindingID string,
	inbound channels.InboundMessage,
) (context.Context, channels.InboundMessage, string, error) {
	var err error
	if strings.TrimSpace(inbound.SubjectID) == "" {
		switch {
		case inbound.Channel == channels.Web:
			inbound.SubjectID = strings.TrimSpace(inbound.WebOwnerID)
			if inbound.SubjectID == "" {
				inbound.SubjectID = strings.TrimSpace(inbound.SenderID)
			}
			inbound.OwnerPlatformUserID = inbound.SubjectID
			inbound.ActorPlatformUserID = inbound.SubjectID
		case inbound.IsGroupConversation():
			inbound.SubjectID, err = channels.GroupSubjectID(inbound.Channel, externalBindingID, inbound.ConversationID)
		default:
			inbound.SubjectID, err = channels.ExternalSubjectID(inbound.Channel, externalBindingID, inbound.SenderID)
		}
		if err != nil {
			return ctx, channels.InboundMessage{}, "", fmt.Errorf("resolve normalized subject fallback: %w", err)
		}
	}

	if inbound.TriggerType == "" {
		if inbound.IsGroupConversation() {
			inbound.TriggerType = channels.TriggerMention
		} else {
			inbound.TriggerType = channels.TriggerDirect
		}
	}

	role := identity.RoleMember
	if r.roleResolver != nil && strings.TrimSpace(inbound.ActorPlatformUserID) != "" {
		role, err = r.roleResolver.RoleFor(ctx, snapshot.Config.TenantID, inbound.ActorPlatformUserID)
		if err != nil {
			return ctx, channels.InboundMessage{}, "", fmt.Errorf("resolve tenant actor role: %w", err)
		}
	}
	ctx = WithActorRole(ctx, string(role))

	sessionKey := resolvedSessionKeyFromContext(ctx)
	if sessionKey != "" {
		return ctx, inbound, sessionKey, nil
	}
	preferredSessionKey, err := channels.BuildSessionKey(snapshot.Config.TenantID, snapshot.Config.AppCode, uuid.NewString())
	if err != nil {
		return ctx, channels.InboundMessage{}, "", fmt.Errorf("build tenant session key: %w", err)
	}
	scope := string(inbound.ConversationScope)
	if scope == "" {
		scope = string(channels.ConversationDirect)
	}
	sessionKey, err = r.sessionManager.ResolveSession(ctx, storage.SessionRoute{
		TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode,
		Channel: string(inbound.Channel), BindingID: externalBindingID,
		ConversationID: inbound.ConversationID, ExternalUserID: inbound.SenderID,
		SubjectID: inbound.SubjectID, OwnerPlatformUserID: inbound.OwnerPlatformUserID, Scope: scope,
	}, preferredSessionKey)
	if err != nil {
		return ctx, channels.InboundMessage{}, "", fmt.Errorf("resolve session route: %w", err)
	}
	return ctx, inbound, sessionKey, nil
}
