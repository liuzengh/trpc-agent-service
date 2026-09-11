package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

var errChannelIngressRejected = errors.New("channel ingress rejected by policy")

type channelIngress struct {
	repository    tenant.Repository
	producer      messaging.Producer
	sessions      storage.SessionManager
	identities    messaging.ChannelIdentityResolver
	controls      *channelControlHandler
	artifacts     channelArtifactProvider
	manifests     *messaging.ExecutionManifestCodec
	resolveSender channelSenderResolver
}

func newChannelIngress(
	repository tenant.Repository,
	producer messaging.Producer,
	sessions storage.SessionManager,
	identities messaging.ChannelIdentityResolver,
	controls *channelControlHandler,
	artifacts channelArtifactProvider,
	manifests *messaging.ExecutionManifestCodec,
	resolveSender channelSenderResolver,
) (*channelIngress, error) {
	if repository == nil || producer == nil || sessions == nil || identities == nil || controls == nil || artifacts == nil || manifests == nil || resolveSender == nil {
		return nil, errors.New("channel ingress dependencies are incomplete")
	}
	return &channelIngress{
		repository: repository, producer: producer, sessions: sessions, identities: identities,
		controls: controls, artifacts: artifacts, manifests: manifests, resolveSender: resolveSender,
	}, nil
}

func (i *channelIngress) publishReliably(ctx context.Context, bindingID string, inbound channels.InboundMessage) error {
	delay := 100 * time.Millisecond
	for {
		err := i.publish(ctx, bindingID, inbound)
		if err == nil {
			return nil
		}
		if errors.Is(err, errChannelIngressRejected) {
			slog.Info("channel inbound rejected by policy",
				"channel", inbound.Channel, "binding_id", bindingID,
				"message_id", inbound.MessageID, "error", err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("channel inbound publish failed; retrying", "channel", inbound.Channel, "binding_id", bindingID, "message_id", inbound.MessageID, "error", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 5*time.Second)
	}
}

func (i *channelIngress) publish(ctx context.Context, bindingID string, inbound channels.InboundMessage) error {
	snapshot, err := i.repository.ResolveBinding(ctx, inbound.Channel, bindingID)
	if err != nil {
		return fmt.Errorf("resolve connector binding: %w", err)
	}
	if channels.RoutePlatformCommand(inbound) == channels.PlatformCommandNewSession {
		return i.handleNewSession(ctx, snapshot, bindingID, inbound)
	}
	if handled, err := i.controls.handle(ctx, snapshot, bindingID, inbound); handled || err != nil {
		return err
	}
	sessionKey, inbound, err := messaging.ResolveInboundSession(ctx, i.sessions, i.identities, snapshot, bindingID, inbound)
	if err != nil {
		return err
	}
	selection, err := tenant.ResolveRelease(ctx, i.repository, snapshot, tenant.ReleaseTarget{
		SessionKey: sessionKey, PlatformUserID: inbound.OwnerPlatformUserID,
		Ingress: tenant.ReleaseIngress(string(inbound.Channel), bindingID), Scope: string(inbound.ConversationScope),
	})
	if err != nil {
		return fmt.Errorf("resolve connector release: %w", err)
	}
	snapshot = selection.Snapshot
	progress := i.startProgress(ctx, snapshot, bindingID, &inbound)
	published := false
	defer func() {
		if !published {
			i.cancelProgress(progress)
		}
	}()
	var (
		artifactService agentartifact.Service
		artifactInfo    agentartifact.SessionInfo
		stagedFiles     []channels.InboundFile
	)
	if len(inbound.ReceivedFiles) > 0 {
		artifactService, err = i.artifacts.ArtifactService(ctx, snapshot.Config)
		if err != nil {
			return fmt.Errorf("resolve inbound file storage: %w", err)
		}
		artifactInfo = agentartifact.SessionInfo{
			AppName: snapshot.Config.AppName(), UserID: inbound.SubjectID, SessionID: sessionKey,
		}
		stagedFiles, err = messaging.StageInboundFiles(ctx, artifactService, artifactInfo, inbound.MessageID, inbound.ReceivedFiles)
		if err != nil {
			return fmt.Errorf("stage inbound files: %w", err)
		}
		inbound.Files = append(inbound.Files, stagedFiles...)
		inbound.ReceivedFiles = nil
	}
	envelope, err := messaging.NewInboundEnvelopeWithContext(ctx, selection, bindingID, sessionKey, inbound, i.manifests)
	if err != nil {
		messaging.DeleteInboundFiles(ctx, artifactService, artifactInfo, stagedFiles)
		return err
	}
	if err := i.producer.Publish(ctx, envelope); err != nil {
		messaging.DeleteInboundFiles(ctx, artifactService, artifactInfo, stagedFiles)
		return err
	}
	published = true
	return nil
}

func (i *channelIngress) handleNewSession(ctx context.Context, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage) error {
	commandStore, ok := i.sessions.(storage.SessionCommandStore)
	if !ok {
		return errors.New("channel session store does not support platform commands")
	}
	route, normalized, err := messaging.PrepareInboundSessionRoute(ctx, i.identities, snapshot, bindingID, inbound)
	if err != nil {
		return err
	}
	if normalized.ConversationScope == channels.ConversationGroup {
		actorPlatformUserID := strings.TrimSpace(normalized.ActorPlatformUserID)
		if actorPlatformUserID == "" {
			return fmt.Errorf("%w: group /new requires a tenant administrator", errChannelIngressRejected)
		}
		role, roleErr := i.identities.RoleFor(ctx, snapshot.Config.TenantID, actorPlatformUserID)
		if roleErr != nil || role != identity.RoleAdmin {
			return fmt.Errorf("%w: group /new requires a tenant administrator", errChannelIngressRejected)
		}
	}
	var progress *inboundProgressHandle
	if normalized.Channel == channels.WeCom {
		progress = i.startProgress(ctx, snapshot, bindingID, &normalized)
		if progress == nil {
			return errors.New("wecom new-session command could not establish callback reply stream")
		}
	}
	committed := false
	defer func() {
		if !committed {
			i.cancelProgress(progress)
		}
	}()
	sessionKey, err := channels.BuildSessionKey(snapshot.Config.TenantID, snapshot.Config.AppCode, uuid.NewString())
	if err != nil {
		return fmt.Errorf("build new channel session key: %w", err)
	}
	payload, err := json.Marshal(map[string]any{
		"channel": string(normalized.Channel), "binding_id": bindingID,
		"app_code": snapshot.Config.AppCode, "config_version": snapshot.Config.ConfigVersion,
		"conversation_id": normalized.ConversationID, "conversation_scope": normalized.ConversationScope,
		"provider_reply_token": normalized.ProviderReplyToken, "progress_message_id": normalized.ProgressMessageID, "web_owner_id": normalized.WebOwnerID,
		"text": channels.NewSessionSuccessReply,
	})
	if err != nil {
		return fmt.Errorf("encode new session reply: %w", err)
	}
	_, err = commandStore.SwitchSession(ctx, storage.SessionSwitchRequest{
		Route: route, SessionKey: sessionKey, RequestID: normalized.MessageID,
		OutboxType: messaging.ChannelReplyEventType(normalized.Channel), OutboxPayload: payload,
	})
	if err != nil {
		return fmt.Errorf("switch channel session: %w", err)
	}
	committed = true
	return nil
}

type inboundProgressHandle struct {
	sender channels.Sender
	target channels.ReplyTarget
	id     string
}

func (i *channelIngress) startProgress(ctx context.Context, snapshot tenant.Snapshot, bindingID string, inbound *channels.InboundMessage) *inboundProgressHandle {
	if inbound == nil || inbound.Channel == channels.Web {
		return nil
	}
	sender, err := i.resolveSender(ctx, snapshot.Config.TenantID, snapshot.Config.AppCode, snapshot.Config.ConfigVersion, channels.BindingKey{Channel: inbound.Channel, BindingID: bindingID})
	if err != nil {
		slog.Warn("resolve IM progress sender", "channel", inbound.Channel, "binding_id", bindingID, "error", err)
		return nil
	}
	progress, ok := sender.(channels.ProgressSender)
	if !ok {
		return nil
	}
	target := channels.ReplyTarget{
		TenantID: snapshot.Config.TenantID, Channel: inbound.Channel, BindingID: bindingID,
		ConversationID: inbound.ConversationID, ConversationScope: inbound.ConversationScope,
		ProviderReplyToken: inbound.ProviderReplyToken,
	}
	receipt, err := progress.StartProgress(ctx, target)
	if err != nil {
		slog.Warn("start IM progress", "channel", inbound.Channel, "binding_id", bindingID, "error", err)
		return nil
	}
	inbound.ProgressMessageID = strings.TrimSpace(receipt.ExternalMessageID)
	if inbound.ProgressMessageID == "" {
		return nil
	}
	return &inboundProgressHandle{sender: sender, target: target, id: inbound.ProgressMessageID}
}

func (i *channelIngress) cancelProgress(handle *inboundProgressHandle) {
	if handle == nil || handle.sender == nil || strings.TrimSpace(handle.id) == "" {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if deleter, ok := handle.sender.(channels.MessageDeleter); ok {
		if err := deleter.DeleteMessage(cleanupCtx, handle.target, handle.id); err == nil {
			return
		}
	}
	if progress, ok := handle.sender.(channels.ProgressSender); ok {
		_ = progress.UpdateProgress(cleanupCtx, handle.target, handle.id, "请求未提交，请重试。")
	}
}
