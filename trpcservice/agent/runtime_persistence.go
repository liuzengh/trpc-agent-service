package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/agent/trace"
)

func (r *Runtime) recordExecution(ctx context.Context, snapshot tenant.Snapshot, bindingID, sessionKey string, inbound channels.InboundMessage, reply string, artifacts []messaging.OutboundArtifactRef, usage tokenUsage, frameworkTrace *agenttrace.Trace, fencingToken uint64) (storage.OutboxEvent, error) {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	payload, err := json.Marshal(map[string]any{
		"channel": string(inbound.Channel), "binding_id": bindingID,
		"app_code": snapshot.Config.AppCode, "config_version": snapshot.Config.ConfigVersion,
		"conversation_id": inbound.ConversationID, "conversation_scope": scopeForRecord(inbound),
		"provider_reply_token": inbound.ProviderReplyToken, "progress_message_id": inbound.ProgressMessageID,
		"web_owner_id": inbound.WebOwnerID,
		"artifacts":    artifacts,
		"text":         reply, "traceparent": carrier.Get("traceparent"),
	})
	if err != nil {
		return storage.OutboxEvent{}, fmt.Errorf("encode outbox payload: %w", err)
	}
	costMicros := int64(0)
	pricedUsage := pricedTokenUsage(usage)
	if usage.known && r.costCalculator != nil {
		costMicros = r.costCalculator.CostMicros(snapshot.Config.Model, pricedUsage)
	}
	event, err := r.stateStore.RecordExecution(ctx, storage.ExecutionRecord{
		TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode, SessionKey: sessionKey,
		MessageID: inbound.MessageID, Channel: string(inbound.Channel), BindingID: bindingID,
		ConversationID: inbound.ConversationID, ConversationScope: scopeForRecord(inbound), ExternalUserID: inbound.SenderID,
		ActorExternalUserID: inbound.SenderID, ActorPlatformUserID: inbound.ActorPlatformUserID, TriggerType: string(inbound.TriggerType),
		TraceID: traceIDFor(ctx, inbound.MessageID),
		Action:  "agent_reply", Result: "queued", AuditDetail: reply, OutboxType: messaging.ChannelReplyEventType(inbound.Channel), OutboxPayload: payload, OutboxRequestID: inbound.MessageID,
		SubjectID:           inbound.SubjectID,
		OwnerPlatformUserID: inbound.OwnerPlatformUserID,
		ExecutionTrace:      projectExecutionTrace(frameworkTrace),
		FencingToken:        fencingToken,
		ModelUsage: &storage.ModelUsage{
			ProviderID: snapshot.Config.Model.ProviderID, ModelName: snapshot.Config.Model.Name,
			Known:        usage.known,
			PromptTokens: usage.promptTokens, CachedPromptTokens: pricedUsage.CachedPromptTokens,
			CompletionTokens: usage.completionTokens, TotalTokens: usage.totalTokens,
			CostMicros: costMicros,
		},
	})
	if err != nil {
		return storage.OutboxEvent{}, fmt.Errorf("record execution state: %w", err)
	}
	return event, nil
}

func pricedTokenUsage(usage tokenUsage) PricedTokenUsage {
	return PricedTokenUsage{
		PromptTokens: usage.promptTokens, CachedPromptTokens: max(0, min(usage.cachedPromptTokens, usage.promptTokens)),
		CompletionTokens: usage.completionTokens,
	}
}

func traceIDFor(ctx context.Context, fallback string) string {
	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	if traceID == "" || traceID == "00000000000000000000000000000000" {
		return fallback
	}
	return traceID
}

func scopeForRecord(inbound channels.InboundMessage) string {
	if inbound.ConversationScope == channels.ConversationGroup {
		return string(channels.ConversationGroup)
	}
	return string(channels.ConversationDirect)
}
