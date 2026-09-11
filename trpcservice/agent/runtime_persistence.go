package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/modelusage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/agent/trace"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func (r *Runtime) recordExecution(ctx context.Context, snapshot tenant.Snapshot, bindingID, sessionKey string, inbound channels.InboundMessage, reply string, card *channels.InteractiveCard, artifacts []messaging.OutboundArtifactRef, modelUsage storage.ModelUsage, frameworkTrace *agenttrace.Trace, fencingToken uint64) (storage.OutboxEvent, error) {
	return r.recordReplyExecution(
		ctx, snapshot, bindingID, sessionKey, inbound, reply, card, artifacts,
		&modelUsage, frameworkTrace, fencingToken, "agent_reply", "queued",
	)
}

func (r *Runtime) recordGovernanceReply(ctx context.Context, snapshot tenant.Snapshot, bindingID, sessionKey string, inbound channels.InboundMessage, reply, reason string, fencingToken uint64) (storage.OutboxEvent, error) {
	return r.recordReplyExecution(
		ctx, snapshot, bindingID, sessionKey, inbound, reply, nil, nil,
		nil, nil, fencingToken, "governance_rejection", reason,
	)
}

func (r *Runtime) recordReplyExecution(ctx context.Context, snapshot tenant.Snapshot, bindingID, sessionKey string, inbound channels.InboundMessage, reply string, card *channels.InteractiveCard, artifacts []messaging.OutboundArtifactRef, modelUsage *storage.ModelUsage, frameworkTrace *agenttrace.Trace, fencingToken uint64, action, result string) (storage.OutboxEvent, error) {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	payload, err := json.Marshal(map[string]any{
		"channel": string(inbound.Channel), "binding_id": bindingID,
		"app_code": snapshot.Config.AppCode, "config_version": snapshot.Config.ConfigVersion,
		"conversation_id": inbound.ConversationID, "conversation_scope": scopeForRecord(inbound),
		"provider_reply_token": inbound.ProviderReplyToken, "progress_message_id": inbound.ProgressMessageID,
		"web_owner_id": inbound.WebOwnerID,
		"artifacts":    artifacts,
		"text":         reply, "card": card, "traceparent": carrier.Get("traceparent"),
	})
	if err != nil {
		return storage.OutboxEvent{}, fmt.Errorf("encode outbox payload: %w", err)
	}
	event, err := r.stateStore.RecordExecution(ctx, storage.ExecutionRecord{
		TenantID: snapshot.Config.TenantID, AppCode: snapshot.Config.AppCode, SessionKey: sessionKey,
		MessageID: inbound.MessageID, Channel: string(inbound.Channel), BindingID: bindingID,
		ConversationID: inbound.ConversationID, ConversationScope: scopeForRecord(inbound), ExternalUserID: inbound.SenderID,
		ActorExternalUserID: inbound.SenderID, ActorPlatformUserID: inbound.ActorPlatformUserID, TriggerType: string(inbound.TriggerType),
		TraceID: traceIDFor(ctx, inbound.MessageID),
		Action:  action, Result: result, AuditDetail: reply, OutboxType: messaging.ChannelReplyEventType(inbound.Channel), OutboxPayload: payload, OutboxRequestID: inbound.MessageID,
		SubjectID:           inbound.SubjectID,
		OwnerPlatformUserID: inbound.OwnerPlatformUserID,
		ExecutionTrace:      projectExecutionTrace(frameworkTrace),
		FencingToken:        fencingToken,
		ModelUsage:          modelUsage,
	})
	if err != nil {
		return storage.OutboxEvent{}, fmt.Errorf("record execution state: %w", err)
	}
	return event, nil
}

func (r *Runtime) priceModelUsage(primary config.ModelConfig, frameworkTrace *agenttrace.Trace, actual []modelusage.Segment) storage.ModelUsage {
	if len(actual) == 0 {
		var reported *model.Usage
		if frameworkTrace != nil {
			reported = frameworkTrace.Usage
		}
		priced := pricedModelUsage(reported)
		costMicros := int64(0)
		known := reported != nil
		if known && r.costCalculator != nil {
			costMicros = r.costCalculator.CostMicros(primary, priced)
		}
		usage := storage.ModelUsage{
			ProviderID: primary.ProviderID, ModelName: primary.Name, Known: known,
			CachedPromptTokens: priced.CachedPromptTokens, CostMicros: costMicros,
		}
		if reported != nil {
			usage.PromptTokens = reported.PromptTokens
			usage.CompletionTokens = reported.CompletionTokens
			usage.TotalTokens = reported.TotalTokens
			usage.Breakdown = []storage.ModelUsageSegment{{
				ProviderID: usage.ProviderID, ModelName: usage.ModelName,
				PromptTokens: usage.PromptTokens, CachedPromptTokens: usage.CachedPromptTokens,
				CompletionTokens: usage.CompletionTokens, TotalTokens: usage.TotalTokens, CostMicros: usage.CostMicros,
			}}
		}
		return usage
	}

	usage := storage.ModelUsage{Known: true}
	uniformProvider, uniformModel := actual[0].ProviderID, actual[0].ModelName
	for _, segment := range actual {
		cached := max(0, min(segment.CachedPromptTokens, segment.PromptTokens))
		costMicros := int64(0)
		if r.costCalculator != nil {
			costMicros = r.costCalculator.CostMicros(config.ModelConfig{ProviderID: segment.ProviderID, Name: segment.ModelName}, PricedTokenUsage{
				PromptTokens: segment.PromptTokens, CachedPromptTokens: cached, CompletionTokens: segment.CompletionTokens,
			})
		}
		usage.PromptTokens += segment.PromptTokens
		usage.CachedPromptTokens += cached
		usage.CompletionTokens += segment.CompletionTokens
		usage.TotalTokens += segment.TotalTokens
		usage.CostMicros = addCosts(usage.CostMicros, costMicros)
		usage.Breakdown = append(usage.Breakdown, storage.ModelUsageSegment{
			ProviderID: segment.ProviderID, ModelName: segment.ModelName, ReportedModel: segment.ReportedModel,
			PromptTokens: segment.PromptTokens, CachedPromptTokens: cached,
			CompletionTokens: segment.CompletionTokens, TotalTokens: segment.TotalTokens, CostMicros: costMicros,
		})
		if segment.ProviderID != uniformProvider || segment.ModelName != uniformModel {
			uniformProvider, uniformModel = "mixed", "mixed"
		}
	}
	usage.ProviderID, usage.ModelName = uniformProvider, uniformModel
	return usage
}

func pricedModelUsage(usage *model.Usage) PricedTokenUsage {
	if usage == nil {
		return PricedTokenUsage{}
	}
	cached := max(usage.PromptTokensDetails.CachedTokens, usage.PromptTokensDetails.CacheReadTokens)
	return PricedTokenUsage{
		PromptTokens: usage.PromptTokens, CachedPromptTokens: max(0, min(cached, usage.PromptTokens)),
		CompletionTokens: usage.CompletionTokens,
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
