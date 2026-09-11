package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type webFailurePublisher interface {
	PublishFailure(context.Context, string, string, string, string, string) error
}

type KafkaProcessorOption func(*KafkaProcessor)

func WithWebFailurePublisher(publisher webFailurePublisher) KafkaProcessorOption {
	return func(processor *KafkaProcessor) {
		processor.webFailures = publisher
	}
}

// KafkaProcessor converts a validated inbound Kafka envelope into one tenant
// Runtime call. A successful Process means both the Agent side effects and
// channel reply completed, allowing messaging.Worker to commit the offset.
type KafkaProcessor struct {
	runtime        *Runtime
	configurations tenant.Repository
	manifests      *messaging.ExecutionManifestCodec
	webFailures    webFailurePublisher
}

// NewKafkaProcessor constructs the worker-side bridge from Kafka to Runtime.
func NewKafkaProcessor(runtime *Runtime, configurations tenant.Repository, manifests *messaging.ExecutionManifestCodec, options ...KafkaProcessorOption) (*KafkaProcessor, error) {
	if runtime == nil {
		return nil, fmt.Errorf("Kafka processor runtime is required")
	}
	if configurations == nil {
		return nil, fmt.Errorf("Kafka processor tenant configuration repository is required")
	}
	if manifests == nil {
		return nil, fmt.Errorf("Kafka processor execution manifest verifier is required")
	}
	processor := &KafkaProcessor{runtime: runtime, configurations: configurations, manifests: manifests}
	for _, option := range options {
		if option != nil {
			option(processor)
		}
	}
	return processor, nil
}

// Process executes one ingress-version-pinned Kafka message.
func (p *KafkaProcessor) Process(ctx context.Context, envelope messaging.Envelope) error {
	if err := p.manifests.VerifyEnvelope(envelope); err != nil {
		return messaging.Permanent(fmt.Errorf("verify execution manifest: %w", err))
	}
	payload, err := messaging.DecodeInboundPayload(envelope)
	if err != nil {
		return messaging.Permanent(err)
	}
	if envelope.Manifest.AppCode != payload.AppCode || envelope.Manifest.ConfigVersion != payload.ConfigVersion {
		return messaging.Permanent(messaging.ErrManifestEnvelopeMismatch)
	}
	snapshot, err := p.configurations.GetVersion(ctx, envelope.TenantID, payload.AppCode, payload.ConfigVersion)
	if err != nil {
		wrapped := fmt.Errorf("load queued tenant configuration version: %w", err)
		if errors.Is(err, tenant.ErrNotFound) {
			return messaging.Permanent(wrapped)
		}
		return messaging.Retryable(wrapped)
	}
	runtimeContext := WithResolvedSessionKey(WithConfigurationSnapshot(ctx, snapshot), envelope.SessionKey)
	if _, err := p.runtime.Handle(runtimeContext, payload.BindingID, payload.Inbound); err != nil {
		if errors.Is(err, ErrDuplicateMessage) {
			// The first execution has already committed the durable result; a
			// crash before Kafka offset commit must therefore acknowledge replay.
			return nil
		}
		if errors.Is(err, ErrMessageInProgress) {
			return messaging.Deferred(fmt.Errorf("process queued runtime message: %w", err))
		}
		if errors.Is(err, governance.ErrTenantRateLimited) || errors.Is(err, governance.ErrConcurrentRunLimit) {
			return messaging.Deferred(fmt.Errorf("process queued runtime message: %w", err))
		}
		if isPermanentExecution(err) {
			return messaging.Permanent(fmt.Errorf("process queued runtime message: %w", err))
		}
		return messaging.Retryable(fmt.Errorf("process queued runtime message: %w", err))
	}
	return nil
}

func (p *KafkaProcessor) NotifyTerminalFailure(ctx context.Context, envelope messaging.Envelope, class string, cause error) error {
	if p == nil || p.webFailures == nil {
		return nil
	}
	payload, err := messaging.DecodeInboundPayload(envelope)
	if err != nil || payload.Inbound.Channel != channels.Web {
		return nil
	}
	tenantID := strings.TrimSpace(envelope.TenantID)
	ownerID := strings.TrimSpace(payload.Inbound.WebOwnerID)
	requestID := strings.TrimSpace(payload.Inbound.MessageID)
	if tenantID == "" || ownerID == "" || requestID == "" {
		return nil
	}
	code, message := terminalWebFailure(cause, class)
	return p.webFailures.PublishFailure(ctx, tenantID, ownerID, requestID, code, message)
}

func terminalWebFailure(cause error, class string) (string, string) {
	message := strings.ToLower(strings.TrimSpace(errorText(cause)))
	switch {
	case errors.Is(cause, context.DeadlineExceeded) || strings.Contains(message, "timed out") || strings.Contains(message, "timeout"):
		return "request_timeout", "请求处理超时，请稍后重试。"
	case errors.Is(cause, backendhealth.ErrCircuitOpen) && strings.Contains(message, "model/"):
		return "model_unavailable", "模型服务暂时不可用，请稍后重试。"
	case strings.Contains(message, "model input") || strings.Contains(message, "input capabilities") || strings.Contains(message, "does not support"):
		return "model_input_unsupported", "当前模型不支持此类输入，请更换模型或调整附件。"
	case strings.Contains(message, "model key") || strings.Contains(message, "model provider") || strings.Contains(message, "provider unavailable"):
		return "model_unavailable", "模型服务暂时不可用，请稍后重试。"
	case errors.Is(cause, ErrAgentExecutionFailed):
		return "execution_failed", "本次处理失败，请稍后重试。"
	case class == "retry_exhausted":
		return "service_unavailable", "服务暂时不可用，请稍后重试。"
	case class == "permanent":
		return "request_failed", "当前请求无法处理，请检查输入或联系管理员。"
	default:
		return "execution_failed", "请求处理失败，请稍后重试。"
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

var _ messaging.Processor = (*KafkaProcessor)(nil)
var _ messaging.TerminalFailureNotifier = (*KafkaProcessor)(nil)
