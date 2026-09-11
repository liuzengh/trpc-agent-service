package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// KafkaProcessor converts a validated inbound Kafka envelope into one tenant
// Runtime call. A successful Process means both the Agent side effects and
// channel reply completed, allowing messaging.Worker to commit the offset.
type KafkaProcessor struct {
	runtime        *Runtime
	configurations tenant.Repository
	manifests      *messaging.ExecutionManifestCodec
}

// NewKafkaProcessor constructs the worker-side bridge from Kafka to Runtime.
func NewKafkaProcessor(runtime *Runtime, configurations tenant.Repository, manifests *messaging.ExecutionManifestCodec) (*KafkaProcessor, error) {
	if runtime == nil {
		return nil, fmt.Errorf("Kafka processor runtime is required")
	}
	if configurations == nil {
		return nil, fmt.Errorf("Kafka processor tenant configuration repository is required")
	}
	if manifests == nil {
		return nil, fmt.Errorf("Kafka processor execution manifest verifier is required")
	}
	return &KafkaProcessor{runtime: runtime, configurations: configurations, manifests: manifests}, nil
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

var _ messaging.Processor = (*KafkaProcessor)(nil)
