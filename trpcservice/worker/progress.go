package worker

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	sharedprogress "github.com/liuzengh/trpc-agent-service/trpcservice/progress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const (
	ProgressRunStarted   = sharedprogress.RunStarted
	ProgressMessageDelta = sharedprogress.MessageDelta
)

type ProgressEvent = sharedprogress.Event
type ProgressPublisher = sharedprogress.Publisher
type ProgressPublisherFunc = sharedprogress.PublisherFunc

type progressEmitter struct {
	publisher ProgressPublisher
	envelope  runtime.ExecutionEnvelope
	sequence  uint64
}

func (p *progressEmitter) publish(kind, content string) {
	if p == nil || p.publisher == nil {
		return
	}
	p.sequence++
	p.publisher.TryPublish(ProgressEvent{SchemaVersion: 1, TenantID: p.envelope.TenantID, RequestID: p.envelope.RequestID,
		Kind: kind, Sequence: p.sequence, Content: content, TraceParent: p.envelope.TraceParent})
}

// progressAllowed prevents the unsafe appearance of an answer before the
// terminal output-DLP decision. A custom renderer may transform the model
// text into a card or binary payload, so it too intentionally uses terminal
// delivery only.
func progressAllowed(executor RunnerExecutor, permit governance.RunPermit) bool {
	if executor.Progress == nil || executor.OutputRenderer != nil {
		return false
	}
	return executor.Governance == nil || permit.Policy.Policy.OutputDLP == governance.DLPDisabled
}
