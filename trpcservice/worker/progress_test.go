package worker

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestProgressEmitterEmitsOrderedEphemeralUpdates(t *testing.T) {
	var got []ProgressEvent
	emitter := progressEmitter{publisher: ProgressPublisherFunc(func(value ProgressEvent) { got = append(got, value) }),
		envelope: runtime.ExecutionEnvelope{TenantID: "tenant", RequestID: "request", TraceParent: "trace"}}
	emitter.publish(ProgressRunStarted, "")
	emitter.publish(ProgressMessageDelta, "hello")
	if len(got) != 2 || got[0].Kind != ProgressRunStarted || got[0].Sequence != 1 || got[1].Kind != ProgressMessageDelta || got[1].Sequence != 2 || got[1].Content != "hello" || got[1].TraceParent != "trace" {
		t.Fatalf("events=%#v", got)
	}
}

func TestConsumeRunnerEventsPublishesOnlyDeltaContent(t *testing.T) {
	events := make(chan *event.Event, 2)
	events <- &event.Event{Response: &model.Response{Choices: []model.Choice{{Delta: model.Message{Content: "hel"}}}}}
	events <- event.NewResponseEvent("runner", "done", &model.Response{Done: true, Object: model.ObjectTypeRunnerCompletion})
	close(events)
	var deltas []string
	result, err := consumeRunnerEventsWithProgress(context.Background(), events, func(delta string) { deltas = append(deltas, delta) })
	if err != nil || result.Content != "hel" || len(deltas) != 1 || deltas[0] != "hel" {
		t.Fatalf("result=%#v deltas=%#v err=%v", result, deltas, err)
	}
}

func TestProgressAllowedRespectsOutputDLPAndRenderer(t *testing.T) {
	publisher := ProgressPublisherFunc(func(ProgressEvent) {})
	permit := governance.RunPermit{Policy: governance.PolicySnapshot{Policy: governance.PolicyV1{OutputDLP: governance.DLPDisabled}}}
	if !progressAllowed(RunnerExecutor{Progress: publisher}, permit) {
		t.Fatal("expected progress when no governance guard is installed")
	}
	guard := governance.Service{}
	if !progressAllowed(RunnerExecutor{Progress: publisher, Governance: guard}, permit) {
		t.Fatal("expected progress with disabled output DLP")
	}
	permit.Policy.Policy.OutputDLP = governance.DLPRequired
	if progressAllowed(RunnerExecutor{Progress: publisher, Governance: guard}, permit) {
		t.Fatal("progress must be disabled until output DLP has a streaming-safe scanner")
	}
	if progressAllowed(RunnerExecutor{Progress: publisher, OutputRenderer: OutboundRendererFunc(func(context.Context, runtime.ExecutionEnvelope, string) (OutboundContent, error) {
		return OutboundContent{}, nil
	})}, governance.RunPermit{}) {
		t.Fatal("progress must be disabled for a transforming renderer")
	}
}
