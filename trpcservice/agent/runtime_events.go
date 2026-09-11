package agent

import (
	"context"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"

	agenttrace "trpc.group/trpc-go/trpc-agent-go/agent/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type runOutcome struct {
	reply string
	trace *agenttrace.Trace
	err   error
}

func collectRun(ctx context.Context, events <-chan *event.Event, onDelta func(string)) runOutcome {
	// 防御调用方传入 nil ctx（nil 会让 ctx.Done() 直接 panic）。
	// 正常调用路径应传入带取消/超时的 ctx，以便及时中断事件排空。
	if ctx == nil {
		ctx = context.Background()
	}
	if events == nil {
		return runOutcome{err: ErrAgentProducedNoReply}
	}

	var completeReply string
	var streamedReply strings.Builder
	var frameworkTrace *agenttrace.Trace
	terminalFailure := false
	drained := false
	defer func() {
		if !drained {
			drainEvents(events)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return runOutcome{trace: frameworkTrace, err: ctx.Err()}
		case agentEvent, ok := <-events:
			if !ok {
				drained = true
				return finishRun(completeReply, streamedReply.String(), frameworkTrace, terminalFailure)
			}
			if agentEvent == nil {
				continue
			}
			if agentEvent.IsTerminalError() {
				terminalFailure = true
				continue
			}
			if agentEvent.IsRunnerCompletion() {
				frameworkTrace = agentEvent.ExecutionTrace
				return finishRun(completeReply, streamedReply.String(), frameworkTrace, terminalFailure)
			}
			if agentEvent.Response == nil {
				continue
			}
			for _, choice := range agentEvent.Response.Choices {
				if content := strings.TrimSpace(choice.Message.Content); content != "" && choice.Message.Role == model.RoleAssistant {
					completeReply = content
					continue
				}
				if content := choice.Delta.Content; content != "" {
					streamedReply.WriteString(content)
					if onDelta != nil {
						onDelta(content)
					}
				}
			}
		}
	}
}

func finishRun(completeReply, streamedReply string, frameworkTrace *agenttrace.Trace, terminalFailure bool) runOutcome {
	if terminalFailure {
		return runOutcome{trace: frameworkTrace, err: ErrAgentExecutionFailed}
	}
	if reply := strings.TrimSpace(completeReply); reply != "" {
		return runOutcome{reply: reply, trace: frameworkTrace}
	}
	if reply := strings.TrimSpace(streamedReply); reply != "" {
		return runOutcome{reply: reply, trace: frameworkTrace}
	}
	return runOutcome{trace: frameworkTrace, err: ErrAgentProducedNoReply}
}

func drainEvents(events <-chan *event.Event) {
	safego.Go("agent event drain", func() {
		for range events {
		}
	})
}
