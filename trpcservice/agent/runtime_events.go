package agent

import (
	"context"
	"strings"

	agenttrace "trpc.group/trpc-go/trpc-agent-go/agent/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type tokenUsage struct {
	known              bool
	promptTokens       int
	cachedPromptTokens int
	completionTokens   int
	totalTokens        int
}

type runOutcome struct {
	reply string
	usage tokenUsage
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
	var usage tokenUsage
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
			return runOutcome{usage: usage, trace: frameworkTrace, err: ctx.Err()}
		case agentEvent, ok := <-events:
			if !ok {
				drained = true
				return finishRun(completeReply, streamedReply.String(), usage, frameworkTrace, terminalFailure)
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
				return finishRun(completeReply, streamedReply.String(), usage, frameworkTrace, terminalFailure)
			}
			if agentEvent.Response == nil {
				continue
			}
			if current := agentEvent.Response.Usage; current != nil {
				usage.known = true
				usage.promptTokens += current.PromptTokens
				usage.cachedPromptTokens += max(current.PromptTokensDetails.CachedTokens, current.PromptTokensDetails.CacheReadTokens)
				usage.completionTokens += current.CompletionTokens
				usage.totalTokens += current.TotalTokens
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

func finishRun(completeReply, streamedReply string, usage tokenUsage, frameworkTrace *agenttrace.Trace, terminalFailure bool) runOutcome {
	if terminalFailure {
		return runOutcome{usage: usage, trace: frameworkTrace, err: ErrAgentExecutionFailed}
	}
	if reply := strings.TrimSpace(completeReply); reply != "" {
		return runOutcome{reply: reply, usage: usage, trace: frameworkTrace}
	}
	if reply := strings.TrimSpace(streamedReply); reply != "" {
		return runOutcome{reply: reply, usage: usage, trace: frameworkTrace}
	}
	return runOutcome{usage: usage, trace: frameworkTrace, err: ErrAgentProducedNoReply}
}

func drainEvents(events <-chan *event.Event) {
	go func() {
		for range events {
		}
	}()
}
