// Package worker consumes durable Agent tasks and records their outcomes.
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

// Runtime is the tenant-scoped Agent execution boundary used by a Worker.
type Runtime interface {
	ChatWithScope(ctx context.Context, input agentruntime.ChatInput) (agentruntime.ChatResult, error)
}

type Options struct {
	WorkerID    string
	MaxAttempts int
	RetryDelay  time.Duration
}

// Worker processes at-least-once queue deliveries. Durable idempotency makes
// reprocessing safe after completion-before-ack crashes.
type Worker struct {
	queue   workqueue.Queue
	journal gateway.Journal
	runtime Runtime
	opts    Options
}

func New(
	queue workqueue.Queue,
	journal gateway.Journal,
	runtime Runtime,
	opts Options,
) (*Worker, error) {
	if queue == nil || journal == nil || runtime == nil {
		return nil, fmt.Errorf("Worker queue, journal and runtime are required")
	}
	if opts.WorkerID == "" || opts.MaxAttempts <= 0 || opts.RetryDelay <= 0 {
		return nil, fmt.Errorf("Worker options are invalid")
	}
	return &Worker{queue: queue, journal: journal, runtime: runtime, opts: opts}, nil
}

// ProcessOne receives and handles a single delivery.
func (w *Worker) ProcessOne(ctx context.Context) (bool, error) {
	delivery, err := w.queue.Receive(ctx)
	if err != nil {
		if errors.Is(err, workqueue.ErrNoMessage) {
			return false, nil
		}
		return false, err
	}
	task := delivery.Task()
	if err := task.Scope.Validate(); err != nil {
		_ = delivery.Ack(ctx)
		return true, fmt.Errorf("reject invalid task scope: %w", err)
	}
	if err := w.journal.MarkRunRunning(ctx, task.RequestID, w.opts.WorkerID); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	result, runErr := w.runtime.ChatWithScope(ctx, agentruntime.ChatInput{
		Scope:     task.Scope,
		MessageID: task.MessageID,
		UserID:    task.UserID,
		SessionID: task.SessionID,
		Text:      task.Text,
		RequestID: task.RequestID,
	})
	if runErr != nil {
		failErr := w.journal.FailRun(ctx, task.RequestID, "agent_execution", runErr)
		return true, w.retryOrAck(ctx, delivery, task, errors.Join(runErr, failErr))
	}
	if err := w.journal.CompleteRun(ctx, task, gateway.RunResult{
		Reply:        result.Reply,
		AgentName:    result.AgentName,
		FencingToken: result.FencingToken,
		EventCount:   result.EventCount,
	}); err != nil {
		return true, w.retryOrAck(ctx, delivery, task, err)
	}
	if err := delivery.Ack(ctx); err != nil {
		return true, err
	}
	return true, nil
}

func (w *Worker) retryOrAck(
	ctx context.Context,
	delivery workqueue.Delivery,
	task workqueue.AgentTask,
	cause error,
) error {
	if task.Attempt+1 < w.opts.MaxAttempts {
		if w.opts.RetryDelay > 0 {
			timer := time.NewTimer(w.opts.RetryDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return errors.Join(cause, context.Cause(ctx))
			}
		}
		return errors.Join(cause, delivery.Retry(ctx))
	}
	return errors.Join(cause, delivery.Ack(ctx))
}

// Run keeps consuming until cancellation. Individual task errors do not stop
// the worker because their run state and retry decision are already durable.
func (w *Worker) Run(ctx context.Context) error {
	for {
		_, err := w.ProcessOne(ctx)
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err != nil {
			continue
		}
	}
}
