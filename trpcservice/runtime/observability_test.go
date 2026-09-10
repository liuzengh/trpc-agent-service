package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/event"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/noop"
)

func TestTracedSessionServiceRejectsLostLeaseBeforeWrites(t *testing.T) {
	base := &sessionWriteRecorder{Service: noop.NewService()}
	service := &tracedSessionService{
		Service: base,
		exec:    testExecution(),
		leaseValidator: runtimeLeaseValidatorFunc(func(context.Context, worker.Execution, queue.Lease) error {
			return queue.ErrLeaseLost
		}),
	}
	ctx, err := worker.ContextWithJobLease(context.Background(), queue.Lease{
		Owner: "worker-a", Token: "run-a", Until: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("attach execution lease: %v", err)
	}

	if err := service.AppendEvent(ctx, &frameworksession.Session{}, &event.Event{}); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("append event error = %v, want lease lost", err)
	}
	if base.appendEventCalls != 0 {
		t.Fatalf("append event calls = %d, want 0", base.appendEventCalls)
	}

	if err := service.UpdateSessionState(ctx, frameworksession.Key{AppName: "app", UserID: "user", SessionID: "session"}, frameworksession.StateMap{}); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("update session state error = %v, want lease lost", err)
	}
	if base.updateSessionStateCalls != 0 {
		t.Fatalf("update session state calls = %d, want 0", base.updateSessionStateCalls)
	}
}

type runtimeLeaseValidatorFunc func(context.Context, worker.Execution, queue.Lease) error

func (f runtimeLeaseValidatorFunc) ValidateExecutionLease(ctx context.Context, exec worker.Execution, lease queue.Lease) error {
	return f(ctx, exec, lease)
}

type sessionWriteRecorder struct {
	frameworksession.Service
	appendEventCalls        int
	updateSessionStateCalls int
}

func (s *sessionWriteRecorder) AppendEvent(
	context.Context,
	*frameworksession.Session,
	*event.Event,
	...frameworksession.Option,
) error {
	s.appendEventCalls++
	return nil
}

func (s *sessionWriteRecorder) UpdateSessionState(
	context.Context,
	frameworksession.Key,
	frameworksession.StateMap,
) error {
	s.updateSessionStateCalls++
	return nil
}
