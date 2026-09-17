package audit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

type backlogSequence struct {
	values []audit.Backlog
	cancel context.CancelFunc
	index  int
}

func (s *backlogSequence) AuditBacklog(context.Context, time.Time) (audit.Backlog, error) {
	value := s.values[s.index]
	s.index++
	if s.index == len(s.values) {
		s.cancel()
	}
	return value, nil
}

type backlogRecorder struct {
	mu     sync.Mutex
	alerts []bool
}

func (r *backlogRecorder) ObserveAuditBacklog(_ audit.Backlog, alerting bool) {
	r.mu.Lock()
	r.alerts = append(r.alerts, alerting)
	r.mu.Unlock()
}

func TestBacklogMonitorReportsThresholdTransitions(t *testing.T) {
	now := time.Now().UTC()
	ctx, cancel := context.WithCancel(context.Background())
	observer := &backlogRecorder{}
	source := &backlogSequence{cancel: cancel, values: []audit.Backlog{
		{Pending: 1, OldestAge: time.Second, ObservedAt: now},
		{RetryWait: 11, OldestAge: time.Second, ObservedAt: now},
		{DeadLetter: 1, ObservedAt: now},
	}}
	var transitions []bool
	monitor := audit.BacklogMonitor{Source: source, Observer: observer, PollInterval: time.Millisecond,
		MaxOldestAge: time.Minute, MaxActive: 10, OnAlertChange: func(value bool) { transitions = append(transitions, value) }}
	if err := monitor.Run(ctx); err != context.Canceled {
		t.Fatalf("run error=%v", err)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.alerts) != 3 || observer.alerts[0] || !observer.alerts[1] || !observer.alerts[2] {
		t.Fatalf("observed alerts=%v", observer.alerts)
	}
	if len(transitions) != 2 || transitions[0] || !transitions[1] {
		t.Fatalf("transitions=%v", transitions)
	}
}
