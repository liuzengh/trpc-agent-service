package audit

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Backlog struct {
	Pending, Claimed, RetryWait, DeadLetter int64
	OldestAge                               time.Duration
	ObservedAt                              time.Time
}

func (b Backlog) Active() int64 { return b.Pending + b.Claimed + b.RetryWait }

type BacklogSource interface {
	AuditBacklog(context.Context, time.Time) (Backlog, error)
}

type BacklogObserver interface {
	ObserveAuditBacklog(Backlog, bool)
}

type BacklogMonitor struct {
	Source        BacklogSource
	Observer      BacklogObserver
	PollInterval  time.Duration
	MaxOldestAge  time.Duration
	MaxActive     int64
	OnAlertChange func(alerting bool)
}

func (m BacklogMonitor) Run(ctx context.Context) error {
	if m.Source == nil || m.Observer == nil || m.PollInterval <= 0 || m.MaxOldestAge <= 0 || m.MaxActive < 1 {
		return runtime.ErrInvariantViolation
	}
	ticker := time.NewTicker(m.PollInterval)
	defer ticker.Stop()
	known, alerting := false, false
	for {
		now := time.Now().UTC()
		value, err := m.Source.AuditBacklog(ctx, now)
		if err == nil {
			next := value.DeadLetter > 0 || value.Active() >= m.MaxActive || value.OldestAge >= m.MaxOldestAge
			m.Observer.ObserveAuditBacklog(value, next)
			if (!known || next != alerting) && m.OnAlertChange != nil {
				m.OnAlertChange(next)
			}
			known, alerting = true, next
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func ValidateBacklog(value Backlog) error {
	if value.Pending < 0 || value.Claimed < 0 || value.RetryWait < 0 || value.DeadLetter < 0 || value.OldestAge < 0 || value.ObservedAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	return nil
}
