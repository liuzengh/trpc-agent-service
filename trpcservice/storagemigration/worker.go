package storagemigration

import (
	"context"
	"errors"
	"sync"
	"time"
)

// WorkerConfig bounds cross-node claims and copy batches.
type WorkerConfig struct {
	Owner        string
	PollInterval time.Duration
	ClaimTTL     time.Duration
	BatchSize    int
	MaxAttempts  int
	RetryBase    time.Duration
}

// Worker repeatedly claims one durable job and advances one checkpoint batch.
type Worker struct {
	store   Store
	copier  Copier
	config  WorkerConfig
	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func NewWorker(store Store, copier Copier, config WorkerConfig) (*Worker, error) {
	if store == nil || copier == nil || config.Owner == "" {
		return nil, errors.New("storage migration: store, copier, and owner are required")
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.ClaimTTL <= 0 {
		config.ClaimTTL = 30 * time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 8
	}
	if config.RetryBase <= 0 {
		config.RetryBase = time.Second
	}
	return &Worker{store: store, copier: copier, config: config}, nil
}

func (worker *Worker) Start(parent context.Context) error {
	if worker == nil || parent == nil {
		return errors.New("storage migration: worker and context are required")
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.started {
		return errors.New("storage migration: worker already started")
	}
	ctx, cancel := context.WithCancel(parent)
	worker.started = true
	worker.cancel = cancel
	worker.done = make(chan struct{})
	go worker.run(ctx)
	return nil
}
func (worker *Worker) run(ctx context.Context) {
	defer close(worker.done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			worker.poll(ctx)
			timer.Reset(worker.config.PollInterval)
		}
	}
}
func (worker *Worker) poll(ctx context.Context) {
	job, ok, err := worker.store.Claim(ctx, worker.config.Owner, worker.config.ClaimTTL)
	if err != nil || !ok {
		return
	}
	progress, err := worker.copier.Step(ctx, job, worker.config.BatchSize)
	updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err == nil {
		_ = worker.store.Save(updateCtx, job, progress)
		return
	}
	delay := worker.config.RetryBase
	for attempt := 1; attempt < job.Attempts && delay < time.Minute; attempt++ {
		delay *= 2
		if delay > time.Minute {
			delay = time.Minute
		}
	}
	_ = worker.store.Fail(updateCtx, job, err, time.Now().UTC().Add(delay), worker.config.MaxAttempts)
}
func (worker *Worker) Close(ctx context.Context) error {
	if worker == nil || ctx == nil {
		return errors.New("storage migration: worker and close context are required")
	}
	worker.mu.Lock()
	if !worker.started {
		worker.mu.Unlock()
		return nil
	}
	cancel, done := worker.cancel, worker.done
	worker.mu.Unlock()
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
