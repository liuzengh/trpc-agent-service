package summary

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	DefaultPollInterval = 250 * time.Millisecond
	DefaultJobTimeout   = 2 * time.Minute
	MaxPollConcurrency  = 128
)

type PollObserver func(workerID string, job Job, elapsed time.Duration, err error)

type PollerConfig struct {
	OwnerPrefix    string
	Concurrency    int
	PollInterval   time.Duration
	LeaseTTL       time.Duration
	JobTimeout     time.Duration
	RetryBackoff   func(int) time.Duration
	Observe        PollObserver
	Store          Store
	Sink           Sink
	Generator      Generator
	TargetResolver TargetResolver
}

// Poller runs a fixed number of Summary processors. Every slot owns a stable
// lease identity, and cancellation waits for all active Processor goroutines
// to stop before returning.
type Poller struct {
	config PollerConfig
}

func NewPoller(config PollerConfig) (*Poller, error) {
	if config.Concurrency == 0 {
		config.Concurrency = 1
	}
	if config.PollInterval == 0 {
		config.PollInterval = DefaultPollInterval
	}
	if config.JobTimeout == 0 {
		config.JobTimeout = DefaultJobTimeout
	}
	if config.OwnerPrefix == "" || len(config.OwnerPrefix) > 120 || !utf8.ValidString(config.OwnerPrefix) ||
		config.Concurrency < 1 || config.Concurrency > MaxPollConcurrency ||
		config.PollInterval < time.Millisecond || config.PollInterval > time.Minute ||
		config.JobTimeout < time.Second || config.JobTimeout > 30*time.Minute ||
		config.Store == nil || config.Sink == nil || config.Generator == nil {
		return nil, fmt.Errorf("%w: invalid Summary poller configuration", ErrStoreUnavailable)
	}
	if err := validateLeaseRequest(config.OwnerPrefix+"-127", config.LeaseTTL); err != nil {
		return nil, err
	}
	return &Poller{config: config}, nil
}

func (p *Poller) Run(ctx context.Context) error {
	if p == nil {
		return ErrStoreUnavailable
	}
	ctx = nonNilContext(ctx)
	var wg sync.WaitGroup
	for slot := 0; slot < p.config.Concurrency; slot++ {
		workerID := fmt.Sprintf("%s-%d", p.config.OwnerPrefix, slot)
		processor := NewProcessor(p.config.Store, p.config.Sink, p.config.Generator, workerID, p.config.LeaseTTL)
		processor.TargetResolver = p.config.TargetResolver
		processor.RetryBackoff = p.config.RetryBackoff
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx, workerID, processor)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (p *Poller) loop(ctx context.Context, workerID string, processor *Processor) {
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		// Parent cancellation closes admission, but a job already protected by a
		// durable lease must either finish or record a bounded failure. Detaching
		// cancellation here gives that active job its configured drain window;
		// context values (including trace identity) are retained.
		jobCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.config.JobTimeout)
		job, err := processor.RunOnce(jobCtx)
		cancel()
		if !errors.Is(err, ErrNoWork) && p.config.Observe != nil {
			p.config.Observe(workerID, job, time.Since(started), err)
		}
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			continue
		}
		if !waitPoll(ctx, p.config.PollInterval) {
			return
		}
	}
}

func waitPoll(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
