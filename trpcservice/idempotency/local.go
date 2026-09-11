package idempotency

import (
	"context"
	"errors"
	"sync"
	"time"
)

const defaultLocalCompletedTTL = 24 * time.Hour

type localRecord struct {
	fingerprint string
	owner       uint64
	processing  bool
	result      Result
	done        chan struct{}
	expiresAt   time.Time
}

// LocalStore deduplicates messages within one service process.
type LocalStore struct {
	mu           sync.Mutex
	records      map[Key]*localRecord
	nextOwner    uint64
	closed       bool
	completedTTL time.Duration
	closeCtx     context.Context
	closeCancel  context.CancelCauseFunc
}

// NewLocalStore creates an in-process idempotency store.
func NewLocalStore() *LocalStore {
	return NewLocalStoreWithCompletedTTL(defaultLocalCompletedTTL)
}

// NewLocalStoreWithCompletedTTL creates a local store with bounded completed
// result retention. Processing records live until their attempt finishes or
// the process exits.
func NewLocalStoreWithCompletedTTL(completedTTL time.Duration) *LocalStore {
	if completedTTL <= 0 {
		completedTTL = defaultLocalCompletedTTL
	}
	closeCtx, closeCancel := context.WithCancelCause(context.Background())
	return &LocalStore{
		records:      make(map[Key]*localRecord),
		completedTTL: completedTTL,
		closeCtx:     closeCtx,
		closeCancel:  closeCancel,
	}
}

// Begin creates one processing record or reports the existing state.
func (s *LocalStore) Begin(
	ctx context.Context,
	key Key,
	fingerprint string,
) (BeginResult, error) {
	if err := validateBeginInput(ctx, key, fingerprint); err != nil {
		return BeginResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return BeginResult{}, ErrStoreClosed
	}
	if record := s.records[key]; record != nil {
		if !record.processing && time.Now().After(record.expiresAt) {
			delete(s.records, key)
		} else {
			if record.fingerprint != fingerprint {
				return BeginResult{}, ErrKeyConflict
			}
			if record.processing {
				return BeginResult{Status: BeginProcessing}, nil
			}
			return BeginResult{Status: BeginCompleted, Result: record.result}, nil
		}
	}

	s.nextOwner++
	record := &localRecord{
		fingerprint: fingerprint,
		owner:       s.nextOwner,
		processing:  true,
		done:        make(chan struct{}),
	}
	s.records[key] = record
	attemptCtx, attemptCancel := context.WithCancelCause(ctx)
	stopCloseCallback := context.AfterFunc(s.closeCtx, func() {
		attemptCancel(ErrStoreClosed)
	})
	return BeginResult{
		Status: BeginStarted,
		Attempt: &localAttempt{
			ctx:               attemptCtx,
			cancel:            attemptCancel,
			stopCloseCallback: stopCloseCallback,
			store:             s,
			key:               key,
			owner:             record.owner,
		},
	}, nil
}

// Wait blocks until the current owner completes, fails or the caller cancels.
func (s *LocalStore) Wait(
	ctx context.Context,
	key Key,
	fingerprint string,
) (Result, error) {
	if err := validateBeginInput(ctx, key, fingerprint); err != nil {
		return Result{}, err
	}
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return Result{}, ErrStoreClosed
		}
		record := s.records[key]
		if record == nil {
			s.mu.Unlock()
			return Result{}, ErrRetry
		}
		if record.fingerprint != fingerprint {
			s.mu.Unlock()
			return Result{}, ErrKeyConflict
		}
		if !record.processing {
			result := record.result
			s.mu.Unlock()
			return result, nil
		}
		done := record.done
		s.mu.Unlock()

		select {
		case <-done:
			continue
		case <-ctx.Done():
			return Result{}, context.Cause(ctx)
		case <-s.closeCtx.Done():
			return Result{}, ErrStoreClosed
		}
	}
}

// Ready reports whether the store is open.
func (s *LocalStore) Ready(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	return nil
}

// Close rejects new messages and cancels active attempt contexts.
func (s *LocalStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.closeCancel(ErrStoreClosed)
	return nil
}

type localAttempt struct {
	ctx               context.Context
	cancel            context.CancelCauseFunc
	stopCloseCallback func() bool
	store             *LocalStore
	key               Key
	owner             uint64
	once              sync.Once
	resultErr         error
}

func (a *localAttempt) Context() context.Context {
	return a.ctx
}

func (a *localAttempt) Complete(ctx context.Context, result Result) error {
	return a.finish(ctx, &result)
}

func (a *localAttempt) Fail(ctx context.Context) error {
	return a.finish(ctx, nil)
}

func (a *localAttempt) finish(ctx context.Context, result *Result) error {
	if a == nil {
		return nil
	}
	a.once.Do(func() {
		if a.stopCloseCallback != nil {
			a.stopCloseCallback()
		}
		a.store.mu.Lock()
		record := a.store.records[a.key]
		if record == nil || !record.processing || record.owner != a.owner {
			a.resultErr = ErrAttemptLost
		} else if result == nil {
			delete(a.store.records, a.key)
			close(record.done)
		} else {
			record.processing = false
			record.result = *result
			record.expiresAt = time.Now().Add(a.store.completedTTL)
			close(record.done)
		}
		a.store.mu.Unlock()
		a.cancel(a.resultErr)
	})
	return a.resultErr
}

func validateBeginInput(ctx context.Context, key Key, fingerprint string) error {
	if ctx == nil {
		return errors.New("idempotency context is required")
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if fingerprint == "" {
		return errors.New("idempotency fingerprint is required")
	}
	return nil
}

var _ Store = (*LocalStore)(nil)
var _ Attempt = (*localAttempt)(nil)
