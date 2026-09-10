// Package queue provides a tenant-scoped durable execution queue contract.
package queue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	// ErrInvalid reports malformed queue input.
	ErrInvalid = errors.New("invalid queue task")
	// ErrNotFound reports that no task matched the requested identity or claim.
	ErrNotFound = errors.New("queue task not found")
	// ErrConflict reports a stale lease or idempotency payload mismatch.
	ErrConflict = errors.New("queue task lease conflict")
	// ErrClosed reports use of a closed queue backend.
	ErrClosed = errors.New("queue is closed")
)

// Status is the durable execution task lifecycle state.
type Status string

const (
	// StatusQueued is an enqueued task awaiting its first lease.
	StatusQueued Status = "queued"
	// StatusLeased is a task currently owned by one worker fence.
	StatusLeased Status = "leased"
	// StatusRetryable is a task waiting for a retry backoff.
	StatusRetryable Status = "retryable"
	// StatusCompleted is a task successfully handled.
	StatusCompleted Status = "completed"
	// StatusFailed is a terminal dead-letter task.
	StatusFailed Status = "failed"
)

// Task is an immutable execution payload plus its durable lease metadata.
type Task struct {
	TenantID       string
	TaskID         string
	Kind           string
	Payload        []byte
	Status         Status
	Attempts       int
	FencingToken   int64
	LeaseOwner     string
	LeaseExpiresAt *time.Time
	NextAttemptAt  time.Time
	LastErrorClass string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// TaskInput contains the identity and payload selected by a caller.
type TaskInput struct {
	TenantID string
	TaskID   string
	Kind     string
	Payload  []byte
}

// Store is the minimal durable queue surface consumed by a worker.
type Store interface {
	Enqueue(context.Context, TaskInput) (Task, bool, error)
	Get(context.Context, string, string) (Task, error)
	Claim(context.Context, string, string, time.Duration) (Task, error)
	Complete(context.Context, string, string, string, int64) (Task, error)
	Retry(context.Context, string, string, string, int64, time.Time, string) (Task, error)
	Fail(context.Context, string, string, string, int64, string) (Task, error)
	Close() error
}

// Handler executes one task. Returning RetryableError makes the worker retry
// until MaxAttempts is reached; other errors are dead-lettered immediately.
type Handler func(context.Context, Task) error

// RetryableError marks a handler failure eligible for another attempt.
type RetryableError struct{ Cause error }

func (e *RetryableError) Error() string {
	if e == nil || e.Cause == nil {
		return "retryable queue task error"
	}
	return e.Cause.Error()
}

func (e *RetryableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Retry wraps an error as retryable for a Worker.
func Retry(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Cause: err}
}

// Config configures a Worker lifecycle and retry policy.
type Config struct {
	Store         Store
	Handler       Handler
	TenantID      string
	Owner         string
	LeaseDuration time.Duration
	PollInterval  time.Duration
	MaxAttempts   int
	BackoffBase   time.Duration
	BackoffMax    time.Duration
}

// Worker owns one queue consumption loop and its handler leases.
type Worker struct {
	store         Store
	handler       Handler
	tenantID      string
	owner         string
	leaseDuration time.Duration
	pollInterval  time.Duration
	maxAttempts   int
	backoffBase   time.Duration
	backoffMax    time.Duration
	mu            sync.Mutex
	cancel        context.CancelFunc
	done          chan struct{}
	started       bool
	closed        bool
}

// New validates configuration and creates a Worker.
func New(config Config) (*Worker, error) {
	if config.Store == nil || config.Handler == nil || config.Owner == "" || config.LeaseDuration <= 0 {
		return nil, ErrInvalid
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.BackoffBase <= 0 {
		config.BackoffBase = 100 * time.Millisecond
	}
	if config.BackoffMax <= 0 {
		config.BackoffMax = 10 * time.Second
	}
	if config.BackoffMax < config.BackoffBase {
		return nil, ErrInvalid
	}
	return &Worker{store: config.Store, handler: config.Handler, tenantID: config.TenantID, owner: config.Owner, leaseDuration: config.LeaseDuration, pollInterval: config.PollInterval, maxAttempts: config.MaxAttempts, backoffBase: config.BackoffBase, backoffMax: config.BackoffMax, done: make(chan struct{})}, nil
}

// RunOnce claims and processes at most one task. It returns false when no task
// is currently eligible.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	if w == nil || ctx == nil {
		return false, ErrInvalid
	}
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		return false, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	tenantID := w.tenantID
	if tenantID == "" {
		tenantID = taskTenantHint(ctx)
	}
	task, err := w.store.Claim(ctx, tenantID, w.owner, w.leaseDuration)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	err = w.handler(ctx, cloneTask(task))
	if err == nil {
		_, completeErr := w.store.Complete(ctx, task.TenantID, task.TaskID, w.owner, task.FencingToken)
		return true, completeErr
	}
	class := errorClass(err)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isRetryable(err) {
		if task.Attempts < w.maxAttempts {
			due := time.Now().UTC().Add(w.backoff(task.Attempts))
			_, retryErr := w.store.Retry(context.Background(), task.TenantID, task.TaskID, w.owner, task.FencingToken, due, class)
			return true, retryErr
		}
	}
	_, failErr := w.store.Fail(context.Background(), task.TenantID, task.TaskID, w.owner, task.FencingToken, class)
	return true, failErr
}

// Start starts one owned run loop. Calling Start twice is an error.
func (w *Worker) Start(ctx context.Context) error {
	if w == nil || ctx == nil {
		return ErrInvalid
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.started {
		return ErrClosed
	}
	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.started = true
	go func() {
		if err := w.loop(workerCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrClosed) {
			logWorkerStopped(w, err)
		}
	}()
	return nil
}

func (w *Worker) loop(ctx context.Context) error {
	defer close(w.done)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		if _, err := w.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrClosed) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Close cancels the run loop and waits for in-flight handler completion.
func (w *Worker) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	cancel := w.cancel
	started := w.started
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if started {
		<-w.done
	}
	return nil
}

func (w *Worker) backoff(attempt int) time.Duration {
	d := w.backoffBase
	for i := 1; i < attempt; i++ {
		if d >= w.backoffMax/2 {
			return w.backoffMax
		}
		d *= 2
	}
	if d > w.backoffMax {
		return w.backoffMax
	}
	return d
}

func isRetryable(err error) bool {
	var value *RetryableError
	return errors.As(err, &value)
}

func errorClass(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "handler_error"
}

// taskTenantHint allows callers to scope a worker through context without
// storing a context in Worker. A worker normally uses a tenant-specific Store
// view; an unscoped view may use the empty hint to claim any tenant.
type tenantContextKey struct{}

// WithTenant scopes a shared Store claim to one tenant for a single call.
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantContextKey{}, tenantID)
}

func taskTenantHint(ctx context.Context) string {
	value, _ := ctx.Value(tenantContextKey{}).(string)
	return value
}

func cloneTask(value Task) Task {
	value.Payload = append([]byte(nil), value.Payload...)
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}

func validateInput(value TaskInput) error {
	if value.TenantID == "" || value.TaskID == "" || value.Kind == "" || len(value.Payload) == 0 {
		return ErrInvalid
	}
	return nil
}

// Backend owns a shared in-memory task graph used by multiple store views.
type Backend struct {
	mu     sync.Mutex
	tasks  map[string]Task
	refs   int
	closed bool
}

// NewBackend creates an isolated shared in-memory task graph.
func NewBackend() *Backend { return &Backend{tasks: map[string]Task{}, refs: 1} }

// MemoryStore is a concurrency-safe queue.Store implementation.
type MemoryStore struct {
	backend   *Backend
	closeOnce sync.Once
}

// NewMemory creates an in-memory queue store.
func NewMemory() *MemoryStore { return &MemoryStore{backend: NewBackend()} }

// NewMemoryWithBackend creates a store view over a shared backend.
func NewMemoryWithBackend(backend *Backend) *MemoryStore {
	if backend == nil {
		backend = NewBackend()
	} else {
		backend.mu.Lock()
		if !backend.closed {
			backend.refs++
		}
		backend.mu.Unlock()
	}
	return &MemoryStore{backend: backend}
}

// Enqueue durably records one idempotent task.
func (s *MemoryStore) Enqueue(ctx context.Context, input TaskInput) (Task, bool, error) {
	if err := contextErr(ctx); err != nil {
		return Task{}, false, err
	}
	if err := validateInput(input); err != nil {
		return Task{}, false, err
	}
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	if s.backend.closed {
		return Task{}, false, ErrClosed
	}
	k := input.TenantID + "\x00" + input.TaskID
	if existing, ok := s.backend.tasks[k]; ok {
		if existing.Kind != input.Kind || string(existing.Payload) != string(input.Payload) {
			return Task{}, false, ErrConflict
		}
		return cloneTask(existing), true, nil
	}
	now := time.Now().UTC()
	task := Task{TenantID: input.TenantID, TaskID: input.TaskID, Kind: input.Kind, Payload: append([]byte(nil), input.Payload...), Status: StatusQueued, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
	s.backend.tasks[k] = task
	return cloneTask(task), false, nil
}

// Get reads one tenant-scoped task snapshot.
func (s *MemoryStore) Get(ctx context.Context, tenantID, taskID string) (Task, error) {
	if err := contextErr(ctx); err != nil {
		return Task{}, err
	}
	if tenantID == "" || taskID == "" {
		return Task{}, ErrInvalid
	}
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	task, ok := s.backend.tasks[tenantID+"\x00"+taskID]
	if !ok {
		return Task{}, ErrNotFound
	}
	return cloneTask(task), nil
}

// Claim atomically leases the oldest eligible task and advances its fence.
func (s *MemoryStore) Claim(ctx context.Context, tenantID, owner string, leaseDuration time.Duration) (Task, error) {
	if err := contextErr(ctx); err != nil {
		return Task{}, err
	}
	if owner == "" || leaseDuration <= 0 {
		return Task{}, ErrInvalid
	}
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	if s.backend.closed {
		return Task{}, ErrClosed
	}
	now := time.Now().UTC()
	keys := make([]string, 0, len(s.backend.tasks))
	for key, task := range s.backend.tasks {
		if tenantID != "" && task.TenantID != tenantID {
			continue
		}
		eligible := (task.Status == StatusQueued || task.Status == StatusRetryable) && !task.NextAttemptAt.After(now)
		if task.Status == StatusLeased && task.LeaseExpiresAt != nil && !task.LeaseExpiresAt.After(now) {
			eligible = true
		}
		if eligible {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return Task{}, ErrNotFound
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := s.backend.tasks[keys[i]], s.backend.tasks[keys[j]]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return keys[i] < keys[j]
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	task := s.backend.tasks[keys[0]]
	expires := now.Add(leaseDuration)
	task.Status, task.Attempts, task.FencingToken = StatusLeased, task.Attempts+1, task.FencingToken+1
	task.LeaseOwner, task.LeaseExpiresAt, task.UpdatedAt = owner, &expires, now
	s.backend.tasks[keys[0]] = task
	return cloneTask(task), nil
}

// Complete commits a successful task under the current lease fence.
func (s *MemoryStore) Complete(ctx context.Context, tenantID, taskID, owner string, fence int64) (Task, error) {
	return s.transition(ctx, tenantID, taskID, owner, fence, StatusCompleted, "", time.Time{})
}

// Retry returns a leased task to the retryable state.
func (s *MemoryStore) Retry(ctx context.Context, tenantID, taskID, owner string, fence int64, next time.Time, class string) (Task, error) {
	return s.transition(ctx, tenantID, taskID, owner, fence, StatusRetryable, class, next)
}

// Fail dead-letters a leased task under the current fence.
func (s *MemoryStore) Fail(ctx context.Context, tenantID, taskID, owner string, fence int64, class string) (Task, error) {
	return s.transition(ctx, tenantID, taskID, owner, fence, StatusFailed, class, time.Time{})
}

func (s *MemoryStore) transition(ctx context.Context, tenantID, taskID, owner string, fence int64, status Status, class string, next time.Time) (Task, error) {
	if err := contextErr(ctx); err != nil {
		return Task{}, err
	}
	if tenantID == "" || taskID == "" || owner == "" || fence <= 0 {
		return Task{}, ErrInvalid
	}
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	task, ok := s.backend.tasks[tenantID+"\x00"+taskID]
	if !ok {
		return Task{}, ErrNotFound
	}
	if task.Status != StatusLeased || task.LeaseOwner != owner || task.FencingToken != fence || (task.LeaseExpiresAt != nil && !task.LeaseExpiresAt.After(time.Now().UTC())) {
		return Task{}, ErrConflict
	}
	task.Status, task.LastErrorClass, task.UpdatedAt = status, class, time.Now().UTC()
	task.NextAttemptAt, task.LeaseOwner, task.LeaseExpiresAt = next, "", nil
	s.backend.tasks[tenantID+"\x00"+taskID] = task
	return cloneTask(task), nil
}

// Close releases this store view; shared state closes after the final view.
func (s *MemoryStore) Close() error {
	if s == nil || s.backend == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.backend.mu.Lock()
		if s.backend.refs > 0 {
			s.backend.refs--
		}
		if s.backend.refs == 0 {
			s.backend.closed = true
		}
		s.backend.mu.Unlock()
	})
	return nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}

var _ Store = (*MemoryStore)(nil)

// FormatTaskKey is useful to adapters that need a stable composite key.
func FormatTaskKey(tenantID, taskID string) string { return fmt.Sprintf("%s:%s", tenantID, taskID) }
