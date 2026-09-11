package backendhealth

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
)

type State string

const (
	StateHealthy            State = "healthy"
	StateDegraded           State = "degraded"
	StateOpen               State = "open"
	StateHalfOpen           State = "half_open"
	defaultProbeConcurrency       = 8
)

var ErrCircuitOpen = errors.New("backend circuit is open")

type Key struct {
	ProfileID string
	Domain    string
	Driver    string
}

func (k Key) normalized() (Key, error) {
	k.ProfileID = strings.TrimSpace(k.ProfileID)
	k.Domain = strings.ToLower(strings.TrimSpace(k.Domain))
	k.Driver = strings.ToLower(strings.TrimSpace(k.Driver))
	if k.ProfileID == "" || k.Domain == "" || k.Driver == "" {
		return Key{}, errors.New("backend health key requires profile, domain, and driver")
	}
	return k, nil
}

type Config struct {
	ConsecutiveFailures int
	MinimumSamples      int
	WindowSize          int
	FailureRatio        float64
	InitialOpen         time.Duration
	MaxOpen             time.Duration
	HalfOpenSuccesses   int
	HalfOpenMaxRequests int
	ProbeInterval       time.Duration
	ProbeTimeout        time.Duration
}

func DefaultConfig() Config {
	return Config{
		ConsecutiveFailures: 5,
		MinimumSamples:      10,
		WindowSize:          20,
		FailureRatio:        0.5,
		InitialOpen:         15 * time.Second,
		MaxOpen:             2 * time.Minute,
		HalfOpenSuccesses:   2,
		HalfOpenMaxRequests: 2,
		ProbeInterval:       30 * time.Second,
		ProbeTimeout:        3 * time.Second,
	}
}

func (c Config) validate() error {
	if c.ConsecutiveFailures <= 0 || c.MinimumSamples <= 0 || c.WindowSize < c.MinimumSamples ||
		c.FailureRatio <= 0 || c.FailureRatio > 1 || c.InitialOpen <= 0 || c.MaxOpen < c.InitialOpen ||
		c.HalfOpenSuccesses <= 0 || c.HalfOpenMaxRequests <= 0 || c.ProbeInterval <= 0 || c.ProbeTimeout <= 0 {
		return errors.New("invalid backend health configuration")
	}
	return nil
}

type Status struct {
	State               State
	ConsecutiveFailures int
	OpenUntil           time.Time
}

type OperationObservation struct {
	Key        Key
	Operation  string
	Duration   time.Duration
	Err        error
	FastFailed bool
}

type TransitionObservation struct {
	Key  Key
	From State
	To   State
}

type ProbeObservation struct {
	Key      Key
	Duration time.Duration
	Err      error
}

type Observer interface {
	RecordBackendOperation(context.Context, OperationObservation)
	RecordBackendTransition(context.Context, TransitionObservation)
	RecordBackendProbe(context.Context, ProbeObservation)
}

type Option func(*Registry)

func WithClock(now func() time.Time) Option {
	return func(registry *Registry) {
		if now != nil {
			registry.now = now
		}
	}
}

func WithObserver(observer Observer) Option {
	return func(registry *Registry) { registry.observer = observer }
}

type Registry struct {
	config   Config
	now      func() time.Time
	observer Observer

	mu       sync.Mutex
	circuits map[Key]*circuit
	probes   map[Key]func(context.Context) error
}

type circuit struct {
	state State

	window       []bool
	windowIndex  int
	windowCount  int
	windowFailed int
	consecutive  int

	openUntil    time.Time
	openDuration time.Duration

	halfOpenInFlight int
	halfOpenSuccess  int
}

func NewRegistry(config Config, options ...Option) (*Registry, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	registry := &Registry{
		config:   config,
		now:      time.Now,
		circuits: make(map[Key]*circuit),
		probes:   make(map[Key]func(context.Context) error),
	}
	for _, option := range options {
		if option != nil {
			option(registry)
		}
	}
	return registry, nil
}

type Permit struct {
	registry  *Registry
	key       Key
	operation string
	ctx       context.Context
	started   time.Time
	halfOpen  bool
	once      sync.Once
}

func Do(ctx context.Context, registry *Registry, key Key, operation string, fn func(context.Context) error) error {
	if registry == nil {
		return fn(ctx)
	}
	permit, err := registry.Begin(ctx, key, operation)
	if err != nil {
		return err
	}
	err = fn(ctx)
	permit.Done(err)
	return err
}

func Value[T any](ctx context.Context, registry *Registry, key Key, operation string, fn func(context.Context) (T, error)) (T, error) {
	if registry == nil {
		return fn(ctx)
	}
	permit, err := registry.Begin(ctx, key, operation)
	if err != nil {
		var zero T
		return zero, err
	}
	value, err := fn(ctx)
	permit.Done(err)
	return value, err
}

func ShouldProtect(driver string) bool {
	switch strings.ToLower(strings.TrimSpace(driver)) {
	case "", "inmemory", "sqlite":
		return false
	default:
		return true
	}
}

func (r *Registry) Begin(ctx context.Context, key Key, operation string) (*Permit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized, err := key.normalized()
	if err != nil {
		return nil, err
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		operation = "operation"
	}
	now := r.now()
	r.mu.Lock()
	c := r.circuitLocked(normalized)
	previous := c.state
	if c.state == StateOpen && !now.Before(c.openUntil) {
		c.state = StateHalfOpen
		c.halfOpenInFlight = 0
		c.halfOpenSuccess = 0
	}
	if c.state == StateOpen || (c.state == StateHalfOpen && c.halfOpenInFlight >= r.config.HalfOpenMaxRequests) {
		current := c.state
		r.mu.Unlock()
		r.observeTransition(ctx, normalized, previous, current)
		r.observeOperation(ctx, OperationObservation{Key: normalized, Operation: operation, Err: ErrCircuitOpen, FastFailed: true})
		return nil, fmt.Errorf("%w: %s/%s/%s", ErrCircuitOpen, normalized.Domain, normalized.Driver, normalized.ProfileID)
	}
	halfOpen := c.state == StateHalfOpen
	if halfOpen {
		c.halfOpenInFlight++
	}
	current := c.state
	r.mu.Unlock()
	r.observeTransition(ctx, normalized, previous, current)
	return &Permit{registry: r, key: normalized, operation: operation, ctx: ctx, started: now, halfOpen: halfOpen}, nil
}

func (p *Permit) Done(operationErr error) {
	if p == nil || p.registry == nil {
		return
	}
	p.once.Do(func() {
		p.registry.finish(p, operationErr)
	})
}

func (r *Registry) finish(permit *Permit, operationErr error) {
	now := r.now()
	r.mu.Lock()
	c := r.circuitLocked(permit.key)
	previous := c.state
	if permit.halfOpen && c.halfOpenInFlight > 0 {
		c.halfOpenInFlight--
	}
	switch classify(operationErr) {
	case outcomeSuccess:
		r.recordSuccessLocked(c, permit.halfOpen)
	case outcomeInfrastructureFailure:
		r.recordFailureLocked(c, now, permit.halfOpen)
	case outcomeIgnored:
	}
	current := c.state
	r.mu.Unlock()
	r.observeTransition(permit.ctx, permit.key, previous, current)
	r.observeOperation(permit.ctx, OperationObservation{
		Key: permit.key, Operation: permit.operation, Duration: now.Sub(permit.started), Err: operationErr,
	})
}

func (r *Registry) Status(key Key) Status {
	normalized, err := key.normalized()
	if err != nil {
		return Status{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.circuitLocked(normalized)
	return Status{State: c.state, ConsecutiveFailures: c.consecutive, OpenUntil: c.openUntil}
}

func (r *Registry) RegisterProbe(key Key, probe func(context.Context) error) error {
	normalized, err := key.normalized()
	if err != nil {
		return err
	}
	if probe == nil {
		return errors.New("backend probe is required")
	}
	r.mu.Lock()
	r.circuitLocked(normalized)
	r.probes[normalized] = probe
	r.mu.Unlock()
	return nil
}

func (r *Registry) Probe(ctx context.Context, key Key) error {
	normalized, err := key.normalized()
	if err != nil {
		return err
	}
	r.mu.Lock()
	probe := r.probes[normalized]
	r.mu.Unlock()
	if probe == nil {
		return errors.New("backend probe is not registered")
	}
	probeCtx, cancel := context.WithTimeout(ctx, r.config.ProbeTimeout)
	started := r.now()
	err = probe(probeCtx)
	cancel()
	now := r.now()

	r.mu.Lock()
	c := r.circuitLocked(normalized)
	previous := c.state
	switch classify(err) {
	case outcomeSuccess:
		if c.state == StateOpen {
			c.state = StateHalfOpen
			c.halfOpenInFlight = 0
			c.halfOpenSuccess = 0
		} else {
			r.recordSuccessLocked(c, c.state == StateHalfOpen)
		}
	case outcomeInfrastructureFailure:
		r.recordFailureLocked(c, now, c.state == StateHalfOpen)
	case outcomeIgnored:
	}
	current := c.state
	r.mu.Unlock()
	r.observeTransition(ctx, normalized, previous, current)
	if r.observer != nil {
		r.observer.RecordBackendProbe(ctx, ProbeObservation{Key: normalized, Duration: now.Sub(started), Err: err})
	}
	return err
}

// Check runs the registered probe on a request path while respecting the
// circuit state. Unlike Probe, it never bypasses an open circuit merely to
// discover recovery; the background monitor owns that responsibility.
func (r *Registry) Check(ctx context.Context, key Key) error {
	normalized, err := key.normalized()
	if err != nil {
		return err
	}
	r.mu.Lock()
	probe := r.probes[normalized]
	r.mu.Unlock()
	if probe == nil {
		return errors.New("backend probe is not registered")
	}
	permit, err := r.Begin(ctx, normalized, "health_check")
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, r.config.ProbeTimeout)
	started := r.now()
	err = probe(probeCtx)
	cancel()
	now := r.now()
	permit.Done(err)
	if r.observer != nil {
		r.observer.RecordBackendProbe(ctx, ProbeObservation{Key: normalized, Duration: now.Sub(started), Err: err})
	}
	return err
}

func (r *Registry) Run(ctx context.Context) {
	if r == nil {
		return
	}
	r.probeAll(ctx)
	ticker := time.NewTicker(r.config.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.probeAll(ctx)
		}
	}
}

func (r *Registry) probeAll(ctx context.Context) {
	r.mu.Lock()
	keys := make([]Key, 0, len(r.probes))
	for key := range r.probes {
		keys = append(keys, key)
	}
	r.mu.Unlock()
	workerCount := defaultProbeConcurrency
	if len(keys) < workerCount {
		workerCount = len(keys)
	}
	if workerCount == 0 {
		return
	}
	jobs := make(chan Key)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		safego.Go("backend health probe worker", func() {
			defer workers.Done()
			for key := range jobs {
				if ctx.Err() != nil {
					return
				}
				_ = r.Probe(ctx, key)
			}
		})
	}
	for _, key := range keys {
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return
		case jobs <- key:
		}
	}
	close(jobs)
	workers.Wait()
}

func (r *Registry) circuitLocked(key Key) *circuit {
	c := r.circuits[key]
	if c == nil {
		c = &circuit{state: StateHealthy, window: make([]bool, r.config.WindowSize), openDuration: r.config.InitialOpen}
		r.circuits[key] = c
	}
	return c
}

func (r *Registry) recordSuccessLocked(c *circuit, halfOpen bool) {
	if halfOpen || c.state == StateHalfOpen {
		c.halfOpenSuccess++
		if c.halfOpenSuccess >= r.config.HalfOpenSuccesses {
			r.closeLocked(c)
		}
		return
	}
	r.pushWindowLocked(c, false)
	c.consecutive = 0
	c.state = StateHealthy
}

func (r *Registry) recordFailureLocked(c *circuit, now time.Time, halfOpen bool) {
	if halfOpen || c.state == StateHalfOpen {
		r.openLocked(c, now)
		return
	}
	r.pushWindowLocked(c, true)
	c.consecutive++
	c.state = StateDegraded
	if c.consecutive >= r.config.ConsecutiveFailures ||
		(c.windowCount >= r.config.MinimumSamples && float64(c.windowFailed)/float64(c.windowCount) >= r.config.FailureRatio) {
		r.openLocked(c, now)
	}
}

func (r *Registry) pushWindowLocked(c *circuit, failed bool) {
	if c.windowCount == len(c.window) {
		if c.window[c.windowIndex] {
			c.windowFailed--
		}
	} else {
		c.windowCount++
	}
	c.window[c.windowIndex] = failed
	if failed {
		c.windowFailed++
	}
	c.windowIndex = (c.windowIndex + 1) % len(c.window)
}

func (r *Registry) openLocked(c *circuit, now time.Time) {
	if c.state == StateOpen || c.state == StateHalfOpen {
		c.openDuration *= 2
		if c.openDuration > r.config.MaxOpen {
			c.openDuration = r.config.MaxOpen
		}
	}
	if c.openDuration <= 0 {
		c.openDuration = r.config.InitialOpen
	}
	c.state = StateOpen
	c.openUntil = now.Add(c.openDuration)
	c.halfOpenInFlight = 0
	c.halfOpenSuccess = 0
}

func (r *Registry) closeLocked(c *circuit) {
	c.state = StateHealthy
	c.consecutive = 0
	c.windowIndex = 0
	c.windowCount = 0
	c.windowFailed = 0
	for index := range c.window {
		c.window[index] = false
	}
	c.openUntil = time.Time{}
	c.openDuration = r.config.InitialOpen
	c.halfOpenInFlight = 0
	c.halfOpenSuccess = 0
}

func (r *Registry) observeTransition(ctx context.Context, key Key, from, to State) {
	if r.observer != nil && from != to {
		r.observer.RecordBackendTransition(ctx, TransitionObservation{Key: key, From: from, To: to})
	}
}

func (r *Registry) observeOperation(ctx context.Context, observation OperationObservation) {
	if r.observer != nil {
		r.observer.RecordBackendOperation(ctx, observation)
	}
}

type outcome uint8

const (
	outcomeIgnored outcome = iota
	outcomeSuccess
	outcomeInfrastructureFailure
)

func classify(err error) outcome {
	if err == nil {
		return outcomeSuccess
	}
	if errors.Is(err, context.Canceled) {
		return outcomeIgnored
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		return outcomeInfrastructureFailure
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return outcomeInfrastructureFailure
	}
	message := strings.ToLower(err.Error())
	for _, fragment := range []string{
		"connection refused", "connection reset", "broken pipe", "i/o timeout", "timed out", "timeout",
		"no such host", "network is unreachable", "connection pool", "server closed", "unexpected eof",
		"rate limit", "too many requests", "service unavailable", "temporarily unavailable", "overloaded",
		"unauthorized", "forbidden", "authentication failed", "password authentication failed", "invalid credentials",
		"status 401", "status 403", "status code 401", "status code 403",
	} {
		if strings.Contains(message, fragment) {
			return outcomeInfrastructureFailure
		}
	}
	return outcomeIgnored
}
