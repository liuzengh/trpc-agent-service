package backendhealth

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistryOpensFastAndRecoversThroughHalfOpen(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	registry, err := NewRegistry(Config{
		ConsecutiveFailures: 2,
		MinimumSamples:      2,
		WindowSize:          4,
		FailureRatio:        0.5,
		InitialOpen:         10 * time.Second,
		MaxOpen:             time.Minute,
		HalfOpenSuccesses:   2,
		HalfOpenMaxRequests: 1,
		ProbeInterval:       time.Minute,
		ProbeTimeout:        time.Second,
	}, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	key := Key{ProfileID: "tenant-redis", Domain: "session", Driver: "redis"}
	backendErr := &net.DNSError{IsTimeout: true, Err: "timeout", Name: "redis.internal"}

	for range 2 {
		permit, err := registry.Begin(context.Background(), key, "get_session")
		if err != nil {
			t.Fatalf("Begin() before threshold error = %v", err)
		}
		permit.Done(backendErr)
	}
	if status := registry.Status(key); status.State != StateOpen {
		t.Fatalf("state = %q, want open", status.State)
	}
	if _, err := registry.Begin(context.Background(), key, "get_session"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Begin() while open error = %v, want ErrCircuitOpen", err)
	}

	now = now.Add(10 * time.Second)
	permit, err := registry.Begin(context.Background(), key, "get_session")
	if err != nil {
		t.Fatalf("Begin() after cooldown error = %v", err)
	}
	if status := registry.Status(key); status.State != StateHalfOpen {
		t.Fatalf("state after cooldown = %q, want half_open", status.State)
	}
	if _, err := registry.Begin(context.Background(), key, "get_session"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second half-open request error = %v, want ErrCircuitOpen", err)
	}
	permit.Done(nil)

	permit, err = registry.Begin(context.Background(), key, "get_session")
	if err != nil {
		t.Fatal(err)
	}
	permit.Done(nil)
	if status := registry.Status(key); status.State != StateHealthy {
		t.Fatalf("state = %q, want healthy", status.State)
	}
}

func TestRegistryIgnoresCallerCancellationAndValidationErrors(t *testing.T) {
	registry, err := NewRegistry(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	key := Key{ProfileID: "memory", Domain: "memory", Driver: "postgres"}
	for _, operationErr := range []error{context.Canceled, errors.New("memory key is required")} {
		permit, beginErr := registry.Begin(context.Background(), key, "read")
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		permit.Done(operationErr)
	}
	status := registry.Status(key)
	if status.State != StateHealthy || status.ConsecutiveFailures != 0 {
		t.Fatalf("status after non-infrastructure errors = %+v", status)
	}
}

func TestRegistryTreatsCapacityAndAvailabilityErrorsAsInfrastructureFailures(t *testing.T) {
	registry, err := NewRegistry(Config{
		ConsecutiveFailures: 1,
		MinimumSamples:      1,
		WindowSize:          1,
		FailureRatio:        1,
		InitialOpen:         time.Minute,
		MaxOpen:             time.Minute,
		HalfOpenSuccesses:   1,
		HalfOpenMaxRequests: 1,
		ProbeInterval:       time.Minute,
		ProbeTimeout:        time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{ProfileID: "model-primary", Domain: "model", Driver: "openai"}
	permit, err := registry.Begin(context.Background(), key, "generate_content")
	if err != nil {
		t.Fatal(err)
	}
	permit.Done(errors.New("service unavailable: rate limit exceeded"))
	if status := registry.Status(key); status.State != StateOpen {
		t.Fatalf("state = %q, want open", status.State)
	}
}

func TestRegistryProbeMovesOpenCircuitToHalfOpen(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	registry, err := NewRegistry(Config{
		ConsecutiveFailures: 1,
		MinimumSamples:      1,
		WindowSize:          2,
		FailureRatio:        1,
		InitialOpen:         time.Hour,
		MaxOpen:             time.Hour,
		HalfOpenSuccesses:   1,
		HalfOpenMaxRequests: 1,
		ProbeInterval:       time.Minute,
		ProbeTimeout:        time.Second,
	}, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	key := Key{ProfileID: "artifact-s3", Domain: "artifact", Driver: "s3"}
	permit, err := registry.Begin(context.Background(), key, "load")
	if err != nil {
		t.Fatal(err)
	}
	permit.Done(&net.DNSError{IsTimeout: true, Err: "timeout"})
	if err := registry.RegisterProbe(key, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.Probe(context.Background(), key); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if status := registry.Status(key); status.State != StateHalfOpen {
		t.Fatalf("state = %q, want half_open", status.State)
	}
}

func TestRegistryRequestCheckRespectsOpenCircuitWhileBackgroundProbeCanRecover(t *testing.T) {
	registry, err := NewRegistry(Config{
		ConsecutiveFailures: 1,
		MinimumSamples:      1,
		WindowSize:          1,
		FailureRatio:        1,
		InitialOpen:         time.Hour,
		MaxOpen:             time.Hour,
		HalfOpenSuccesses:   1,
		HalfOpenMaxRequests: 1,
		ProbeInterval:       time.Minute,
		ProbeTimeout:        time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	key := Key{ProfileID: "memory-mem0", Domain: "memory", Driver: "mem0"}
	calls := 0
	probeErr := &net.DNSError{IsTimeout: true, Err: "timeout"}
	if err := registry.RegisterProbe(key, func(context.Context) error {
		calls++
		if calls == 1 {
			return probeErr
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Check(context.Background(), key); !errors.Is(err, probeErr) {
		t.Fatalf("first Check() error = %v", err)
	}
	if err := registry.Check(context.Background(), key); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second Check() error = %v, want circuit open", err)
	}
	if calls != 1 {
		t.Fatalf("request path bypassed open circuit: probe calls = %d", calls)
	}
	if err := registry.Probe(context.Background(), key); err != nil {
		t.Fatalf("background Probe() error = %v", err)
	}
	if calls != 2 || registry.Status(key).State != StateHalfOpen {
		t.Fatalf("background recovery calls=%d status=%+v", calls, registry.Status(key))
	}
}

func TestRegistryRejectsInvalidConfigurationAndKey(t *testing.T) {
	if _, err := NewRegistry(Config{}); err == nil {
		t.Fatal("NewRegistry(Config{}) error = nil")
	}
	registry, err := NewRegistry(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Begin(context.Background(), Key{}, "read"); err == nil {
		t.Fatal("Begin() accepted empty key")
	}
}

func TestRegistryProbeAllUsesBoundedConcurrency(t *testing.T) {
	registry, err := NewRegistry(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Int32
	var maximum atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, defaultProbeConcurrency)
	for index := 0; index < defaultProbeConcurrency+4; index++ {
		key := Key{ProfileID: fmt.Sprintf("profile-%d", index), Domain: "session", Driver: "redis"}
		if err := registry.RegisterProbe(key, func(context.Context) error {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			active.Add(-1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() {
		registry.probeAll(context.Background())
		close(done)
	}()
	for range defaultProbeConcurrency {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("probe pool did not reach configured concurrency")
		}
	}
	if got := maximum.Load(); got != defaultProbeConcurrency {
		t.Fatalf("max probe concurrency = %d, want %d", got, defaultProbeConcurrency)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe pool did not finish")
	}
}
