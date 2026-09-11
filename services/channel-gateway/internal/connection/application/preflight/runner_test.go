package preflight

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
)

const runnerEpoch = "11111111-1111-4111-8111-111111111111"
const runnerBoot = "22222222-2222-4222-8222-222222222222"

func runnerConfig(t *testing.T) ConfigSnapshot {
	t.Helper()
	v, e := NewConfig("pool", runnerEpoch, "https://gateway.example.com")
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func runnerGrant(r ClaimRequest) Grant {
	now := time.Now().UTC()
	g := Grant{PreflightID: "cpf_test", ScopeID: "pool", SourceEpoch: runnerEpoch, TenantID: "tnt_test", AccountID: "cha_test", Provider: "telegram", ProviderAccountID: "123", WebhookPath: "/v1/telegram/cha_test", AccountRevision: 7, ConnectionRevision: 4, LeaseEpoch: 1, Credential: Credential{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 2, Configured: true}, WebhookSecretConfigured: true, ServerTime: now, LeaseExpiresAt: now.Add(30 * time.Second), JobDeadlineAt: now.Add(120 * time.Second), ConfigDigest: r.Config.Digest, Request: r}
	if r.DiagnosticPolicy != "" {
		g.DiagnosticPolicy = r.DiagnosticPolicy
		g.ReceiveMode = "webhook"
		g.EffectiveConfigDigest, _ = wire.PreflightEffectiveConfigDigest(g.ScopeID, g.SourceEpoch, g.ReceiveMode, g.ConnectionRevision, r.Config.PublicOrigin, r.Config.OriginStatus)
	}
	return g
}

type runnerControl struct {
	claim    func(context.Context, ClaimRequest) (*Grant, error)
	complete func(context.Context, Grant, Result) error
}

func (c runnerControl) Claim(ctx context.Context, r ClaimRequest) (*Grant, error) {
	return c.claim(ctx, r)
}
func (c runnerControl) ResolveCredential(context.Context, Grant) (Secret, error) {
	return Secret{}, ErrDenied
}
func (c runnerControl) Complete(ctx context.Context, g Grant, r Result) error {
	if c.complete != nil {
		return c.complete(ctx, g, r)
	}
	return nil
}

type runnerProbe struct{}

func (runnerProbe) Inspect(context.Context, ProbeRequest) (ProbeResult, error) {
	return ProbeResult{}, ErrDenied
}
func TestRunnerFourSlotsAndSharedClaimRate(t *testing.T) {
	cfg := runnerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var starts []time.Time
	var active, peak atomic.Int32
	four := make(chan struct{})
	var once sync.Once
	c := runnerControl{claim: func(_ context.Context, r ClaimRequest) (*Grant, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		g := runnerGrant(r)
		return &g, nil
	}}
	r, e := NewRunner(c, runnerProbe{}, cfg, runnerBoot)
	if e != nil {
		t.Fatal(e)
	}
	r.execute = func(ctx context.Context, _ Grant, _ ConfigSnapshot) (Result, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := peak.Load()
			if old >= n || peak.CompareAndSwap(old, n) {
				break
			}
		}
		if n == 4 {
			once.Do(func() { close(four) })
		}
		<-ctx.Done()
		return Result{}, ErrExpired
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-four:
	case <-time.After(5 * time.Second):
		t.Fatal("four slots not started")
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(starts) != 4 || peak.Load() != 4 || active.Load() != 0 {
		t.Fatalf("claims=%d peak=%d active=%d", len(starts), peak.Load(), active.Load())
	}
	for i := 1; i < len(starts); i++ {
		if starts[i].Sub(starts[i-1]) < claimInterval {
			t.Fatal("instance claim rate exceeded")
		}
	}
}
func TestRunnerUncertainClaimReusesExactRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var requests []ClaimRequest
	c := runnerControl{claim: func(_ context.Context, r ClaimRequest) (*Grant, error) {
		requests = append(requests, r)
		if len(requests) == 1 {
			return nil, ErrUnavailable
		}
		cancel()
		return nil, nil
	}}
	r, e := NewRunner(c, runnerProbe{}, runnerConfig(t), runnerBoot)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Run(ctx); e != nil {
		t.Fatal(e)
	}
	if len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) {
		t.Fatal("uncertain claim changed request")
	}
	token, e := base64.RawURLEncoding.DecodeString(requests[0].Token.Reveal())
	if e != nil || len(token) != 32 || len(requests[0].Token.Reveal()) != 43 {
		t.Fatal("bad claim token")
	}
}
func TestRunnerDeadlinesUseServerDeltaNotWallClock(t *testing.T) {
	t0 := time.Now()
	server := time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct{ lease, job, work, report time.Duration }{{30 * time.Second, 120 * time.Second, 20 * time.Second, 30 * time.Second}, {9 * time.Second, 120 * time.Second, 4 * time.Second, 9 * time.Second}, {30 * time.Second, 7 * time.Second, 2 * time.Second, 7 * time.Second}, {-time.Second, time.Second, -6 * time.Second, -time.Second}} {
		g := Grant{ServerTime: server, LeaseExpiresAt: server.Add(tc.lease), JobDeadlineAt: server.Add(tc.job)}
		w, p := executionDeadlines(t0, g)
		if !w.Equal(t0.Add(tc.work)) || !p.Equal(t0.Add(tc.report)) {
			t.Fatal("incorrect bounded deadline")
		}
	}
}
func TestRunnerCompleteRetryIsIdenticalAndFenced(t *testing.T) {
	for _, first := range []error{ErrUnavailable, ErrConflict, ErrDenied, ErrExpired} {
		t.Run(first.Error(), func(t *testing.T) {
			cfg := runnerConfig(t)
			req := ClaimRequest{Config: cfg, InstanceEpoch: runnerBoot, RequestID: runnerEpoch, Token: NewSecret(strings.Repeat("A", 43))}
			g := runnerGrant(req)
			var results []Result
			c := runnerControl{complete: func(ctx context.Context, _ Grant, v Result) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("unbounded complete")
				}
				results = append(results, v)
				if len(results) == 1 {
					return first
				}
				return nil
			}}
			r, _ := NewRunner(c, runnerProbe{}, cfg, runnerBoot)
			want := Result{Config: cfg, ObservedAt: time.Now().UTC(), Checks: []Check{{ID: "synthetic", Details: json.RawMessage(`{}`)}}}
			r.execute = func(context.Context, Grant, ConfigSnapshot) (Result, error) { return want, nil }
			r.runClaim(context.Background(), time.Now(), g)
			n := 1
			if errors.Is(first, ErrUnavailable) {
				n = 2
			}
			if len(results) != n {
				t.Fatalf("completes=%d", len(results))
			}
			for _, got := range results {
				if !reflect.DeepEqual(want, got) {
					t.Fatal("retry changed result")
				}
			}
		})
	}
}
func TestRunnerStaleBudgetAndResolveFailureNeverComplete(t *testing.T) {
	cfg := runnerConfig(t)
	g := runnerGrant(ClaimRequest{Config: cfg})
	var calls atomic.Int32
	c := runnerControl{complete: func(context.Context, Grant, Result) error { calls.Add(100); return nil }}
	r, _ := NewRunner(c, runnerProbe{}, cfg, runnerBoot)
	r.execute = func(context.Context, Grant, ConfigSnapshot) (Result, error) {
		calls.Add(1)
		return Result{}, ErrUnavailable
	}
	r.runClaim(context.Background(), time.Now().Add(-time.Minute), g)
	if calls.Load() != 0 {
		t.Fatal("expired grant executed")
	}
	r.runClaim(context.Background(), time.Now(), g)
	if calls.Load() != 1 {
		t.Fatal("resolve error produced provider result")
	}
}
func TestRunnerInputAndPollingBounds(t *testing.T) {
	cfg := runnerConfig(t)
	if _, e := NewRunner(nil, runnerProbe{}, cfg, runnerBoot); !errors.Is(e, ErrInvalid) {
		t.Fatal("nil control")
	}
	if _, e := NewRunner(runnerControl{}, nil, cfg, runnerBoot); !errors.Is(e, ErrInvalid) {
		t.Fatal("nil probe")
	}
	for i := 0; i < 1000; i++ {
		d := emptyPollDelay()
		if d < 1600*time.Millisecond || d > 2400*time.Millisecond {
			t.Fatal("poll jitter outside bounds")
		}
	}
}

func TestRunnerCancelDuringClaimNeverStartsWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	var work, complete atomic.Int32
	c := runnerControl{claim: func(ctx context.Context, _ ClaimRequest) (*Grant, error) {
		close(entered)
		<-ctx.Done()
		return nil, ErrExpired
	}, complete: func(context.Context, Grant, Result) error { complete.Add(1); return nil }}
	r, _ := NewRunner(c, runnerProbe{}, runnerConfig(t), runnerBoot)
	r.execute = func(context.Context, Grant, ConfigSnapshot) (Result, error) { work.Add(1); return Result{}, nil }
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("claim never entered")
	}
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("claim cancellation did not drain")
	}
	if work.Load() != 0 || complete.Load() != 0 {
		t.Fatal("cancelled claim executed")
	}
}
func TestRunnerShutdownReportPreservesOriginalDeadline(t *testing.T) {
	cfg := runnerConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var claims, work, completes atomic.Int32
	reporting := make(chan time.Time, 1)
	c := runnerControl{claim: func(_ context.Context, r ClaimRequest) (*Grant, error) {
		claims.Add(1)
		g := runnerGrant(r)
		g.LeaseExpiresAt = g.ServerTime.Add(5100 * time.Millisecond)
		return &g, nil
	}, complete: func(reportCtx context.Context, _ Grant, _ Result) error {
		completes.Add(1)
		deadline, ok := reportCtx.Deadline()
		if !ok {
			t.Error("report lacks deadline")
		}
		reporting <- deadline
		<-reportCtx.Done()
		return ErrExpired
	}}
	r, _ := NewRunner(c, runnerProbe{}, cfg, runnerBoot)
	r.execute = func(context.Context, Grant, ConfigSnapshot) (Result, error) {
		work.Add(1)
		return Result{Config: cfg, ObservedAt: time.Now().UTC()}, nil
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var deadline time.Time
	select {
	case deadline = <-reporting:
	case <-time.After(time.Second):
		t.Fatal("report never started")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("shutdown discarded bounded report in progress")
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
		if time.Now().Before(deadline) {
			t.Fatal("report returned before original deadline")
		}
	case <-time.After(time.Until(deadline) + time.Second):
		t.Fatal("shutdown extended report deadline")
	}
	if claims.Load() != 1 || work.Load() != 1 || completes.Load() != 1 {
		t.Fatal("shutdown claimed new work or retried expired report")
	}
}
