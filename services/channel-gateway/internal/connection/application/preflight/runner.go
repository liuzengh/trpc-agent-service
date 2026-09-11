package preflight

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	catalog "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

const (
	RunnerConcurrency = 4
	claimInterval     = 500 * time.Millisecond
	workBudget        = 20 * time.Second
	reportReserve     = 5 * time.Second
)

// Runner has one claim scheduler for this instance and four bounded execution
// slots. It does not consult runtime account permits, catalog readiness or NATS.
type Runner struct {
	policy        string
	control       Control
	config        ConfigSnapshot
	instanceEpoch string
	execute       func(context.Context, Grant, ConfigSnapshot) (Result, error)
}

func NewRunner(control Control, probe TelegramProbe, cfg ConfigSnapshot, instanceEpoch string) (*Runner, error) {
	if control == nil || probe == nil || ValidateConfig(cfg) != nil || cfg.Policy != "" || !catalog.ValidEpoch(instanceEpoch) {
		return nil, ErrInvalid
	}
	// Capture pointer fields too: an external configuration mutation must not
	// silently change an in-flight task's pinned fingerprint.
	if cfg.PublicOrigin != nil {
		value := *cfg.PublicOrigin
		cfg.PublicOrigin = &value
	}
	service := &Service{Control: control, Probe: probe}
	return &Runner{policy: wire.PreflightReceiveModesPolicy, control: control, config: cfg, instanceEpoch: instanceEpoch, execute: service.Execute}, nil
}
func (r *Runner) Run(ctx context.Context) error {
	if ctx == nil || r == nil || r.control == nil || r.execute == nil {
		return ErrInvalid
	}
	slots := make(chan struct{}, RunnerConcurrency)
	var running sync.WaitGroup
	defer running.Wait()
	var nextClaim time.Time
	backoff := 2 * time.Second
	// All retries pass through this same instance gate; there are no per-slot
	// polling loops that could turn four slots into eight requests per second.
	claim := func(request ClaimRequest) (*Grant, error) {
		if !waitUntil(ctx, nextClaim) {
			return nil, ErrExpired
		}
		nextClaim = time.Now().Add(claimInterval)
		return r.control.Claim(ctx, request)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case slots <- struct{}{}:
		}
		if !waitUntil(ctx, nextClaim) {
			<-slots
			return nil
		}
		req, err := r.newClaim()
		if err != nil {
			<-slots
			return err
		}
		t0 := time.Now()
		grant, err := claim(req)
		if errors.Is(err, ErrUnavailable) && ctx.Err() == nil {
			// The exact request, token and ID are reused after an uncertain response.
			grant, err = claim(req)
		}
		if err != nil {
			<-slots
			if ctx.Err() != nil {
				return nil
			}
			nextClaim = later(nextClaim, time.Now().Add(backoff))
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = 2 * time.Second
		if grant == nil {
			<-slots
			nextClaim = later(nextClaim, time.Now().Add(emptyPollDelay()))
			continue
		}
		if ValidateGrant(*grant, r.config) != nil {
			<-slots
			nextClaim = later(nextClaim, time.Now().Add(backoff))
			continue
		}
		// A port implementation cannot substitute a different authenticated claim.
		if grant.Request.RequestID != req.RequestID || grant.Request.InstanceEpoch != req.InstanceEpoch || grant.Request.Token.Reveal() != req.Token.Reveal() {
			<-slots
			nextClaim = later(nextClaim, time.Now().Add(backoff))
			continue
		}
		running.Add(1)
		go func(g Grant) { defer running.Done(); defer func() { <-slots }(); r.runClaim(ctx, t0, g) }(*grant)
	}
}
func (r *Runner) newClaim() (ClaimRequest, error) {
	var token [32]byte
	var id [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return ClaimRequest{}, ErrUnavailable
	}
	if _, err := rand.Read(id[:]); err != nil {
		return ClaimRequest{}, ErrUnavailable
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	requestID := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
	return ClaimRequest{DiagnosticPolicy: r.policy, Config: r.config, InstanceEpoch: r.instanceEpoch, RequestID: requestID, Token: NewSecret(base64.RawURLEncoding.EncodeToString(token[:]))}, nil
}
func executionDeadlines(t0 time.Time, g Grant) (work, report time.Time) {
	remaining := min(g.LeaseExpiresAt.Sub(g.ServerTime), g.JobDeadlineAt.Sub(g.ServerTime))
	report = t0.Add(remaining)
	work = minTime(t0.Add(workBudget), report.Add(-reportReserve))
	return
}
func (r *Runner) runClaim(ctx context.Context, t0 time.Time, g Grant) {
	workDeadline, reportDeadline := executionDeadlines(t0, g)
	if ctx.Err() != nil || !time.Now().Before(workDeadline) {
		return
	}
	workCtx, cancel := context.WithDeadline(ctx, workDeadline)
	result, err := r.execute(workCtx, g, r.config)
	workExpired := workCtx.Err() != nil
	cancel()
	if err != nil || workExpired || !time.Now().Before(reportDeadline) {
		return
	}
	// A completed observation may be reported during bounded shutdown cleanup,
	// but neither shutdown nor retry extends the original report deadline.
	reportCtx, stop := context.WithDeadline(context.WithoutCancel(ctx), reportDeadline)
	defer stop()
	err = r.control.Complete(reportCtx, g, result)
	if errors.Is(err, ErrUnavailable) && reportCtx.Err() == nil && time.Now().Before(reportDeadline) {
		_ = r.control.Complete(reportCtx, g, result)
	}
}
func emptyPollDelay() time.Duration {
	return 1600*time.Millisecond + time.Duration(mathrand.Int64N(int64(800*time.Millisecond)+1))
}
func waitUntil(ctx context.Context, t time.Time) bool {
	if ctx.Err() != nil {
		return false
	}
	d := time.Until(t)
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func NewWeComRunner(control Control, probe WeComProbe, cfg ConfigSnapshot, instanceEpoch string) (*Runner, error) {
	if control == nil || probe == nil || ValidateConfig(cfg) != nil || cfg.Policy != "wecom_long_connection_v1" || !catalog.ValidEpoch(instanceEpoch) {
		return nil, ErrInvalid
	}
	service := &WeComService{Control: control, Probe: probe}
	return &Runner{policy: cfg.Policy, control: control, config: cfg, instanceEpoch: instanceEpoch, execute: service.Execute}, nil
}
