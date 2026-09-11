package bootstrap

import (
	"context"
	"time"

	control "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/controlhttp"
	telegram "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/telegrampreflight"
	wecom "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/wecompreflight"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

// Provider runners use separate policy claims and mTLS/egress pools. Neither
// depends on runtime permits, account admission or NATS availability.
type preflightRuntime struct {
	runners []*app.Runner
	closers []func()
}

func newPreflight(c Config, boot string) (result *preflightRuntime, err error) {
	if c.AccountSource != "control" || (!c.TelegramPreflightEnabled && !c.WeComPreflightEnabled) {
		return nil, nil
	}
	r := &preflightRuntime{}
	gate := &preflightClaimGate{slot: make(chan struct{}, 1)}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	options, err := c.Control.clientOptions(c.InstanceID)
	if err != nil {
		return nil, err
	}
	if c.TelegramPreflightEnabled {
		cfg, e := app.NewConfig(c.Control.ScopeID, c.Control.SourceEpoch, c.Control.PublicOrigin)
		if e != nil {
			return nil, e
		}
		client, e := control.NewPreflight(options)
		if e != nil {
			return nil, e
		}
		r.closers = append(r.closers, client.Close)
		probe := telegram.New()
		r.closers = append(r.closers, probe.Close)
		runner, e := app.NewRunner(gate.wrap(client), probe, cfg, boot)
		if e != nil {
			return nil, e
		}
		r.runners = append(r.runners, runner)
	}
	if c.WeComPreflightEnabled {
		cfg, e := app.NewWeComConfig(c.Control.ScopeID, c.Control.SourceEpoch)
		if e != nil {
			return nil, e
		}
		client, e := control.NewPreflight(options)
		if e != nil {
			return nil, e
		}
		r.closers = append(r.closers, client.Close)
		probe := wecom.New()
		r.closers = append(r.closers, probe.Close)
		runner, e := app.NewWeComRunner(gate.wrap(client), probe, cfg, boot)
		if e != nil {
			return nil, e
		}
		r.runners = append(r.runners, runner)
	}
	return r, nil
}
func (r *preflightRuntime) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, len(r.runners))
	for _, runner := range r.runners {
		go func() { done <- runner.Run(ctx) }()
	}
	var result error
	for range r.runners {
		if err := <-done; err != nil && result == nil {
			result = err
		}
		cancel()
	}
	return result
}
func (r *preflightRuntime) Close() {
	for _, close := range r.closers {
		close()
	}
}

// Control counts claims across policies for a principal/instance. Both provider
// loops (including uncertain-response retries) share this gate, not two 2/s budgets.
type preflightClaimGate struct {
	slot chan struct{}
	next time.Time
}
type gatedPreflightControl struct {
	app.Control
	gate *preflightClaimGate
}

func (g *preflightClaimGate) wrap(control app.Control) app.Control {
	return gatedPreflightControl{Control: control, gate: g}
}
func (c gatedPreflightControl) Claim(ctx context.Context, r app.ClaimRequest) (*app.Grant, error) {
	select {
	case c.gate.slot <- struct{}{}:
	case <-ctx.Done():
		return nil, app.ErrExpired
	}
	defer func() { <-c.gate.slot }()
	if wait := time.Until(c.gate.next); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, app.ErrExpired
		}
	}
	if ctx.Err() != nil {
		return nil, app.ErrExpired
	}
	grant, err := c.Control.Claim(ctx, r)
	// Start after the response so network/transaction jitter cannot compress the
	// rolling server-time interval. Resolve/Complete do not occupy this gate.
	c.gate.next = time.Now().Add(510 * time.Millisecond)
	return grant, err
}
