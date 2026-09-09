package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

// ErrUntrustedRun reports a run that carries no platform-authenticated scope,
// or one whose scope does not belong to this Runtime.
var ErrUntrustedRun = errors.New("agent: run has no trusted identity context")

// contextRunner is the Runner handed to protocol adapters. An adapter derives
// userID and sessionID from request fields the client controls, so this wrapper
// drops both arguments and replays the run with the scope the platform
// authenticated. The Runtime keeps the real Runner, so a future IM or scheduler
// path can still call it directly with a scope it derived itself.
type contextRunner struct {
	inner      runner.Runner
	tenantID   string
	appID      string
	revisionID string
}

// Run ignores userID and sessionID: both arrive from the untrusted request body
// and headers. A request without a matching trusted scope never reaches the
// agent.
func (r *contextRunner) Run(
	ctx context.Context,
	_ string,
	_ string,
	message model.Message,
	options ...trpcagent.RunOption,
) (<-chan *event.Event, error) {
	if r == nil || r.inner == nil {
		return nil, fmt.Errorf("%w: protocol runner is not configured", ErrUntrustedRun)
	}
	runContext, err := identity.RunContextFrom(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUntrustedRun, err)
	}
	if runContext.TenantID != r.tenantID ||
		runContext.AppID != r.appID ||
		runContext.RevisionID != r.revisionID {
		return nil, fmt.Errorf(
			"%w: scope %q/%q/%q does not match runtime %q/%q/%q",
			ErrUntrustedRun,
			runContext.TenantID,
			runContext.AppID,
			runContext.RevisionID,
			r.tenantID,
			r.appID,
			r.revisionID,
		)
	}
	return r.inner.Run(
		ctx,
		runContext.UserID(),
		runContext.SessionID,
		message,
		TrustedRunOptions(runContext, options)...,
	)
}

// TrustedRunOptions appends the platform request id to a caller's run options.
//
// Something always labels the run, and by default it is not the platform. The
// Runner seeds RunOptions with a uuid of its own before applying any option,
// and mints another if an option leaves the field empty (runner/runner.go:546
// and :550) — so a run with no id from here still gets one, just one that
// nothing outside the framework has ever seen. The OpenAI adapter contributes
// no id at all: it builds only history, the tool-result rewriter and external
// tools (server/openai/run_input.go, buildRunOptions).
//
// The id is therefore appended, never merged. The framework applies RunOptions
// in order and keeps the last write, so putting the platform id last overwrites
// the Runner's seed and anything a caller passed, and makes "this cannot be
// overridden" a property of the option list rather than a rule someone has to
// remember.
//
// It exists as one exported function because there are two ways into a Runner:
// this wrapper, for protocol adapters, and sessionrun.Handle.Run, for callers
// that have no adapter. Two copies of this rule would be two chances to lose
// it.
func TrustedRunOptions(
	runContext identity.RunContext,
	options []trpcagent.RunOption,
) []trpcagent.RunOption {
	// A fresh slice: appending to the caller's would write into an array it
	// still owns whenever it had spare capacity.
	trusted := make([]trpcagent.RunOption, 0, len(options)+1)
	trusted = append(trusted, options...)
	return append(trusted, trpcagent.WithRequestID(runContext.RequestID))
}

// Close is deliberately a no-op. The Runtime owns the real Runner and closes it
// exactly once, so an adapter that decided to close its injected Runner must
// not be able to reach it through this wrapper.
func (r *contextRunner) Close() error {
	return nil
}
