// Package text is the durable half of a text channel: it records what a
// platform adapter accepted, executes each message once through the shared
// Session Run service, and sends each final answer at most once.
//
// It is protocol-independent by construction. Everything it needs from a
// platform arrives through channels.TextAdapter — an identity, a reply limit, a
// delivery loop and one send attempt — so a second channel is a second adapter
// and not a second branch in here. Nothing in this package names a platform,
// imports one or reads a protocol payload: a DeliveryTarget is opaque here and
// is interpreted only by the adapter that minted it.
//
// What it promises is what the first WeCom slice promised before it was
// generalised. An accepted message is durable before the adapter is told it was
// accepted; a Run that reached the Runner is never sent to it again; a failed
// send never re-runs an agent; one final reply is attempted once.
package text

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/model"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrAcceptFailed reports a message that could not be recorded. The
	// adapter must stop accepting; a protocol with inbound acknowledgement
	// may report failure so its platform can redeliver the event.
	ErrAcceptFailed = errors.New("channels/text: an accepted message could not be recorded")

	// ErrStorageUnavailable reports that channel storage stopped answering. It
	// is terminal for the same reason: the durable record is the only thing
	// keeping a claimed Run or an unsent answer from being lost, and a process
	// that cannot write it has nothing left to be trusted with.
	ErrStorageUnavailable = errors.New("channels/text: channel storage is unavailable")

	// ErrAdapterStopped reports that the adapter's delivery loop ended for a
	// reason of its own. It is a fixed sentinel because the adapter's error is
	// not: it can quote a socket, a credential or a peer, and the error this
	// package returns reaches a process log.
	ErrAdapterStopped = errors.New("channels/text: the adapter stopped serving")

	// ErrConfig is the sentinel behind every construction refusal, so a caller
	// can tell a misconfigured consumer from a failing one.
	ErrConfig = errors.New("channels/text: invalid configuration")

	// errForeignEnvelope and errUnsupportedInput are refusals of one accepted
	// event. Neither leaves this package: Accept reports ErrAcceptFailed, which
	// names no tenant, no binding and no message.
	errForeignEnvelope  = errors.New("channels/text: envelope does not belong to this binding")
	errUnsupportedInput = errors.New("channels/text: message carries input this channel cannot answer")
)

// The consumer policy. These are fixed rather than configurable: they are one
// coherent set, and a deployment that could tune them individually would be
// able to configure combinations this slice has never run.
const (
	// consumerMaxAttempts is generous because an attempt is a claim, not an
	// execution. Losing a race for the session lease against the web entry
	// spends one, and a conversation a person is also using through the browser
	// can lose several in a row before it gets its turn.
	consumerMaxAttempts = 16

	// consumerRunDuration bounds one execution. It is the deadline the Store
	// stamps and the recovery scanner honours, so it also decides how long a
	// dead process can hold a session.
	consumerRunDuration = 2 * time.Minute

	// consumerRecoveryGrace covers clock skew and a slow terminal write before
	// another scan may take a Run over.
	consumerRecoveryGrace = 30 * time.Second

	// busyBudget is how long a claim waits for the session lease before giving
	// the Run back. Yielding immediately would spend an attempt every time the
	// web entry holds the conversation, which is an ordinary event, not a
	// failure.
	busyBudget = 10 * time.Second
	busyDelay  = 250 * time.Millisecond

	// sendAttempts is one. A final reply is sent at most once: the protocol
	// gives no way to tell a redelivered stream from a second answer, so a
	// retry would risk saying the same thing twice.
	sendAttempts = 1
	sendTimeout  = 15 * time.Second

	// pollInterval is the floor on how often durable work is looked for. The
	// accept loop also nudges the task loop, so this is the safety net rather
	// than the normal path.
	pollInterval = time.Second

	// recoverInterval bounds how often the two recovery scans run. They only
	// matter after a crash, and they take locks.
	recoverInterval = 15 * time.Second

	// scanLimit is a page of candidates. One consumer serves one binding, so a
	// page this size is already more than a single serialized loop will get
	// through between ticks.
	scanLimit = 16

	// staleAfter is the dispatch scan window. This consumer publishes no
	// wakeups and marks nothing as dispatched, so the column it filters on is
	// always null and the value only has to be positive.
	staleAfter = time.Minute

	// persistAttempts bounds a retry of a write whose outcome is unknown. The
	// retry is the same write with the same ids: if the first one committed,
	// the second is fenced out by its own claim token and reports that, which
	// is how an ambiguous commit is resolved without ever re-running anything.
	persistAttempts = 3
	persistDelay    = 200 * time.Millisecond

	// storageFailures is how many consecutive scan failures are tolerated
	// before the consumer stops. A single failure is a blip and the next tick
	// retries it; a run of them is a database this process cannot serve from.
	storageFailures = 10
)

// RevisionCheck reports whether the revision a Run resolved to may execute in
// this process.
//
// It is a callback rather than a repository because the only question the
// consumer asks is a yes or no, and the answer comes from the control plane
// that startup already holds. Passing the repository instead would put a
// second reader of tenant configuration inside a protocol adapter.
type RevisionCheck func(ctx context.Context, tenantID, appID, revisionID string) error

// Config supplies the execution dependencies and optional observation/test hooks.
type Config struct {
	// Identity is the static trust anchor this consumer serves, from process
	// configuration. It decides which rows this consumer may see, claim,
	// execute and answer, and it is checked against the adapter's own identity
	// below: a consumer built from one binding and an adapter connected on
	// another would record one conversation and answer a different one.
	Identity channels.BindingIdentity
	// Adapter is the connected platform half. The consumer takes its accepted
	// events and sends replies through it, and never dials anything itself.
	Adapter channels.TextAdapter
	Store   channels.Store
	Runs    *sessionrun.Service
	// Revisions is re-asked before every execution; see RevisionCheck.
	Revisions RevisionCheck

	// Telemetry is optional and off by default. When it is nil the consumer
	// records nothing, which is the behaviour every existing caller has: the
	// stage records below are an operator's view of this loop and never a part
	// of it, so a process without a collector runs exactly as before.
	Telemetry *telemetry.Telemetry

	// Now and NewID are seams for tests. They default to the wall clock and to
	// UUIDs.
	Now   func() time.Time
	NewID func() string
}

// Consumer is the durable half of a text channel: it records what the adapter
// accepted, executes each Run once, and sends each answer at most once.
//
// It is deliberately two sequential loops and no pool. One loop writes accepted
// messages to the Store in arrival order; the other claims, executes and sends,
// one item at a time. Nothing queues in memory between them — the Store is the
// queue — so a crash loses nothing that was accepted, and a slow execution
// cannot make this process hold more work than it has recorded.
//
// The cost is stated rather than hidden: two conversations of one binding are
// answered one after the other, and this build does not promise cross-session
// parallelism.
type Consumer struct {
	identity  channels.BindingIdentity
	adapter   channels.TextAdapter
	store     channels.Store
	runs      *sessionrun.Service
	revisions RevisionCheck
	now       func() time.Time
	newID     func() string
	// replyLimit is the adapter's own byte limit, read once at construction so
	// that one execution cannot be bounded by two different numbers.
	replyLimit int

	scope  tenant.TenantContext
	scan   channels.ScanScope
	policy channels.RunPolicy
	worker string
	// stages records what the loops below did, for an operator. It is nil unless
	// telemetry was configured.
	stages *telemetry.ChannelRecorder

	// nudge shortens the wait after an accept. It is an optimisation and never
	// a guarantee: every durable item is found by the poll below whether or not
	// a nudge was delivered.
	nudge chan struct{}
	// nextRecover is read and written by the task loop only.
	nextRecover time.Time
	failures    int
}

// New validates the configuration and builds a Consumer. It opens no
// connection and touches no storage.
func New(cfg Config) (*Consumer, error) {
	if err := cfg.Identity.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if cfg.Adapter == nil || cfg.Store == nil || cfg.Runs == nil || cfg.Revisions == nil {
		return nil, fmt.Errorf(
			"%w: a consumer needs an adapter, a store, a run service and a revision check",
			ErrConfig)
	}
	// One binding, one connection, one consumer. The envelopes this consumer
	// records are written under cfg.Identity while the principal, the session
	// and the reply target come from the adapter, so two different identities
	// here would record one conversation under another tenant and answer it
	// down a connection that belongs to someone else.
	if cfg.Adapter.Identity() != cfg.Identity {
		return nil, fmt.Errorf(
			"%w: consumer and adapter must be built from the same binding", ErrConfig)
	}
	// Read once, and refused now rather than at the first answer. A limit that
	// cannot hold its own truncation notice would replace a long answer with
	// the notice alone, and one above what the Store accepts would produce
	// answers that execute and then fail to persist.
	replyLimit := cfg.Adapter.ReplyTextLimit()
	if replyLimit < minReplyTextLimit || replyLimit > channels.MaxMessageTextBytes {
		return nil, fmt.Errorf(
			"%w: the adapter reply limit must be between %d and %d bytes",
			ErrConfig, minReplyTextLimit, channels.MaxMessageTextBytes)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	newID := cfg.NewID
	if newID == nil {
		newID = uuid.NewString
	}
	policy := channels.RunPolicy{
		MaxAttempts:    consumerMaxAttempts,
		MaxRunDuration: consumerRunDuration,
		RecoveryGrace:  consumerRecoveryGrace,
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	// Built from this consumer's own binding rather than passed in, so a record
	// cannot be attributed to a tenant this consumer does not serve.
	stages, err := cfg.Telemetry.ChannelRecorder(telemetry.Binding{
		TenantID:  cfg.Identity.TenantID,
		AppID:     cfg.Identity.AgentAppID,
		BindingID: cfg.Identity.BindingID,
		Channel:   cfg.Identity.Channel,
	})
	if err != nil {
		return nil, err
	}
	return &Consumer{
		identity:   cfg.Identity,
		adapter:    cfg.Adapter,
		store:      cfg.Store,
		runs:       cfg.Runs,
		revisions:  cfg.Revisions,
		now:        now,
		newID:      newID,
		replyLimit: replyLimit,
		scope:      cfg.Identity.Scope(),
		scan: channels.ScanScope{
			TenantID:  cfg.Identity.TenantID,
			BindingID: cfg.Identity.BindingID,
		},
		policy: policy,
		worker: string(cfg.Identity.Channel) + "-" + cfg.Identity.BindingID,
		stages: stages,
		nudge:  make(chan struct{}, 1),
	}, nil
}

// Run serves until ctx ends or a half fails. It always returns a non-nil error,
// and never one that repeats a message, an identifier or a backend message.
func (c *Consumer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	failures := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		failures <- c.serve(ctx)
	}()
	go func() {
		defer wg.Done()
		failures <- c.taskLoop(ctx)
	}()
	err := <-failures
	// The other half is stopped and waited for before returning, so a caller
	// that goes on to close the Runtime and the pool cannot close them under a
	// goroutine that is still executing or still writing. Serve returns only
	// after the callbacks it started have finished, which is what makes waiting
	// on the accept half enough.
	cancel()
	wg.Wait()
	<-failures
	return err
}

// serve is the accept half: the adapter's own delivery loop, recording each
// event it accepts before it is told the event was accepted.
func (c *Consumer) serve(ctx context.Context) error {
	return scrubbed(c.adapter.Serve(ctx, c.Accept))
}

// scrubbed reduces what the adapter returned to something safe to log.
//
// Cancellation and this package's own sentinels pass through as themselves;
// everything else, including a Serve that simply returned, becomes
// ErrAdapterStopped. An adapter's error is not this package's to publish: it
// can quote a socket, a URL, a credential or a peer message, and whatever Run
// returns reaches a process log.
func scrubbed(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, ErrAcceptFailed):
		return ErrAcceptFailed
	case errors.Is(err, ErrStorageUnavailable):
		return ErrStorageUnavailable
	default:
		return ErrAdapterStopped
	}
}

// Accept records one accepted event and reports the stage once, however many
// times the write below had to be retried.
//
// This is the entry an adapter drives, and it implements channels.AcceptFunc: a
// nil return means the event is durable — newly recorded, or recognised as one
// already there — so a protocol that acknowledges its own deliveries may
// acknowledge then and not before.
func (c *Consumer) Accept(ctx context.Context, envelope channels.InboundEnvelope) error {
	ctx, span := c.stages.Start(ctx, telemetry.StageAccept)
	stage, err := c.record(ctx, envelope)
	span.End(stage)
	if err != nil {
		return err
	}
	c.wake()
	return nil
}

// record writes one message to the Store, retrying with the same identifiers.
//
// The ids are minted before the first call and reused, which is what makes a
// retry recognisable: the Store returns the row the first call created rather
// than creating a second Run. A redelivery of the same platform message id is
// recognised the same way, by the Store, on the external event id.
func (c *Consumer) record(
	ctx context.Context,
	envelope channels.InboundEnvelope,
) (telemetry.Result, error) {
	failed := telemetry.Result{
		Outcome:   telemetry.OutcomeFailed,
		ErrorType: channels.ErrorInternal,
	}
	if err := c.admits(envelope); err != nil {
		// Nothing about this event will pass on a retry, and the reason is not
		// carried anywhere: it would quote an envelope.
		failed.ErrorType = channels.ErrorPermanent
		return failed, ErrAcceptFailed
	}
	request := channels.AcceptRequest{
		IDs: channels.AcceptIDs{
			InboxID:   c.mint("in"),
			RunID:     c.mint("run"),
			RequestID: c.mint("req"),
		},
		Policy:   c.policy,
		Envelope: envelope,
	}
	for attempt := 1; ; attempt++ {
		request.Now = c.now()
		failed.Attempt = int32(attempt)
		if stored, err := c.store.Accept(ctx, c.scope, request); err == nil {
			// The row's own identifiers, which on the duplicate path are the
			// ones the first delivery created. The ids this call minted were not
			// stored, so recording them would invent a request that never
			// existed and break the correlation with the two stages below.
			outcome := telemetry.OutcomeSucceeded
			if stored.Duplicate {
				outcome = telemetry.OutcomeDuplicate
			}
			return telemetry.Result{
				Outcome:   outcome,
				RequestID: stored.RequestID,
				RunID:     stored.RunID,
				Attempt:   int32(attempt),
			}, nil
		}
		if attempt >= persistAttempts {
			return failed, ErrAcceptFailed
		}
		if err := sleepContext(ctx, persistDelay); err != nil {
			return failed, ErrAcceptFailed
		}
	}
}

// admits refuses an event this consumer must not record.
//
// Three checks, in decreasing order of distrust. All four identity fields are
// compared against the configured identity first: the adapter is the component
// this package trusts least, and a mis-wired or third-party one naming another
// tenant, app, binding or channel would otherwise write into an inbox nobody
// configured. Attachments are then refused rather than dropped, because this
// slice answers text and quietly ignoring a picture would answer a message the
// user did not send. What remains is the envelope's own validation, under the
// configured scope — never under a scope read out of the envelope.
func (c *Consumer) admits(envelope channels.InboundEnvelope) error {
	if !c.identity.Owns(envelope) {
		return errForeignEnvelope
	}
	if len(envelope.Message.Attachments) > 0 {
		return errUnsupportedInput
	}
	return envelope.Validate(c.scope)
}

// taskLoop is the one place execution and sending happen.
func (c *Consumer) taskLoop(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if err := c.drain(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-c.nudge:
		}
	}
}

func (c *Consumer) drain(ctx context.Context) error {
	if err := c.recover(ctx); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		// Sending comes first and runs to completion, including answers this
		// process found already recorded. The Store orders the parts of one
		// answer and no further, so the order of the answers themselves is the
		// job of this loop: a Session whose earlier reply is still unsettled
		// must not have a later message executed, or the two could go out
		// reversed.
		settled, err := c.sendPending(ctx)
		if err != nil {
			return err
		}
		if !settled {
			// The send side could not be read. Executing now would add a newer
			// answer while an older one may still be waiting.
			return nil
		}
		executed, err := c.executeNext(ctx)
		if err != nil {
			return err
		}
		if !executed {
			return nil
		}
	}
}

// recover moves work that an earlier process left in flight. Both scans are
// scoped to this binding inside the database, so neither reads nor rewrites
// another tenant or another binding.
func (c *Consumer) recover(ctx context.Context) error {
	now := c.now()
	if now.Before(c.nextRecover) {
		return nil
	}
	c.nextRecover = now.Add(recoverInterval)
	request := channels.RecoverRequest{Now: now, Scope: c.scan, Limit: scanLimit}
	if _, err := c.store.RecoverRuns(ctx, request); err != nil {
		return c.storageFailed(ctx)
	}
	if _, err := c.store.RecoverOutbox(ctx, request); err != nil {
		return c.storageFailed(ctx)
	}
	c.failures = 0
	return nil
}

// wake asks the task loop to look now. A full channel already means exactly
// that, so the send is dropped rather than blocking the accept loop.
func (c *Consumer) wake() {
	select {
	case c.nudge <- struct{}{}:
	default:
	}
}

// storageFailed decides whether a failed scan is a blip or the end. It never
// reports the underlying error: a driver message can carry a DSN, and this
// error reaches a process-level log.
func (c *Consumer) storageFailed(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	c.failures++
	if c.failures >= storageFailures {
		return ErrStorageUnavailable
	}
	return nil
}

// mint returns a fresh identifier for this channel. The channel prefix is the
// adapter's own type, so the identifiers one binding writes keep the shape they
// have always had and stay distinguishable in the shared tables.
func (c *Consumer) mint(kind string) string {
	return string(c.identity.Channel) + "-" + kind + "-" + c.newID()
}

// ownsRun reports whether a claimed row is one this consumer may act on.
//
// The scans are already scoped in SQL, so this can only fail if a row was
// rewritten underneath the scan. It is checked anyway because the alternative
// to a redundant check here is executing another tenant conversation, and
// because the claim is the first point where the whole row is in hand.
func (c *Consumer) ownsRun(run channels.Run) bool {
	return run.TenantID == c.identity.TenantID &&
		run.ChannelBindingID == c.identity.BindingID &&
		run.AgentAppID == c.identity.AgentAppID &&
		run.Channel == c.identity.Channel
}

// ownsPart is ownsRun for the delivery side.
func (c *Consumer) ownsPart(part channels.OutboxPart) bool {
	return part.TenantID == c.identity.TenantID &&
		part.ChannelBindingID == c.identity.BindingID &&
		part.Channel == c.identity.Channel
}

// sleepContext waits, or reports that the wait was cut short.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// runToken is the fence every write of one execution carries.
func runToken(run channels.Run) channels.RunToken {
	return channels.RunToken{RunID: run.RunID, ClaimToken: run.ClaimToken}
}

// busyStart acquires the session, waiting inside the claim budget while the
// conversation is busy elsewhere.
//
// Waiting rather than yielding is the difference between sharing a conversation
// with the web entry and starving behind it: a yield spends an attempt and puts
// the Run back on a backoff, so a browser session holding the lease for a
// minute could exhaust the budget without a single execution.
func (c *Consumer) busyStart(
	ctx context.Context,
	request sessionrun.Request,
	until time.Time,
) (*sessionrun.Handle, error) {
	for {
		handle, err := c.runs.Start(ctx, request)
		if err == nil {
			return handle, nil
		}
		if !errors.Is(err, sessionlease.ErrSessionBusy) || !c.now().Before(until) {
			return nil, err
		}
		if err := sleepContext(ctx, busyDelay); err != nil {
			return nil, err
		}
	}
}

// trimmed reports whether there is an answer worth storing.
func trimmed(text string) bool { return strings.TrimSpace(text) != "" }

// executeNext claims and executes at most one Run, and reports whether it found
// one. One per call, so the caller can send the answer before claiming again.
func (c *Consumer) executeNext(ctx context.Context) (bool, error) {
	candidates, err := c.store.ListDispatchableRuns(ctx, channels.DispatchScanRequest{
		Now:        c.now(),
		StaleAfter: staleAfter,
		Scope:      c.scan,
		Limit:      scanLimit,
	})
	if err != nil {
		return false, c.storageFailed(ctx)
	}
	c.failures = 0
	for _, candidate := range candidates {
		run, err := c.store.GetRun(ctx, c.scope, candidate.RunID)
		if err != nil {
			return false, c.storageFailed(ctx)
		}
		if !c.ownsRun(run) {
			continue
		}
		// Read before the claim, never after: the budget the Store returns is
		// measured from the moment it stamped the row, so anchoring it to a
		// later reading would push this execution past the deadline the
		// recovery scanner is enforcing.
		claimedAt := c.now()
		claim, ok, err := c.store.ClaimNextRun(ctx, c.scope, run.SessionKey(), channels.ClaimRunRequest{
			ClaimToken: c.mint("claim"),
			ClaimedBy:  c.worker,
			Now:        claimedAt,
		})
		if err != nil {
			return false, c.storageFailed(ctx)
		}
		if !ok {
			// The head of that session is running elsewhere, or is in backoff.
			// Ordering is strict, so there is nothing later to skip ahead to.
			continue
		}
		if !c.ownsRun(claim.Run) {
			return true, c.yield(ctx, runToken(claim.Run), channels.ErrorInternal)
		}
		return true, c.execute(ctx, claim, claimedAt.Add(claim.RemainingExecutionBudget))
	}
	return false, nil
}

// execute answers one claimed Run, or records why it will not be answered, and
// reports the stage once.
func (c *Consumer) execute(
	ctx context.Context,
	claim channels.RunClaim,
	deadline time.Time,
) error {
	ctx, span := c.stages.Start(ctx, telemetry.StageExecute)
	stage, err := c.answer(ctx, claim, deadline)
	span.End(stage)
	return err
}

// answer is that execution.
//
// What it returns for the record is what this attempt decided, which is not the
// same claim as "this was durably recorded": the row in the Store is the
// record of a Run, and a terminal write that could not be made comes back to
// the caller as an error instead.
func (c *Consumer) answer(
	ctx context.Context,
	claim channels.RunClaim,
	deadline time.Time,
) (telemetry.Result, error) {
	token := runToken(claim.Run)
	stage := telemetry.Result{
		RequestID: claim.Run.RequestID,
		RunID:     claim.Run.RunID,
		Attempt:   claim.Run.Attempt,
	}

	// An attempt that already reached the Runner is never sent to it again.
	// The Events of that attempt are not reconstructible here, so this process
	// cannot tell an execution that produced nothing from one that produced an
	// answer nobody stored — and a model call is not a safe thing to repeat on
	// a guess. The Run fails with that stated, which is the honest outcome and
	// the one an operator can act on.
	if claim.Run.FirstExecutionStartedAt != nil {
		// Recorded as its own outcome: this Run failed without the Runner being
		// called, which is a different event from one the model answered badly.
		stage.Outcome = telemetry.OutcomeInterrupted
		stage.ErrorType = channels.ErrorInterruptedBeforeOutput
		stage.RevisionID = claim.Run.RevisionID
		return stage, c.finish(ctx, channels.FinishRunRequest{
			Token:      token,
			Status:     channels.RunFailed,
			RevisionID: claim.Run.RevisionID,
			ErrorType:  channels.ErrorInterruptedBeforeOutput,
		})
	}

	runCtx, cancel := context.WithDeadline(ctx, deadline)
	busyUntil := c.now().Add(busyBudget)
	if busyUntil.After(deadline) {
		busyUntil = deadline
	}
	handle, err := c.busyStart(runCtx, sessionrun.Request{
		RequestID:   claim.Run.RequestID,
		TenantID:    claim.Run.TenantID,
		AppID:       claim.Run.AgentAppID,
		PrincipalID: claim.Run.PrincipalID,
		SessionID:   claim.Run.SessionID,
	}, busyUntil)
	if err != nil {
		// Nothing has executed, so the Run can go back and be tried again. The
		// error itself is not carried anywhere: it can quote a backend.
		cancel()
		errorType := startErrorType(err)
		stage.Outcome, stage.ErrorType = telemetry.OutcomeYielded, errorType
		return stage, c.yield(ctx, token, errorType)
	}

	scope := handle.Scope()
	stage.RevisionID = scope.RevisionID
	// The pin is resolved now, so this is the first point at which the revision
	// that will actually answer is known. Startup checked the published one;
	// this checks the one in hand, because a route published in between could
	// otherwise move a durable conversation onto a store that forgets it.
	if err := c.revisions(runCtx, scope.TenantID, scope.AppID, scope.RevisionID); err != nil {
		handle.Close()
		cancel()
		stage.Outcome, stage.ErrorType = telemetry.OutcomeFailed, channels.ErrorPermanent
		return stage, c.finish(ctx, channels.FinishRunRequest{
			Token:      token,
			Status:     channels.RunFailed,
			RevisionID: scope.RevisionID,
			ErrorType:  channels.ErrorPermanent,
		})
	}
	if err := c.store.RecordRunRevision(ctx, c.scope, token, scope.RevisionID, c.now()); err != nil {
		handle.Close()
		cancel()
		stage.Outcome, stage.ErrorType = telemetry.OutcomeSkipped, channels.ErrorInternal
		return stage, c.claimLost(ctx, err)
	}
	// After this the claim may no longer be yielded, and no later attempt may
	// call the Runner for this Run.
	if err := c.store.MarkRunStarted(ctx, c.scope, token, c.now()); err != nil {
		handle.Close()
		cancel()
		stage.Outcome, stage.ErrorType = telemetry.OutcomeSkipped, channels.ErrorInternal
		return stage, c.claimLost(ctx, err)
	}

	startedAt := c.now()
	events, err := handle.Run(model.NewUserMessage(claim.Message.Text))
	if err != nil {
		handle.Close()
		cancel()
		stage.Outcome, stage.ErrorType = telemetry.OutcomeFailed, channels.ErrorAgentFailed
		return stage, c.finish(ctx, channels.FinishRunRequest{
			Token:      token,
			Status:     channels.RunFailed,
			RevisionID: scope.RevisionID,
			ErrorType:  channels.ErrorAgentFailed,
			Stats:      channels.RunStats{ExecutionMillis: c.now().Sub(startedAt).Milliseconds()},
		})
	}
	var collected replyCollector
	// Drained to the end, always. The Runtime lease is what keeps the Runner
	// alive, and a reader that stops early strands the goroutine writing to it.
	for e := range events {
		collected.observe(e)
	}
	// Read before the cancel below, which would make it non-nil regardless.
	expired := runCtx.Err() != nil
	// Close first, then cancel: Close is what releases the Runtime lease and
	// the session lease in the order that package requires, and cancelling the
	// context it derived from first would tear that down underneath it.
	handle.Close()
	cancel()

	request := channels.FinishRunRequest{
		Token:      token,
		Status:     channels.RunSucceeded,
		RevisionID: scope.RevisionID,
		Stats: channels.RunStats{
			EventCount:      collected.events,
			ExecutionMillis: c.now().Sub(startedAt).Milliseconds(),
		},
	}
	// Bounded here, before it is stored, so the record and the message the user
	// sees are the same bytes: a reply shortened at send time would leave the
	// Store holding an answer that was never delivered.
	answer := boundReply(collected.answer(), c.replyLimit)
	switch {
	case collected.failed:
		request.Status = channels.RunFailed
		request.ErrorType = channels.ErrorAgentFailed
	case expired && !trimmed(answer):
		request.Status = channels.RunFailed
		request.ErrorType = channels.ErrorRunTimeout
	case trimmed(answer):
		// One part, because this slice sends one final reply per message.
		request.Outbox = []channels.OutboxDraft{{
			OutboxID:        c.mint("out"),
			PartNo:          0,
			ClientMessageID: claim.Run.RequestID,
			MaxAttempts:     sendAttempts,
			Message:         channels.OutboundMessage{Text: answer},
			DeliveryTarget:  claim.DeliveryTarget,
		}}
	}
	stage.Outcome, stage.Events = telemetry.OutcomeSucceeded, collected.events
	if request.Status == channels.RunFailed {
		stage.Outcome, stage.ErrorType = telemetry.OutcomeFailed, request.ErrorType
	}
	return stage, c.finish(ctx, request)
}

// startErrorType classifies a failed Start for the operator record. It reads
// the error and stores a class; it never stores the error.
func startErrorType(err error) channels.ErrorType {
	if errors.Is(err, sessionlease.ErrSessionBusy) ||
		errors.Is(err, sessionrun.ErrCoordinationUnavailable) {
		return channels.ErrorInternal
	}
	return channels.ErrorAgentFailed
}

// claimLost turns a mid-execution write failure into a decision. A stale claim
// means this attempt no longer owns the Run and must write nothing more.
func (c *Consumer) claimLost(ctx context.Context, err error) error {
	if errors.Is(err, channels.ErrStaleClaim) {
		return nil
	}
	return c.storageFailed(ctx)
}

// yield returns a claimed Run to the queue.
func (c *Consumer) yield(
	ctx context.Context,
	token channels.RunToken,
	errorType channels.ErrorType,
) error {
	_, err := c.store.YieldRun(ctx, c.scope, token, channels.YieldRunRequest{
		ErrorType: errorType,
		Now:       c.now(),
	})
	switch {
	case err == nil,
		errors.Is(err, channels.ErrStaleClaim),
		errors.Is(err, channels.ErrExecutionStarted):
		return nil
	default:
		return c.storageFailed(ctx)
	}
}

// finish writes the outcome and the whole answer in one transaction.
//
// A failure whose outcome is unknown is retried with the same request and the
// same ids. If the first write did commit, the second finds the fence cleared
// and reports a stale claim, which is the answer rather than an error: the Run
// is decided, and re-running the model to find out would be the one thing this
// slice must never do.
func (c *Consumer) finish(ctx context.Context, request channels.FinishRunRequest) error {
	request.Stats.OutputParts = int32(len(request.Outbox))
	for attempt := 1; ; attempt++ {
		request.Now = c.now()
		if _, err := c.store.FinishRun(ctx, c.scope, request); err == nil {
			c.wake()
			return nil
		} else if errors.Is(err, channels.ErrStaleClaim) {
			return nil
		}
		if attempt >= persistAttempts {
			return ErrStorageUnavailable
		}
		if err := sleepContext(ctx, persistDelay); err != nil {
			// Shutting down. The Run keeps its claim until the deadline the
			// Store stamped, and recovery decides it.
			return nil
		}
	}
}

// sendPending sends every answer that is ready, oldest first, and reports
// whether the send side is settled — that is, whether it now knows there is
// nothing left to send. A scan it could not read is not settled.
func (c *Consumer) sendPending(ctx context.Context) (bool, error) {
	for {
		if ctx.Err() != nil {
			return false, nil
		}
		parts, read, err := c.pendingSends(ctx)
		if err != nil || !read {
			return false, err
		}
		sent := false
		blocked := ""
		for _, part := range parts {
			if part.SessionID == blocked {
				continue
			}
			claim, ok, err := c.store.ClaimOutbox(ctx, c.scope, channels.ClaimOutboxRequest{
				OutboxID:        part.OutboxID,
				ExpectedChannel: c.identity.Channel,
				SendToken:       c.mint("send"),
				SentBy:          c.worker,
				SendTimeout:     sendTimeout,
				Now:             c.now(),
			})
			if err != nil {
				return false, c.storageFailed(ctx)
			}
			if !ok {
				// Somebody else holds the oldest answer of this Session, or it
				// is not due. Its successors wait for it either way.
				blocked = part.SessionID
				continue
			}
			if err := c.complete(ctx, channels.OutboxToken{
				OutboxID:  claim.Part.OutboxID,
				SendToken: claim.Part.SendToken,
			}, c.deliver(ctx, claim.Part)); err != nil {
				return false, err
			}
			sent = true
			break
		}
		if !sent {
			return true, nil
		}
	}
}

// pendingSends reads this binding's sendable parts, oldest answer first, and
// reports whether the read succeeded.
//
// The scan returns references in identifier order, which says nothing about
// which answer came first, so each one is read back and ordered by the position
// of its Run in the Session. Two answers of one Session are only both pending
// after a crash — this consumer settles one before executing the next — and
// that is exactly when the order has to be reconstructed rather than assumed.
func (c *Consumer) pendingSends(ctx context.Context) ([]channels.OutboxPart, bool, error) {
	candidates, err := c.store.ListDispatchableOutbox(ctx, channels.DispatchScanRequest{
		Now:        c.now(),
		StaleAfter: staleAfter,
		Scope:      c.scan,
		Limit:      scanLimit,
	})
	if err != nil {
		return nil, false, c.storageFailed(ctx)
	}
	c.failures = 0
	parts := make([]channels.OutboxPart, 0, len(candidates))
	accepted := make(map[string]int64, len(candidates))
	for _, candidate := range candidates {
		if candidate.Channel != c.identity.Channel {
			continue
		}
		part, err := c.store.GetOutboxPart(ctx, c.scope, candidate.OutboxID)
		if err != nil {
			return nil, false, c.storageFailed(ctx)
		}
		if !c.ownsPart(part) {
			continue
		}
		if _, ok := accepted[part.RunID]; !ok {
			run, err := c.store.GetRun(ctx, c.scope, part.RunID)
			if err != nil {
				return nil, false, c.storageFailed(ctx)
			}
			accepted[part.RunID] = run.AcceptSequence
		}
		parts = append(parts, part)
	}
	sort.Slice(parts, func(i, j int) bool {
		left, right := parts[i], parts[j]
		if left.SessionID != right.SessionID {
			return left.SessionID < right.SessionID
		}
		if accepted[left.RunID] != accepted[right.RunID] {
			return accepted[left.RunID] < accepted[right.RunID]
		}
		return left.PartNo < right.PartNo
	})
	return parts, true, nil
}

// deliver makes at most one attempt to put one answer on the wire.
//
// It is a separate step from recording the result so that a failure to record
// can be retried without the possibility of sending again: the send is behind
// this call, and the caller only has the result.
//
// The record is taken here, around that one attempt, and never around the
// retried write in complete: a stage that counted the write would report a
// second send that did not happen.
func (c *Consumer) deliver(ctx context.Context, part channels.OutboxPart) channels.SendResult {
	ctx, span := c.stages.Start(ctx, telemetry.StageDeliver)
	result, outcome := c.send(ctx, part)
	span.End(telemetry.Result{
		Outcome:   outcome,
		ErrorType: result.ErrorType,
		RequestID: part.RequestID,
		RunID:     part.RunID,
		OutboxID:  part.OutboxID,
		Attempt:   part.Attempt,
	})
	return result
}

// send is that attempt, and reports what the stage saw alongside what the Store
// stores. The two are not the same judgement: the Store needs to know whether
// the part may be tried again, and an operator needs to know whether anything
// was put on the wire at all.
//
// Every refusal below is this package's own, and none of them consults the
// adapter. The one consequence worth stating: a shutdown that coincides with a
// target the adapter would have rejected is recorded as skipped rather than as
// a stale target, because the decode now happens behind Send. Nothing is put on
// the wire either way.
func (c *Consumer) send(
	ctx context.Context,
	part channels.OutboxPart,
) (channels.SendResult, telemetry.Outcome) {
	if !c.ownsPart(part) {
		return channels.SendResult{
			Outcome:   channels.SendPermanent,
			ErrorType: channels.ErrorPermanent,
		}, telemetry.OutcomeSkipped
	}
	if part.DuplicateRisk || part.Attempt > sendAttempts {
		// Some earlier attempt may already have been delivered. Sending again
		// to find out would be the duplicate this flag exists to prevent, so
		// the part keeps the warning and stops here.
		return channels.SendResult{
			Outcome:   channels.SendUnknown,
			ErrorType: channels.ErrorOutcomeUnknown,
		}, telemetry.OutcomeSkipped
	}
	if ctx.Err() != nil {
		// Shutting down. Nothing was written, so this is a plain failure and
		// not an unknown outcome.
		return channels.SendResult{
			Outcome:   channels.SendRetryable,
			ErrorType: channels.ErrorInternal,
		}, telemetry.OutcomeSkipped
	}
	// The stored text, transmitted as it stands or refused. The adapter owns
	// the target it minted and reports what its protocol said; it never gets to
	// decide whether this attempt was allowed to happen.
	report := c.adapter.Send(ctx, part.DeliveryTarget, part.Message)
	result := report.SendResult()
	return result, deliverOutcome(result, report)
}

// deliverOutcome reads the result the Store will store together with the report
// it was made from, and splits that one permanent class into the two an
// operator acts on differently: a reply address that is spent, or that belongs
// to a connection which is gone, is the channel working as designed, and a
// refused message is not.
//
// It reads the stored result first so the two can never disagree — a report
// that failed closed on its way to a SendResult is unknown here as well.
func deliverOutcome(
	result channels.SendResult,
	report channels.DeliveryReport,
) telemetry.Outcome {
	switch result.Outcome {
	case channels.SendSucceeded:
		return telemetry.OutcomeSucceeded
	case channels.SendPermanent:
		if report.Outcome == channels.DeliveryTargetStale {
			return telemetry.OutcomeStaleTarget
		}
		return telemetry.OutcomeRejected
	default:
		return telemetry.OutcomeUnknown
	}
}

// complete records one send attempt. It never sends.
func (c *Consumer) complete(
	ctx context.Context,
	token channels.OutboxToken,
	result channels.SendResult,
) error {
	request := channels.CompleteOutboxRequest{Token: token, Result: result}
	for attempt := 1; ; attempt++ {
		request.Now = c.now()
		if _, err := c.store.CompleteOutbox(ctx, c.scope, request); err == nil {
			return nil
		} else if errors.Is(err, channels.ErrStaleClaim) {
			// The row moved on: this attempt was already recorded, or the send
			// deadline passed and recovery decided it. Either way the outcome
			// is written and this attempt has nothing left to say.
			return nil
		}
		if attempt >= persistAttempts {
			return ErrStorageUnavailable
		}
		if err := sleepContext(ctx, persistDelay); err != nil {
			return nil
		}
	}
}
