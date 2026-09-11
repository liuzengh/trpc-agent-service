package application

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

var (
	ErrManifestMissing     = errors.New("fixed manifest has not arrived")
	ErrManifestInvalid     = errors.New("fixed manifest failed validation")
	ErrManifestUnsupported = errors.New("fixed manifest is not supported by Worker V1")
	// ErrManifestContractMismatch is release skew, not a manifest defect: this
	// process pinned a different platform contract release than the one that
	// compiled the manifest. The Run waits for a matching Worker release instead
	// of terminating permanently.
	ErrManifestContractMismatch = errors.New("fixed manifest requires a different platform contract release")
	ErrDependency               = errors.New("execution dependency temporarily unavailable")
	ErrCredentialDenied         = errors.New("execution credentials rejected")
	ErrRuntimeFailed            = errors.New("agent runtime failed")
	ErrMemoryApply              = errors.New("accepted memory application failed")
	ErrMemoryFinalize           = errors.New("accepted memory finalization failed")
	ErrSessionInvalid           = errors.New("session snapshot failed validation")
	ErrSessionPreparation       = errors.New("session store requires explicit preparation")
)

type ManifestReader interface {
	Resolve(context.Context, domain.Route) (domain.Plan, error)
}
type RuntimeFactory interface {
	// Prepare resolves one complete credential batch, invokes check after batch
	// receipt and before constructors, and never retries Resolve transparently.
	Prepare(context.Context, domain.Grant, domain.Plan, func(context.Context) error) (AttemptRuntime, error)
}
type AttemptRuntime interface {
	Load(context.Context, domain.Head) ([]byte, error)
	Execute(context.Context, []byte) (domain.RuntimeResult, error)
	Stage(context.Context, []byte) (domain.Candidate, error)
	Close()
}

// AcceptedMemoryRuntime is optional. The ordinary runtime contract remains
// unchanged; Memory-enabled results require this post-accept-only operation.
type AcceptedMemoryRuntime interface {
	ApplyAccepted(context.Context, domain.Completion) error
}

type Processor struct {
	ledger    Ledger
	manifests ManifestReader
	runtime   RuntimeFactory
	worker    string
	maxActive int
	observer  Observer
}

func NewProcessor(ledger Ledger, manifests ManifestReader, runtime RuntimeFactory, worker string, maxActive int, observers ...Observer) (*Processor, error) {
	if ledger == nil || manifests == nil || runtime == nil || worker == "" || maxActive < 1 || len(observers) > 1 {
		return nil, domain.ErrInvalid
	}
	p := &Processor{ledger: ledger, manifests: manifests, runtime: runtime, worker: worker, maxActive: maxActive}
	if len(observers) == 1 {
		p.observer = observers[0]
	}
	return p, nil
}

// Advance performs one bounded attempt. Intake and retry scheduling remain
// durable ledger facts, so a process exit never loses an accepted Run.
func (p *Processor) Advance(ctx context.Context, r domain.Run) (advanceErr error) {
	advanceStart := time.Now()
	defer func() { p.observe(ctx, "advance", r, "", advanceStart, advanceErr) }()
	terminalStart := time.Now()
	if terminal, err := p.ledger.Terminalize(ctx, r.Request.Route.TenantID, r.Request.RunID, ""); err != nil || terminal {
		p.observe(ctx, "terminalize", r, "", terminalStart, err)
		return err
	}
	start := time.Now()
	plan, err := p.manifests.Resolve(ctx, r.Request.Route)
	p.observe(ctx, "manifest", r, "", start, err)
	if err != nil {
		if errors.Is(err, ErrManifestInvalid) || errors.Is(err, ErrManifestUnsupported) {
			reason := "MANIFEST_INVALID"
			if errors.Is(err, ErrManifestUnsupported) {
				reason = "UNSUPPORTED_MANIFEST"
			}
			_, e := p.ledger.Terminalize(ctx, r.Request.Route.TenantID, r.Request.RunID, reason)
			return e
		}
		return err
	}
	if plan.TenantID != r.Request.Route.TenantID || plan.ManifestID != r.Request.Route.ManifestRef || plan.ManifestDigest != r.Request.Route.ManifestDigest || plan.DeploymentRevisionID != r.Request.Route.DeploymentRevisionID {
		_, err = p.ledger.Terminalize(ctx, r.Request.Route.TenantID, r.Request.RunID, "MANIFEST_INVALID")
		return err
	}
	start = time.Now()
	g, err := p.ledger.Claim(ctx, domain.ClaimRequest{TenantID: r.Request.Route.TenantID, RunID: r.Request.RunID, WorkerID: p.worker, MaxRunSeconds: plan.MaxRunSeconds, MaxActive: p.maxActive})
	p.observe(ctx, "claim", r, g.AttemptID, start, err)
	if err != nil {
		return err
	}
	if g.Run.ExecutionDeadline == nil {
		return domain.ErrInvalid
	}
	attemptCtx, cancel := context.WithDeadline(ctx, *g.Run.ExecutionDeadline)
	defer cancel()
	usageRecorder, _ := p.ledger.(interface {
		RecordUsage(context.Context, domain.Grant, domain.ModelUsage) error
	})
	usageSettled, modelStarted := false, false
	settleUsage := func(usage domain.ModelUsage) error {
		if usageSettled || usageRecorder == nil {
			return nil
		}
		if err := usageRecorder.RecordUsage(attemptCtx, g, usage); err != nil {
			return err
		}
		usageSettled = true
		return nil
	}
	stopRenew := make(chan struct{})
	renewDone := make(chan struct{})
	var renewMu sync.Mutex
	var renewErr error
	go func() {
		defer close(renewDone)
		timer := time.NewTicker(g.Run.Policy.RenewalInterval)
		defer timer.Stop()
		for {
			select {
			case <-stopRenew:
				return
			case <-attemptCtx.Done():
				return
			case <-timer.C:
				renewStart := time.Now()
				_, e := p.ledger.Renew(attemptCtx, g)
				p.observe(attemptCtx, "renew", r, g.AttemptID, renewStart, e)
				if e != nil {
					renewMu.Lock()
					renewErr = e
					renewMu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	var stopRenewOnce sync.Once
	stopRenewal := func() { stopRenewOnce.Do(func() { close(stopRenew) }); <-renewDone }
	defer stopRenewal()
	check := func(ctx context.Context) error {
		start := time.Now()
		err := p.ledger.Check(ctx, g)
		p.observe(ctx, "fence", r, g.AttemptID, start, err)
		return err
	}
	fail := func(cause error) error {
		if !modelStarted {
			if e := settleUsage(domain.ModelUsage{Known: true}); e != nil {
				return e
			}
		}
		renewMu.Lock()
		lost := renewErr
		renewMu.Unlock()
		if lost != nil || errors.Is(cause, domain.ErrFenced) {
			return domain.ErrFenced
		}
		// A canceled/deadline attempt loses eligibility through the same durable
		// lease; recovery does not release it early while external calls may run.
		if attemptCtx.Err() != nil {
			return attemptCtx.Err()
		}
		reason, retry := "RUNTIME_FAILED", false
		switch {
		case errors.Is(cause, ErrSessionPreparation):
			reason, retry = "SESSION_PREPARATION_REQUIRED", true
		case errors.Is(cause, ErrDependency):
			reason, retry = "DEPENDENCY_UNAVAILABLE", true
		case errors.Is(cause, ErrCredentialDenied):
			reason = "CREDENTIAL_DENIED"
		case errors.Is(cause, ErrSessionInvalid):
			reason = "SESSION_INVALID"
		}
		if e := p.ledger.FailAttempt(attemptCtx, g, reason, retry); e != nil {
			return e
		}
		return cause
	}
	start = time.Now()
	runtime, err := p.runtime.Prepare(attemptCtx, g, plan, check)
	p.observe(attemptCtx, "prepare", r, g.AttemptID, start, err)
	if err != nil {
		return fail(err)
	}
	defer runtime.Close()
	if err = check(attemptCtx); err != nil {
		return fail(err)
	}
	start = time.Now()
	history, err := runtime.Load(attemptCtx, g.Parent)
	p.observe(attemptCtx, "session_load", r, g.AttemptID, start, err)
	if err != nil {
		return fail(err)
	}
	if err = p.ledger.MarkExecuting(attemptCtx, g); err != nil {
		return fail(err)
	}
	start = time.Now()
	modelStarted = true
	result, err := runtime.Execute(attemptCtx, history)
	p.observe(attemptCtx, "execute", r, g.AttemptID, start, err)
	usage := domain.ModelUsage{Known: err == nil && result.UsageKnown, InputTokens: result.InputTokens, OutputTokens: result.OutputTokens, TotalTokens: result.TotalTokens}
	if !usage.Known {
		usage.InputTokens, usage.OutputTokens, usage.TotalTokens = 0, 0, 0
	}
	if usageErr := settleUsage(usage); usageErr != nil {
		return fail(usageErr)
	}
	if err == nil && p.observer != nil && (result.InputTokens > 0 || result.OutputTokens > 0 || result.TotalTokens > 0) {
		p.observer.Observe(attemptCtx, Observation{Operation: "usage", Result: "ok", TenantID: r.Request.Route.TenantID, RunID: r.Request.RunID, AttemptID: g.AttemptID, InputTokens: result.InputTokens, OutputTokens: result.OutputTokens, TotalTokens: result.TotalTokens})
	}
	if err != nil {
		return fail(err)
	}
	if len(result.Attachments) > 0 {
		if _, _, err = EncodeFinalIntent(g.Run, g.AttemptID, g.Generation, result.FinalText, result.Attachments); err != nil {
			return fail(ErrRuntimeFailed)
		}
	}
	var memoryRuntime AcceptedMemoryRuntime
	if result.MemoryDigest != "" {
		var ok bool
		memoryRuntime, ok = runtime.(AcceptedMemoryRuntime)
		if !ok || !domain.DigestValid(result.MemoryDigest) || result.MemoryTimeout <= 0 {
			return fail(ErrRuntimeFailed)
		}
	}
	if err = check(attemptCtx); err != nil {
		return fail(err)
	}
	start = time.Now()
	candidate, err := runtime.Stage(attemptCtx, result.Snapshot)
	p.observe(attemptCtx, "session_stage", r, g.AttemptID, start, err)
	if err != nil {
		return fail(err)
	}
	finish := domain.Finish{Grant: g, Status: domain.Succeeded, Candidate: candidate, FinalText: result.FinalText, MemoryDigest: result.MemoryDigest, Attachments: append([]domain.Attachment(nil), result.Attachments...)}
	accepted := func(c domain.Completion) error {
		if memoryRuntime == nil {
			return nil
		}
		if c.TenantID != r.Request.Route.TenantID || c.RunID != r.Request.RunID || c.AttemptID != g.AttemptID || c.Kind != "ATTEMPT" || c.Status != domain.Succeeded || c.ResultDigest != domain.FinishDigest(finish) || c.MemoryDigest != result.MemoryDigest || c.Candidate != (domain.Head{Ref: candidate.Ref, Digest: candidate.Digest}) {
			return domain.ErrFenced
		}
		// Complete has ended the Attempt. Renewal/check now reject it by design;
		// synchronous Memory application uses the accepted identity, not that lease.
		stopRenewal()
		applyCtx, applyCancel := context.WithTimeout(ctx, result.MemoryTimeout)
		applyStart := time.Now()
		applyErr := memoryRuntime.ApplyAccepted(applyCtx, c)
		p.observe(applyCtx, "memory_apply", r, g.AttemptID, applyStart, applyErr)
		applyCancel()
		// Use a fresh bounded child of the caller for the visibility decision: an
		// expired backend call must still be able to release a fixed failure Final.
		finalCtx, finalCancel := context.WithTimeout(ctx, result.MemoryTimeout)
		defer finalCancel()
		finalStart := time.Now()
		finalErr := p.ledger.FinalizeMemory(finalCtx, c, applyErr == nil)
		if finalErr != nil && finalCtx.Err() == nil && !errors.Is(finalErr, domain.ErrConflict) && !errors.Is(finalErr, domain.ErrInvalid) {
			// A local commit response can be lost. Repeat only the identical decision.
			finalErr = p.ledger.FinalizeMemory(finalCtx, c, applyErr == nil)
		}
		p.observe(finalCtx, "memory_finalize", r, g.AttemptID, finalStart, finalErr)
		if applyErr != nil {
			if finalErr != nil {
				return errors.Join(ErrMemoryApply, ErrMemoryFinalize)
			}
			return ErrMemoryApply
		}
		if finalErr != nil {
			return ErrMemoryFinalize
		}
		return nil
	}
	start = time.Now()
	completed, err := p.ledger.Complete(attemptCtx, finish)
	p.observe(attemptCtx, "complete", r, g.AttemptID, start, err)
	if err == nil {
		return accepted(completed)
	}
	// Query stable identity after uncertain Completion before attempting any
	// other state change. A retry uses the identical in-memory candidate/result.
	proofCtx, proofCancel := context.WithTimeout(context.WithoutCancel(ctx), g.Run.Policy.LeaseTTL)
	defer proofCancel()
	if committed, e := p.ledger.FindCompletion(proofCtx, r.Request.Route.TenantID, r.Request.RunID); e == nil {
		if committed.AttemptID == g.AttemptID && committed.Status == domain.Succeeded && committed.ResultDigest == domain.FinishDigest(finish) {
			return accepted(committed)
		}
		return domain.ErrFenced
	}
	if errors.Is(err, domain.ErrFenced) || errors.Is(err, domain.ErrConflict) {
		return err
	}
	if e := check(attemptCtx); e != nil {
		return e
	}
	start = time.Now()
	completed, err = p.ledger.Complete(attemptCtx, finish)
	p.observe(attemptCtx, "complete", r, g.AttemptID, start, err)
	if err != nil {
		return err
	}
	return accepted(completed)
}
