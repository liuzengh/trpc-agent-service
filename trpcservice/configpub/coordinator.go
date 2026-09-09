package configpub

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Telemetry is the narrow, injected observability seam. It is satisfied by a
// small adapter over the P1-07 telemetry runtime; a nil Telemetry disables
// observability without changing any business outcome.
type Telemetry interface {
	StartConfigSpan(ctx context.Context, operation string) (context.Context, func(outcome string))
	ConfigOperation(operation, outcome string)
	Event(ctx context.Context, level int, event, component, operation, outcome string, fields ...string)
}

// Clock allows deterministic tests; production uses the real clock.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// Coordinator exposes the publication operations. Every operation is
// tenant-scoped by the validated TenantContext, idempotent by a
// caller-supplied operation id that the server scopes per tenant, and
// protected by PostgreSQL CAS against stale expected versions.
type Coordinator struct {
	repo      Repository
	validator Validator
	telemetry Telemetry
	clock     Clock
}

// NewCoordinator fails closed without a repository.
func NewCoordinator(repo Repository, checker BindingChecker, telemetry Telemetry) (*Coordinator, error) {
	if repo == nil {
		return nil, ErrInvalidArgument
	}
	return &Coordinator{repo: repo, validator: Validator{Bindings: checker}, telemetry: telemetry}, nil
}

// actorCategory bounds the server-owned actor categories.
func validActorCategory(actor string) bool {
	switch actor {
	case "operator", "service", "system":
		return true
	default:
		return false
	}
}

func validOperationID(id string) bool {
	return len(id) >= 8 && len(id) <= 128 && validID(id)
}

func (c *Coordinator) tenantOf(tc tenant.TenantContext) (string, error) {
	if tc.Validate() != nil {
		return "", ErrTenantMismatch
	}
	return tc.TenantID, nil
}

func (c *Coordinator) observe(ctx context.Context, operation string) (context.Context, func(string)) {
	if c.telemetry == nil {
		return ctx, func(string) {}
	}
	return c.telemetry.StartConfigSpan(ctx, operation)
}

func (c *Coordinator) metric(operation, outcome string) {
	if c.telemetry != nil {
		c.telemetry.ConfigOperation(operation, outcome)
	}
}

func (c *Coordinator) logEvent(ctx context.Context, event, operation, outcome string, fields ...string) {
	if c.telemetry != nil {
		// Level 4 (info); the adapter owns level mapping.
		c.telemetry.Event(ctx, 4, event, "configpub", operation, outcome, fields...)
	}
}

// CreateRevisionInput carries a create request.
type CreateRevisionInput struct {
	Version       int64
	Document      Document
	ActorCategory string
}

// CreateRevision stores a new immutable draft revision.
func (c *Coordinator) CreateRevision(ctx context.Context, tc tenant.TenantContext, in CreateRevisionInput) (Revision, error) {
	if in.Version < 1 || !validActorCategory(in.ActorCategory) {
		return Revision{}, ErrInvalidArgument
	}
	doc, actor := in.Document, in.ActorCategory
	if _, err := c.tenantOf(tc); err != nil {
		return Revision{}, err
	}
	ctx, endSpan := c.observe(ctx, "config_create")
	outcome := "committed"
	defer func() { endSpan(outcome); c.metric("create", outcome) }()
	revision, err := c.repo.CreateRevision(ctx, tc, in.Version, doc, actor, c.now())
	if err != nil {
		outcome = outcomeOf(err)
		c.logEvent(ctx, "config_revision_create", "create", outcome, "error_category", safeCategory(err))
		return Revision{}, err
	}
	return revision, nil
}

// ValidateRevision runs semantic validation and moves draft -> validated or
// draft -> rejected with a bounded reason category.
func (c *Coordinator) ValidateRevision(ctx context.Context, tc tenant.TenantContext, version int64, actor string) (Revision, error) {
	if _, err := c.tenantOf(tc); err != nil {
		return Revision{}, err
	}
	if version < 1 || !validActorCategory(actor) {
		return Revision{}, ErrInvalidArgument
	}
	ctx, endSpan := c.observe(ctx, "config_validate")
	outcome := "committed"
	defer func() { endSpan(outcome); c.metric("validate", outcome) }()
	revision, err := c.repo.ValidateRevision(ctx, tc, version, c.validator, actor, c.now())
	if err != nil {
		outcome = outcomeOf(err)
		c.logEvent(ctx, "config_revision_validate", "validate", outcome, "error_category", safeCategory(err), "reason_category", CategoryOf(err))
		return Revision{}, err
	}
	if revision.Status == StatusRejected {
		outcome = "rejected"
	}
	return revision, nil
}

// PublishInput carries a publish request. ExpectedActiveVersion is the
// caller's view of the current active revision (0 when none); a mismatch is a
// stale-version rejection, never a silent overwrite.
type PublishInput struct {
	Version               int64
	ExpectedActiveVersion int64
	Percentage            int
	OperationID           string
	ActorCategory         string
}

// Publish activates a validated revision under CAS with canary percentage.
func (c *Coordinator) Publish(ctx context.Context, tc tenant.TenantContext, in PublishInput) (OperationRecord, error) {
	tenantID, err := c.tenantOf(tc)
	if err != nil {
		return OperationRecord{}, err
	}
	if in.Version < 1 || in.ExpectedActiveVersion < 0 || in.Percentage < 0 || in.Percentage > 100 ||
		!validActorCategory(in.ActorCategory) || !validOperationID(in.OperationID) {
		return OperationRecord{}, ErrInvalidArgument
	}
	ctx, endSpan := c.observe(ctx, "config_publish")
	outcome := OutcomeCommitted
	defer func() { endSpan(outcome); c.metric("publish", outcome) }()

	if record, ok, err := c.repo.Operation(ctx, tenantID, in.OperationID); err != nil {
		outcome = outcomeOf(err)
		return OperationRecord{}, err
	} else if ok {
		if !sameOperation(record, OperationRecord{Kind: OperationPublish, TargetVersion: in.Version, ExpectedActiveVersion: in.ExpectedActiveVersion, RequestedPercentage: in.Percentage, ActorCategory: in.ActorCategory}) {
			return OperationRecord{}, ErrOperationMismatch
		}
		return record, nil // committed operation replay: idempotent convergence
	}

	record := OperationRecord{
		TenantID: tenantID, OperationID: in.OperationID, Kind: OperationPublish,
		TargetVersion: in.Version, ExpectedActiveVersion: in.ExpectedActiveVersion,
		RequestedPercentage: in.Percentage,
		ActorCategory:       in.ActorCategory,
	}
	if in.Percentage < 0 || in.Percentage > 100 {
		return OperationRecord{}, ErrInvalidRollout
	}
	committed, err := c.repo.Publish(ctx, tc, record, in.Percentage, c.now())
	if err != nil {
		if replay, replayErr := c.committedReplay(ctx, tenantID, record); replayErr != nil {
			return OperationRecord{}, replayErr
		} else if replay != nil {
			return *replay, nil
		}
		outcome = outcomeOf(err)
		c.logEvent(ctx, "config_publish", "publish", outcome, "error_category", safeCategory(err), "reason_category", reasonOf(err))
		return OperationRecord{}, err
	}
	return committed, nil
}

// RolloutInput updates the canary percentage of the currently active revision.
type RolloutInput struct {
	Version               int64
	Percentage            int
	ExpectedActiveVersion int64
	OperationID           string
	ActorCategory         string
}

// SetRollout moves the canary percentage for an already active revision.
func (c *Coordinator) SetRollout(ctx context.Context, tc tenant.TenantContext, in RolloutInput) (OperationRecord, error) {
	tenantID, err := c.tenantOf(tc)
	if err != nil {
		return OperationRecord{}, err
	}
	if in.Version < 1 || in.ExpectedActiveVersion < 0 || in.Percentage < 0 || in.Percentage > 100 ||
		!validActorCategory(in.ActorCategory) || !validOperationID(in.OperationID) {
		return OperationRecord{}, ErrInvalidArgument
	}
	ctx, endSpan := c.observe(ctx, "config_rollout")
	outcome := OutcomeCommitted
	defer func() { endSpan(outcome); c.metric("rollout", outcome) }()

	if record, ok, err := c.repo.Operation(ctx, tenantID, in.OperationID); err != nil {
		outcome = outcomeOf(err)
		return OperationRecord{}, err
	} else if ok {
		if !sameOperation(record, OperationRecord{Kind: OperationRollout, TargetVersion: in.Version, ExpectedActiveVersion: in.ExpectedActiveVersion, RequestedPercentage: in.Percentage, ActorCategory: in.ActorCategory}) {
			return OperationRecord{}, ErrOperationMismatch
		}
		return record, nil
	}

	record := OperationRecord{
		TenantID: tenantID, OperationID: in.OperationID, Kind: OperationRollout,
		TargetVersion: in.Version, ExpectedActiveVersion: in.ExpectedActiveVersion,
		RequestedPercentage: in.Percentage,
		ActorCategory:       in.ActorCategory,
	}
	committed, err := c.repo.SetRollout(ctx, tc, record, in.Percentage, c.now())
	if err != nil {
		if replay, replayErr := c.committedReplay(ctx, tenantID, record); replayErr != nil {
			return OperationRecord{}, replayErr
		} else if replay != nil {
			return *replay, nil
		}
		outcome = outcomeOf(err)
		c.logEvent(ctx, "config_rollout", "rollout", outcome, "error_category", safeCategory(err), "reason_category", reasonOf(err))
		return OperationRecord{}, err
	}
	return committed, nil
}

// RollbackInput recalls the active revision and re-activates a previously
// published target. Rollback only affects new requests.
type RollbackInput struct {
	TargetVersion         int64
	ExpectedActiveVersion int64
	OperationID           string
	ActorCategory         string
}

// Rollback performs the durable rollback under CAS.
func (c *Coordinator) Rollback(ctx context.Context, tc tenant.TenantContext, in RollbackInput) (OperationRecord, error) {
	tenantID, err := c.tenantOf(tc)
	if err != nil {
		return OperationRecord{}, err
	}
	if in.TargetVersion < 1 || in.ExpectedActiveVersion < 0 || !validActorCategory(in.ActorCategory) || !validOperationID(in.OperationID) {
		return OperationRecord{}, ErrInvalidArgument
	}
	ctx, endSpan := c.observe(ctx, "config_rollback")
	outcome := OutcomeCommitted
	defer func() { endSpan(outcome); c.metric("rollback", outcome) }()

	if record, ok, err := c.repo.Operation(ctx, tenantID, in.OperationID); err != nil {
		outcome = outcomeOf(err)
		return OperationRecord{}, err
	} else if ok {
		if !sameOperation(record, OperationRecord{Kind: OperationRollback, TargetVersion: in.TargetVersion, ExpectedActiveVersion: in.ExpectedActiveVersion, RequestedPercentage: 100, ActorCategory: in.ActorCategory}) {
			return OperationRecord{}, ErrOperationMismatch
		}
		return record, nil
	}

	record := OperationRecord{
		TenantID: tenantID, OperationID: in.OperationID, Kind: OperationRollback,
		TargetVersion: in.TargetVersion, ExpectedActiveVersion: in.ExpectedActiveVersion,
		RequestedPercentage: 100,
		ActorCategory:       in.ActorCategory,
	}
	committed, err := c.repo.Rollback(ctx, tc, record, c.now())
	if err != nil {
		if replay, replayErr := c.committedReplay(ctx, tenantID, record); replayErr != nil {
			return OperationRecord{}, replayErr
		} else if replay != nil {
			return *replay, nil
		}
		outcome = outcomeOf(err)
		c.logEvent(ctx, "config_rollback", "rollback", outcome, "error_category", safeCategory(err), "reason_category", reasonOf(err))
		return OperationRecord{}, err
	}
	return committed, nil
}

func sameOperation(got, want OperationRecord) bool {
	return got.Kind == want.Kind && got.TargetVersion == want.TargetVersion &&
		got.ExpectedActiveVersion == want.ExpectedActiveVersion &&
		got.RequestedPercentage == want.RequestedPercentage &&
		got.ActorCategory == want.ActorCategory
}

// committedReplay closes the small race between the initial idempotency read
// and the repository transaction. A concurrent identical operation may commit
// first; retrying it must converge to that durable result rather than turning
// the winner's response into a spurious state error.
func (c *Coordinator) committedReplay(ctx context.Context, tenantID string, want OperationRecord) (*OperationRecord, error) {
	record, ok, err := c.repo.Operation(ctx, tenantID, want.OperationID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	if !sameOperation(record, want) {
		return nil, ErrOperationMismatch
	}
	return &record, nil
}

// RolloutState returns the durable assignment state for a tenant.
func (c *Coordinator) RolloutState(ctx context.Context, tenantID string) (RolloutState, bool, error) {
	if !validID(tenantID) {
		return RolloutState{}, false, ErrInvalidArgument
	}
	ctx, endSpan := c.observe(ctx, "config_read")
	outcome := "hit"
	defer func() { endSpan(outcome); c.metric("read", outcome) }()
	state, managed, err := c.repo.Rollout(ctx, tenantID)
	if err != nil {
		outcome = outcomeOf(err)
		return RolloutState{}, false, err
	}
	if !managed {
		outcome = "unmanaged"
	}
	return state, managed, nil
}

// Snapshot reads a runtime-readable revision snapshot for a job's immutable
// config version. Draft and rejected revisions never serve runtime traffic.
func (c *Coordinator) Snapshot(ctx context.Context, tenantID string, version int64) (Revision, error) {
	if !validID(tenantID) || version < 1 {
		return Revision{}, ErrInvalidArgument
	}
	ctx, endSpan := c.observe(ctx, "config_read")
	outcome := "hit"
	defer func() { endSpan(outcome); c.metric("read", outcome) }()
	revision, err := c.repo.Revision(ctx, tenantID, version)
	if err != nil {
		outcome = outcomeOf(err)
		return Revision{}, err
	}
	if !revision.Status.RuntimeReadable() {
		outcome = "not_readable"
		return Revision{}, ErrRevisionState
	}
	return revision, nil
}

func (c *Coordinator) now() time.Time {
	if c.clock != nil {
		return c.clock.Now()
	}
	return time.Now().UTC()
}

// outcomeOf maps a failure to a bounded metric outcome. Unavailable class
// failures never become "committed".
func outcomeOf(err error) string {
	switch {
	case err == nil:
		return OutcomeCommitted
	case isUnavailable(err):
		return "unavailable"
	default:
		return "failed"
	}
}

// safeCategory returns a bounded error category for logs. It never embeds raw
// backend error text.
func safeCategory(err error) string {
	if err == nil {
		return ""
	}
	if isUnavailable(err) {
		return "unavailable"
	}
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return "invalid_argument"
	case errors.Is(err, ErrTenantMismatch):
		return "tenant_mismatch"
	case errors.Is(err, ErrRevisionNotFound):
		return "revision_not_found"
	case errors.Is(err, ErrRevisionState):
		return "revision_state"
	case errors.Is(err, ErrStaleExpectedVersion):
		return "stale_expected_version"
	case errors.Is(err, ErrDuplicateContent):
		return "duplicate_content"
	case errors.Is(err, ErrInvalidRollout):
		return "invalid_rollout"
	case errors.Is(err, ErrOperationMismatch):
		return "operation_mismatch"
	case errors.Is(err, ErrNotManaged):
		return "not_managed"
	default:
		return "failed"
	}
}

func reasonOf(err error) string {
	if CategoryOf(err) != "" {
		return CategoryOf(err)
	}
	switch {
	case errors.Is(err, ErrRevisionNotFound):
		return ReasonMissingRevision
	case errors.Is(err, ErrRevisionState):
		return ReasonWrongStatus
	case errors.Is(err, ErrStaleExpectedVersion):
		return ReasonStaleVersion
	case errors.Is(err, ErrDuplicateContent):
		return ReasonDuplicateContent
	case errors.Is(err, ErrInvalidRollout):
		return ReasonInvalidRollout
	case errors.Is(err, ErrTenantMismatch):
		return ReasonTenantMismatch
	default:
		return ""
	}
}
