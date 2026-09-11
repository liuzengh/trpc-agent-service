// Package application owns account connection lifecycles behind the Supervisor
// use case. Protocol, credential storage, and lease persistence are injected ports.
package application

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type LeaseStore interface {
	ApplyAndAcquire(context.Context, domain.Account, string, time.Duration) (domain.OwnerGrant, error)
	Renew(context.Context, domain.OwnerGrant, time.Duration) (domain.OwnerGrant, error)
	Release(context.Context, domain.OwnerGrant) error
	Check(context.Context, domain.OwnerGrant) error
	MarkReplaced(context.Context, domain.OwnerGrant) error
}
type AccountSource interface {
	List(context.Context) ([]domain.Account, error)
}
type CredentialMaterial struct{ Secret string }
type CredentialResolver interface {
	Resolve(context.Context, domain.Account) (CredentialMaterial, error)
}

// OwnedCredentialResolver is used by Control-backed composition. The grant is
// the live lease checked by Supervisor, never reconstructed from status.
type OwnedCredentialResolver interface {
	ResolveOwned(context.Context, domain.Account, domain.OwnerGrant) (CredentialMaterial, error)
}
type ClientFactory interface {
	New(context.Context, domain.Account, domain.OwnerGrant, CredentialMaterial) (Client, error)
}

// Ports must honor context deadlines. Quiesce and Status must be nonblocking.
// Run is called once per Client. Close must stop protocol work; returning nil does
// not excuse a still-running Run, which the supervisor verifies before release.
type Client interface {
	Run(context.Context) error
	Quiesce()
	Drain(context.Context) error
	Close(context.Context) error
	Status() ClientStatus
}
type ClientStatus struct {
	Ready    bool
	Replaced bool
	Terminal bool
	// Retryable is classified by the provider Adapter, never inferred from a
	// raw Run error. It is consulted only for terminal/done Clients. Replaced
	// always wins. Authentication/protocol/SDK transport-budget exhaustion
	// remain false; a temporary Admission retry budget is classified separately.
	Retryable bool
}
type Options struct {
	// CredentialResolveTimeout is independent of the short lease-store budget.
	CredentialResolveTimeout                               time.Duration
	InstanceID                                             string
	LeaseTTL, PollInterval, OperationTimeout, DrainTimeout time.Duration
	MaxAccounts                                            int
	// Same-revision temporary Client failures use bounded restart slots. These
	// limits are per Supervisor process, not a durable/global retry budget.
	RestartBackoff, RestartCooldown time.Duration
	MaxRestarts                     int
}
type Phase string

const (
	PhasePending   Phase = "PENDING"
	PhaseAcquiring Phase = "ACQUIRING"
	PhaseStandby   Phase = "STANDBY"
	PhaseResolving Phase = "RESOLVING"
	PhaseStarting  Phase = "STARTING"
	PhaseReady     Phase = "READY"
	PhaseDraining  Phase = "DRAINING"
	PhaseDisabled  Phase = "DISABLED"
	PhaseBlocked   Phase = "BLOCKED"
	PhaseFailed    Phase = "FAILED"
	PhaseStopped   Phase = "STOPPED"
	PhaseBackoff   Phase = "BACKOFF"
	PhaseCooldown  Phase = "COOLDOWN"
)

type Reason string

const (
	ReasonNone       Reason = ""
	ReasonSource     Reason = "source_unavailable"
	ReasonInvalid    Reason = "invalid_configuration"
	ReasonMissing    Reason = "account_removed"
	ReasonHeld       Reason = "held_by_other_owner"
	ReasonDisabled   Reason = "disabled"
	ReasonStale      Reason = "stale_revision"
	ReasonConflict   Reason = "revision_conflict"
	ReasonReplaced   Reason = "provider_replaced"
	ReasonAcquire    Reason = "acquire_failed"
	ReasonCredential Reason = "credential_unavailable"
	ReasonFactory    Reason = "client_creation_failed"
	ReasonLost       Reason = "lease_lost"
	ReasonClient     Reason = "client_stopped"
	ReasonRetryable  Reason = "temporary_client_failure"
	ReasonCooldown   Reason = "restart_cooldown"
	ReasonDrain      Reason = "drain_failed"
	ReasonClose      Reason = "close_failed"
	ReasonRelease    Reason = "release_failed"
	ReasonShutdown   Reason = "shutdown"
)

type Status struct {
	AccountID, BotID string
	Revision, Epoch  int64
	Phase            Phase
	Reason           Reason
	Ready, Owned     bool
	LeaseUntil       time.Time
	RestartAttempts  int
	NextRetryAt      time.Time
	// Both false means no replacement-isolation write was attempted for this
	// status revision. These survive a separate Close failure.
	IsolationPersisted bool
	IsolationError     bool
}

var (
	ErrInvalid    = errors.New("connection supervisor: invalid configuration")
	ErrAlreadyRun = errors.New("connection supervisor: Run already started")
	ErrCleanup    = errors.New("connection supervisor: cleanup incomplete")
)

func normalizeOptions(o Options) (Options, error) {
	if o.CredentialResolveTimeout == 0 {
		o.CredentialResolveTimeout = 5 * time.Second
	}
	if o.CredentialResolveTimeout < time.Millisecond || o.CredentialResolveTimeout > 5*time.Second {
		return o, ErrInvalid
	}
	if o.LeaseTTL == 0 {
		o.LeaseTTL = 15 * time.Second
	}
	if o.PollInterval == 0 {
		o.PollInterval = time.Second
	}
	if o.OperationTimeout == 0 {
		o.OperationTimeout = 2 * time.Second
	}
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 5 * time.Second
	}
	if o.MaxAccounts == 0 {
		o.MaxAccounts = 100
	}
	if o.RestartBackoff == 0 {
		o.RestartBackoff = time.Second
	}
	if o.RestartCooldown == 0 {
		o.RestartCooldown = time.Minute
	}
	if o.MaxRestarts == 0 {
		o.MaxRestarts = 3
	}
	if o.RestartBackoff < time.Millisecond || o.RestartCooldown < o.RestartBackoff || o.RestartCooldown > time.Hour || o.MaxRestarts < 1 || o.MaxRestarts > 10 {
		return o, ErrInvalid
	}
	if domain.ValidateInstanceID(o.InstanceID) != nil || domain.ValidateLeaseTTL(o.LeaseTTL) != nil || o.PollInterval < time.Millisecond || o.PollInterval > time.Minute || o.OperationTimeout < time.Millisecond || o.OperationTimeout >= o.LeaseTTL/3 || o.DrainTimeout < time.Millisecond || o.DrainTimeout > time.Minute || o.MaxAccounts < 1 || o.MaxAccounts > 10000 {
		return o, ErrInvalid
	}
	return o, nil
}
