package application

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type Supervisor struct {
	store                     LeaseStore
	source                    AccountSource
	resolver                  CredentialResolver
	factory                   ClientFactory
	options                   Options
	mu                        sync.Mutex
	started, running, healthy bool
	records                   map[string]*accountRecord
	wg                        sync.WaitGroup
	cleanupFailed             bool
	runContext                context.Context
}
type accountRecord struct {
	preparingCancel    context.CancelFunc
	preparingAccount   domain.Account
	preparingID        uint64
	supervisor         *Supervisor
	account            domain.Account
	present            bool
	sourceReason       Reason
	blockedRevision    int64
	blockedReason      Reason
	fatal              bool
	status             Status
	wake               chan struct{}
	lease              *localLease
	retryRevision      int64
	restartAttempts    int
	nextRetry          time.Time
	cooling            bool
	isolationRevision  int64
	isolationPersisted bool
	isolationError     bool
}

func NewSupervisor(store LeaseStore, source AccountSource, resolver CredentialResolver, factory ClientFactory, options Options) (*Supervisor, error) {
	if store == nil || source == nil || resolver == nil || factory == nil {
		return nil, ErrInvalid
	}
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return &Supervisor{store: store, source: source, resolver: resolver, factory: factory, options: options, records: make(map[string]*accountRecord)}, nil
}
func (s *Supervisor) Status(id string) (Status, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[id]
	if !ok {
		return Status{}, false
	}
	result := r.status
	if r.isolationRevision == result.Revision {
		result.IsolationPersisted = r.isolationPersisted
		result.IsolationError = r.isolationError
	}
	if !s.running || (s.runContext != nil && s.runContext.Err() != nil) || !s.healthy || !r.present || r.sourceReason != "" || r.blockedRevision >= r.account.Revision || result.Revision != r.account.Revision {
		result.Ready = false
	}
	if result.Owned && (r.lease == nil || !r.lease.valid()) {
		result.Ready = false
	}
	return result, true
}

// Ready reports the Supervisor's shared control loop, not the health of every
// bot. A complete valid source snapshot is required; a failed/blocked/not-yet-
// authenticated account remains visible through Status without withdrawing the
// whole Gateway. Process/database/routing health remains owned by bootstrap.
func (s *Supervisor) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running && s.runContext != nil && s.runContext.Err() == nil && s.healthy
}

// Run is a single lifecycle. Its cancellation removes readiness immediately,
// then independently drains each account while keeping its lease alive.
func (s *Supervisor) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return ErrAlreadyRun
	}
	s.started = true
	s.running = true
	s.runContext = ctx
	s.mu.Unlock()
	for {
		op, cancel := context.WithTimeout(ctx, s.options.OperationTimeout)
		accounts, err := s.source.List(op)
		cancel()
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			s.invalidate(ReasonSource)
		} else {
			s.applySnapshot(ctx, accounts)
		}
		timer := time.NewTimer(s.options.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			break
		}
	}
	s.mu.Lock()
	s.running = false
	s.healthy = false
	s.mu.Unlock()
	s.wg.Wait()
	s.mu.Lock()
	failed := s.cleanupFailed
	s.mu.Unlock()
	if failed {
		return ErrCleanup
	}
	return nil
}
func (s *Supervisor) invalidate(reason Reason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthy = false
	for _, r := range s.records {
		r.sourceReason = reason
		r.signal()
	}
}
func (s *Supervisor) applySnapshot(ctx context.Context, accounts []domain.Account) {
	if len(accounts) > s.options.MaxAccounts {
		s.invalidate(ReasonInvalid)
		return
	}
	byID := make(map[string]domain.Account, len(accounts))
	bots := make(map[string]string, len(accounts))
	for _, a := range accounts {
		if a.Validate() != nil {
			s.invalidate(ReasonInvalid)
			return
		}
		if _, exists := byID[a.ID]; exists {
			s.invalidate(ReasonConflict)
			return
		}
		if prior, exists := bots[a.BotID]; exists && prior != a.ID && a.Enabled && byID[prior].Enabled {
			s.invalidate(ReasonConflict)
			return
		}
		byID[a.ID] = a
		if a.Enabled {
			bots[a.BotID] = a.ID
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	newIDs := 0
	for id := range byID {
		if s.records[id] == nil {
			newIDs++
		}
	}
	if len(s.records)+newIDs > s.options.MaxAccounts {
		s.healthy = false
		for _, r := range s.records {
			r.sourceReason = ReasonInvalid
			r.signal()
		}
		return
	}
	s.healthy = true
	for id, r := range s.records {
		if _, exists := byID[id]; !exists {
			r.present = false
			r.sourceReason = ReasonMissing
			r.signal()
		}
	}
	for id, a := range byID {
		r := s.records[id]
		if r == nil {
			r = &accountRecord{supervisor: s, account: a, present: true, wake: make(chan struct{}, 1), status: Status{AccountID: id, BotID: a.BotID, Revision: a.Revision, Phase: PhasePending}}
			s.records[id] = r
			s.wg.Add(1)
			go func() { defer s.wg.Done(); r.run(ctx) }()
			continue
		}
		r.present = true
		r.sourceReason = ""
		switch {
		case a.Revision < r.account.Revision:
			r.blockedRevision = max(r.blockedRevision, r.account.Revision)
			r.blockedReason = ReasonStale
		case a.BotID != r.account.BotID:
			r.blockedRevision = max(r.blockedRevision, a.Revision)
			r.blockedReason = ReasonConflict
		case a.Revision == r.account.Revision && a != r.account:
			r.blockedRevision = max(r.blockedRevision, a.Revision)
			r.blockedReason = ReasonConflict
		case a.Revision > r.account.Revision:
			r.account = a
		}
		r.signal()
	}
}
func (r *accountRecord) signal() {
	// Called with the Supervisor lock held. Source/config revocation must cancel
	// credential I/O even while the account actor is inside Resolve.
	if r.preparingCancel != nil && (!r.present || r.sourceReason != "" || r.account != r.preparingAccount || r.blockedRevision >= r.account.Revision) {
		r.preparingCancel()
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (r *accountRecord) desired() (domain.Account, Reason, bool) {
	s := r.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.fatal {
		return r.account, ReasonClose, false
	}
	if !r.present {
		return r.account, ReasonMissing, false
	}
	if r.sourceReason != "" {
		return r.account, r.sourceReason, false
	}
	if r.blockedRevision >= r.account.Revision {
		return r.account, r.blockedReason, false
	}
	return r.account, "", true
}
func (r *accountRecord) matches(a domain.Account) bool {
	current, _, allowed := r.desired()
	return allowed && current == a
}
func (r *accountRecord) block(revision int64, reason Reason) {
	s := r.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision >= r.blockedRevision {
		r.blockedRevision = revision
		r.blockedReason = reason
	}
}
func (r *accountRecord) set(a domain.Account, g domain.OwnerGrant, phase Phase, reason Reason, owned, ready bool) {
	s := r.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	r.status = Status{AccountID: a.ID, BotID: a.BotID, Revision: a.Revision, Epoch: g.Epoch, Phase: phase, Reason: reason, Owned: owned, Ready: ready, LeaseUntil: g.LeaseUntil, RestartAttempts: r.restartAttempts, NextRetryAt: r.nextRetry}
}
func (r *accountRecord) cleanupFailure() {
	s := r.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupFailed = true
	r.fatal = true
}
func (r *accountRecord) pause(ctx context.Context) bool {
	timer := time.NewTimer(r.supervisor.options.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-r.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (r *accountRecord) recordIsolation(revision int64, persisted bool) {
	s := r.supervisor
	s.mu.Lock()
	defer s.mu.Unlock()
	r.isolationRevision = revision
	r.isolationPersisted = persisted
	r.isolationError = !persisted
}
