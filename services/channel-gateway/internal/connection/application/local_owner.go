package application

import (
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

// LocalOwner returns a copy of the actual currently usable local grant. It is
// only a scheduling eligibility snapshot: A1/A2 must still verify the owner in
// their database transactions, and ReserveFinal must match the original socket.
// This read performs no acquisition, renewal, database call or credential lookup.
func (s *Supervisor) LocalOwner(accountID string) (domain.OwnerGrant, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[accountID]
	if !s.running || s.runContext == nil || s.runContext.Err() != nil || !s.healthy || r == nil || !r.present || !r.account.Enabled || r.sourceReason != "" || r.blockedRevision >= r.account.Revision || r.fatal || r.status.Phase == PhaseDraining || r.lease == nil {
		return domain.OwnerGrant{}, false
	}
	// Keep the same Supervisor -> localLease lock order as sender lookup. Never
	// reconstruct a grant from the eventually-updated public Status snapshot.
	l := r.lease
	l.mu.Lock()
	defer l.mu.Unlock()
	g := l.grant
	delta := g.LeaseUntil.Sub(g.ObservedAt)
	if l.account != r.account || g.Validate() != nil || g.AccountID != accountID || g.AccountID != r.account.ID || g.BotID != r.account.BotID || g.InstanceID != s.options.InstanceID || g.Revision != r.account.Revision || delta <= 0 || delta > s.options.LeaseTTL {
		return domain.OwnerGrant{}, false
	}
	if l.expired || l.stopped || l.ctx == nil || l.ctx.Err() != nil || !time.Now().Before(l.deadline) || l.client == nil {
		return domain.OwnerGrant{}, false
	}
	status := l.client.Status()
	if !status.Ready || status.Terminal || status.Replaced || s.runContext.Err() != nil || l.ctx.Err() != nil || !time.Now().Before(l.deadline) {
		return domain.OwnerGrant{}, false
	}
	return g, true
}
