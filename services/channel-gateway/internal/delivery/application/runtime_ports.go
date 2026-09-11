package application

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

// AccountKey is a scheduling identity, never a credential or a send authority.
type AccountKey struct{ Provider, AccountID string }
type DueAccountQuery struct {
	Provider, AfterAccountID string
	Limit                    int
}
type DueAccountPage struct {
	Accounts      []AccountKey
	NextAccountID string
	Exhausted     bool
}
type DueAccountReader interface {
	ListDueAccounts(context.Context, DueAccountQuery) (DueAccountPage, error)
}

// Eligibility is only a local pre-check. ClaimDue and MarkCalling still perform
// authoritative database fencing. Telegram must have no Owner; WeCom must have one.
type SendEligibility struct {
	Eligible bool
	Owner    *domain.OwnerFence
}
type AccountEligibility interface {
	InspectAccount(context.Context, AccountKey) (SendEligibility, error)
}
type AccountDispatcher interface {
	DispatchAccount(context.Context, domain.ClaimRequest) (int, error)
}

type ObservedAttemptQuery struct {
	AfterAttemptID string
	Limit          int
}
type ObservedAttemptPage struct {
	AttemptIDs    []string
	NextAttemptID string
	Exhausted     bool
}

// MaintenanceStore changes only delivery-owned facts. Expiry is not GC and does
// not release the ledger's total-row capacity or authorize any Provider call.
type MaintenanceStore interface {
	RecoverExpiredClaims(context.Context, int) (int, error)
	RecoverStaleCalling(context.Context, int) (int, error)
	ExpirePending(context.Context, int) (int, error)
	ListResolvableObserved(context.Context, ObservedAttemptQuery) (ObservedAttemptPage, error)
	ResolveObserved(context.Context, string) (bool, error)
}
