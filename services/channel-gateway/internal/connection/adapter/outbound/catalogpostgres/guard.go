package catalogpostgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

// Issue creates a local permit bounded at the authenticated poll START. Callers
// must revoke it on source failure, account/client rotation, ownership loss and
// shutdown. Guard also checks the shared authoritative row inside each use Tx.
func (s *Store) Issue(parent context.Context, p Poll, q Qualification, a c.Account, kind string, clientGeneration int64) (*c.Permit, error) {
	if p.ScopeID != s.scope || p.SourceEpoch != s.epoch || p.InstanceID != s.instance || p.InstanceEpoch != s.boot || p.LocalStarted.IsZero() || q.Generation < 1 || !q.ValidUntil.Equal(p.StartedAt.Add(30*time.Second)) || a.Validate() != nil || !a.Enabled {
		return nil, c.ErrUnauthorized
	}
	b := c.UseBinding{ScopeID: s.scope, SourceEpoch: s.epoch, InstanceID: s.instance, InstanceEpoch: s.boot, TenantID: a.TenantID, Provider: a.Provider, AccountID: a.ID, Kind: kind, ConnectionRevision: a.ConnectionRevision, QualificationGeneration: q.Generation, ClientGeneration: clientGeneration}
	return c.NewPermit(parent, b, p.LocalStarted.Add(30*time.Second))
}

// Guard is a transaction seam for Admission/Delivery/registration. The caller
// acquires this account->instance prefix before its own existing locks and calls
// RecheckExpiry again at the final state write. It never commits the caller's Tx.
// routeGeneration is nonnil only for new Run admission, never historical replies.
func (s *Store) Guard(ctx context.Context, tx pgx.Tx, p *c.Permit, routeGeneration *int64) error {
	if tx == nil || p.Check() != nil {
		return c.ErrUnauthorized
	}
	b := p.Binding()
	if b.ScopeID != s.scope || b.SourceEpoch != s.epoch || b.InstanceID != s.instance || b.InstanceEpoch != s.boot {
		return c.ErrUnauthorized
	}
	var tenant, provider, epoch, mode string
	var revision, floor int64
	var enabled, present bool
	err := tx.QueryRow(ctx, `SELECT tenant_id,provider,source_epoch,connection_revision,min_route_generation,enabled,present,COALESCE(account_json->'config'->>'receive_mode','webhook') FROM gateway_account_directory WHERE scope_id=$1 AND account_id=$2 FOR SHARE`, s.scope, b.AccountID).Scan(&tenant, &provider, &epoch, &revision, &floor, &enabled, &present, &mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return c.ErrUnauthorized
	}
	if err != nil {
		return dbError(err)
	}
	if !present || !enabled || epoch != b.SourceEpoch || tenant != b.TenantID || provider != b.Provider || revision != b.ConnectionRevision || !c.ValidUseKind(provider, b.Kind) {
		return c.ErrUnauthorized
	}
	if provider == "telegram" && (b.Kind == "telegram_webhook" || b.Kind == "telegram_registration") && mode != "webhook" {
		return c.ErrUnauthorized
	}
	if routeGeneration != nil {
		if b.Kind != "telegram_webhook" && b.Kind != "telegram_receiver" && b.Kind != "wecom_ingress" {
			return c.ErrUnauthorized
		}
		if !c.ValidRevision(*routeGeneration) || *routeGeneration < floor {
			return c.ErrVersion
		}
	}
	var generation int64
	var usable bool
	err = tx.QueryRow(ctx, `SELECT generation,enabled AND clock_timestamp()<valid_until FROM gateway_account_qualifications WHERE scope_id=$1 AND instance_id=$2 AND instance_epoch=$3 AND source_epoch=$4 FOR SHARE`, s.scope, s.instance, s.boot, s.epoch).Scan(&generation, &usable)
	if errors.Is(err, pgx.ErrNoRows) {
		return c.ErrUnauthorized
	}
	if err != nil {
		return dbError(err)
	}
	if !usable || generation != b.QualificationGeneration || p.Check() != nil {
		return c.ErrUnauthorized
	}
	return nil
}

// RecheckExpiry assumes Guard's row locks are still held in this same Tx.
// Application checks Permit again before commit and before the SDK call.
func (s *Store) RecheckExpiry(ctx context.Context, tx pgx.Tx, p *c.Permit) error {
	if tx == nil || p.Check() != nil {
		return c.ErrUnauthorized
	}
	b := p.Binding()
	if b.ScopeID != s.scope || b.InstanceID != s.instance || b.InstanceEpoch != s.boot {
		return c.ErrUnauthorized
	}
	var allowed bool
	err := tx.QueryRow(ctx, `SELECT enabled AND generation=$4 AND clock_timestamp()<valid_until FROM gateway_account_qualifications WHERE scope_id=$1 AND instance_id=$2 AND instance_epoch=$3 AND source_epoch=$5`, s.scope, s.instance, s.boot, b.QualificationGeneration, s.epoch).Scan(&allowed)
	if err != nil {
		return dbError(err)
	}
	if !allowed || p.Check() != nil {
		return c.ErrUnauthorized
	}
	return nil
}
