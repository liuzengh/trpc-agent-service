package postgresadapter

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// PreflightAuthorizer returns authorization facts while holding owner row
// locks. A historical completion receipt may still be acknowledged after the
// original requester loses access; only Application decides that distinction.
type PreflightAuthorizer interface {
	TenantAuthorizer
	LockRequesterUsers(context.Context, pgx.Tx, []string) error
	AuthorizeRequester(context.Context, pgx.Tx, string, string) (bool, error)
}

type PreflightStore struct {
	base *Store
	auth PreflightAuthorizer
}

func NewPreflightStore(db DB, auth PreflightAuthorizer, options Options) (*PreflightStore, error) {
	base, err := NewStore(db, auth, options)
	if err != nil {
		return nil, err
	}
	return &PreflightStore{base: base, auth: auth}, nil
}

func (s *PreflightStore) WithTransaction(ctx context.Context, fence string, fn func(application.PreflightTransaction) error) error {
	if fence == "" || len(fence) > 4096 || strings.ContainsRune(fence, 0) || fn == nil {
		return integrity()
	}
	tx, err := s.base.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx)
	c, err := s.base.catalog(ctx, tx, "FOR SHARE")
	if err != nil {
		return err
	}
	// Hash collisions only serialize unrelated diagnostics; the prefix prevents
	// intentional reuse of another module's advisory-lock namespace.
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "control:channel-preflight:"+s.base.options.ScopeID+":"+fence)
	if err != nil {
		return dbError(err)
	}
	t := &preflightTx{store: s, tx: tx, epoch: c.epoch, loaded: make(map[string]application.PreflightRecord)}
	if err = fn(t); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return dbError(err)
	}
	return nil
}

type preflightTx struct {
	store   *PreflightStore
	tx      pgx.Tx
	epoch   string
	account *application.PreflightAccount
	user    string
	loaded  map[string]application.PreflightRecord
}

func (t *preflightTx) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := t.tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, dbError(err)
	}
	return now.UTC(), nil
}

func (t *preflightTx) SourceEpoch() string { return t.epoch }

func (t *preflightTx) LockAccount(ctx context.Context, tenant, account, user string, session bool, additionalRequesters ...string) (application.PreflightAccount, error) {
	var result application.PreflightAccount
	if t.account != nil || !domain.ValidID(tenant) || !domain.ValidID(account) || !domain.ValidID(user) || len(additionalRequesters) > 1 {
		return result, integrity()
	}
	users := append([]string{user}, additionalRequesters...)
	for _, requestedBy := range users {
		if !domain.ValidID(requestedBy) {
			return result, integrity()
		}
	}
	slices.Sort(users)
	users = slices.Compact(users)
	var err error
	// Prelock all requester identities before any Tenant/Session owner query.
	// Create may need both the current actor and the old active task's owner;
	// acquiring the latter after Account/task would invert the lock order.
	if err = t.store.auth.LockRequesterUsers(ctx, t.tx, users); err != nil {
		return result, dbError(err)
	}
	// Public creation additionally checks the original Session through the
	// normal owner adapter; background operations never call that path.
	if session {
		result.SessionOwner, err = t.store.auth.AuthorizeOwner(ctx, t.tx, tenant, user)
		if err != nil {
			return result, dbError(err)
		}
	}
	result.RequesterOwners = make(map[string]bool, len(users))
	for _, requestedBy := range users {
		allowed, err := t.store.auth.AuthorizeRequester(ctx, t.tx, tenant, requestedBy)
		if err != nil {
			return result, dbError(err)
		}
		result.RequesterOwners[requestedBy] = allowed
	}
	result.RequesterOwner = result.RequesterOwners[user]
	result.TenantActive, err = t.store.auth.AuthorizeActiveTenant(ctx, t.tx, tenant)
	if err != nil {
		return result, dbError(err)
	}
	result.Account, err = scanAccount(t.tx.QueryRow(ctx, `SELECT `+accountColumns+` FROM channel_accounts WHERE tenant_id=$1 AND id=$2 AND scope_id=$3 FOR SHARE`, tenant, account, t.store.base.options.ScopeID))
	if err != nil {
		return result, err
	}
	rows, err := t.tx.Query(ctx, `SELECT tenant_id,account_id,provider,purpose,id,credential_version,configured,COALESCE(key_id,''),ciphertext FROM channel_account_credentials WHERE tenant_id=$1 AND account_id=$2 ORDER BY purpose`, tenant, account)
	if err != nil {
		return result, dbError(err)
	}
	for rows.Next() {
		var c domain.CredentialRecord
		if err = rows.Scan(&c.TenantID, &c.AccountID, &c.Provider, &c.Meta.Purpose, &c.Meta.ID, &c.Meta.Version, &c.Meta.Configured, &c.KeyID, &c.Ciphertext); err != nil {
			rows.Close()
			return result, dbError(err)
		}
		result.Credentials = append(result.Credentials, c)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return result, dbError(err)
	}
	// Reuse the owner invariant without reading Binding, Route, or Outbox.
	aggregate := application.Aggregate{Account: result.Account, Credentials: result.Credentials, Route: domain.RouteState{TenantID: tenant, AccountID: account, Generation: result.Account.MinRouteGeneration}}
	if err = validateAggregate(aggregate); err != nil {
		return result, integrity()
	}
	t.account, t.user = &result, user
	return result, nil
}

func (s *PreflightStore) Lookup(ctx context.Context, scope, id string) (application.PreflightKey, bool, error) {
	if scope != s.base.options.ScopeID || !domain.ValidID(id) {
		return application.PreflightKey{}, false, application.ErrWorkloadDenied
	}
	return scanPreflightKey(s.base.db.QueryRow(ctx, `SELECT id,tenant_id,account_id,requested_by FROM channel_preflights WHERE scope_id=$1 AND id=$2`, scope, id))
}

func (s *PreflightStore) Candidate(ctx context.Context, scope string, policy ...string) (application.PreflightKey, bool, error) {
	if scope != s.base.options.ScopeID {
		return application.PreflightKey{}, false, application.ErrWorkloadDenied
	}
	diagnosticPolicy := ""
	if len(policy) > 1 {
		return application.PreflightKey{}, false, integrity()
	}
	if len(policy) == 1 {
		diagnosticPolicy = policy[0]
	}
	// The choice is deliberately unlocked: the transaction must lock owning
	// Identity/Tenant/Account before it may lock and recheck the task.
	return scanPreflightKey(s.base.db.QueryRow(ctx, `SELECT id,tenant_id,account_id,requested_by FROM channel_preflights WHERE scope_id=$1 AND COALESCE(record_jsonb->'view'->>'diagnostic_policy','')=$2 AND (state='QUEUED' OR (state='RUNNING' AND lease_expires_at<=clock_timestamp())) ORDER BY requested_at,id LIMIT 1`, scope, diagnosticPolicy))
}

// ActiveAccount discovers the original requester before the transaction locks
// any Identity/Account rows. Application rechecks this candidate under the
// common lock order; this read never grants access or changes task state.
func (s *PreflightStore) ActiveAccount(ctx context.Context, scope, tenant, account string) (application.PreflightKey, bool, error) {
	if scope != s.base.options.ScopeID || !domain.ValidID(tenant) || !domain.ValidID(account) {
		return application.PreflightKey{}, false, application.ErrWorkloadDenied
	}
	return scanPreflightKey(s.base.db.QueryRow(ctx, `SELECT id,tenant_id,account_id,requested_by FROM channel_preflights WHERE scope_id=$1 AND tenant_id=$2 AND account_id=$3 AND state IN ('QUEUED','RUNNING') ORDER BY requested_at,id LIMIT 1`, scope, tenant, account))
}

func scanPreflightKey(row pgx.Row) (application.PreflightKey, bool, error) {
	var key application.PreflightKey
	err := row.Scan(&key.ID, &key.TenantID, &key.AccountID, &key.RequestedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return key, false, nil
	}
	if err != nil {
		return key, false, dbError(err)
	}
	if !domain.ValidID(key.ID) || !domain.ValidID(key.TenantID) || !domain.ValidID(key.AccountID) || !domain.ValidID(key.RequestedBy) {
		return key, false, integrity()
	}
	return key, true, nil
}

func (s *PreflightStore) MaintenanceCandidates(ctx context.Context, scope string, limit int) ([]application.PreflightKey, error) {
	if scope != s.base.options.ScopeID || limit < 1 || limit > 64 {
		return nil, integrity()
	}
	rows, err := s.base.db.Query(ctx, `SELECT id,tenant_id,account_id,requested_by FROM channel_preflights WHERE scope_id=$1 AND state IN ('QUEUED','RUNNING') ORDER BY last_checked_at,id LIMIT $2`, scope, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	result := make([]application.PreflightKey, 0, limit)
	for rows.Next() {
		key, _, err := scanPreflightKey(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, key)
	}
	if err = rows.Err(); err != nil {
		return nil, dbError(err)
	}
	return result, nil
}

func (s *PreflightStore) Cleanup(ctx context.Context) error {
	// Each bounded DELETE commits without acquiring an account/tenant lock.
	// Thus cleanup never waits for an account after taking a task lock. Receipts
	// that still reference a retained task protect it until their own expiry.
	_, err := s.base.db.Exec(ctx, `WITH expired AS (
	 SELECT scope_id,kind,key_hash FROM channel_preflight_requests
	 WHERE scope_id=$1 AND expires_at<=clock_timestamp()
	 ORDER BY expires_at,kind,key_hash LIMIT 64 FOR UPDATE SKIP LOCKED
	) DELETE FROM channel_preflight_requests r USING expired e
	 WHERE r.scope_id=e.scope_id AND r.kind=e.kind AND r.key_hash=e.key_hash`, s.base.options.ScopeID)
	if err != nil {
		return dbError(err)
	}
	_, err = s.base.db.Exec(ctx, `WITH expired AS (
	 SELECT p.id FROM channel_preflights p
	 WHERE p.scope_id=$1 AND p.state NOT IN ('QUEUED','RUNNING')
	 AND p.requested_at<=clock_timestamp()-interval '24 hours'
	 AND NOT EXISTS (SELECT 1 FROM channel_preflight_requests r WHERE r.scope_id=p.scope_id AND r.preflight_id=p.id)
	 ORDER BY p.requested_at,p.id LIMIT 64 FOR UPDATE OF p SKIP LOCKED
	) DELETE FROM channel_preflights p USING expired e WHERE p.id=e.id`, s.base.options.ScopeID)
	if err != nil {
		return dbError(err)
	}
	return nil
}

var _ application.PreflightStore = (*PreflightStore)(nil)
var _ application.PreflightTransaction = (*preflightTx)(nil)
