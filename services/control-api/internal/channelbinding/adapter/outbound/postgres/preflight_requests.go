package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func (t *preflightTx) FindRequest(ctx context.Context, kind, key string) (application.PreflightRequest, bool, error) {
	var r application.PreflightRequest
	if (kind != "create" && kind != "claim") || !domain.ValidDigest(key) {
		return r, false, integrity()
	}
	var digest, tenant, account, id, principal, instance, epoch string
	var created, expires time.Time
	var raw []byte
	// The transaction fence serializes same-key writers. No row lock is taken
	// before Identity/Tenant/Account, including on public idempotent replay.
	err := t.tx.QueryRow(ctx, `SELECT request_digest,COALESCE(tenant_id,''),COALESCE(account_id,''),COALESCE(preflight_id,''),principal_id,instance_id,instance_epoch,created_at,expires_at,record_jsonb FROM channel_preflight_requests WHERE scope_id=$1 AND kind=$2 AND key_hash=$3 AND expires_at>clock_timestamp()`, t.store.base.options.ScopeID, kind, key).Scan(&digest, &tenant, &account, &id, &principal, &instance, &epoch, &created, &expires, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, dbError(err)
	}
	if err = strictPreflightJSON(raw, &r); err != nil {
		return r, false, err
	}
	if r.Kind != kind || r.Key != key || r.Digest != digest || r.TenantID != tenant || r.AccountID != account || r.PreflightID != id || r.PrincipalID != principal || r.InstanceID != instance || r.InstanceEpoch != epoch || !r.CreatedAt.Equal(created) || !r.ExpiresAt.Equal(expires) {
		return r, false, integrity()
	}
	if err = validatePreflightRequest(r, t.store.base.options.ScopeID); err != nil {
		return r, false, err
	}
	return r, true, nil
}

func validatePreflightRequest(r application.PreflightRequest, scope string) error {
	if !domain.ValidDigest(r.Key) || !domain.ValidDigest(r.Digest) || r.CreatedAt.IsZero() || !r.ExpiresAt.After(r.CreatedAt) {
		return integrity()
	}
	if r.Kind == "create" {
		if !domain.ValidID(r.TenantID) || !domain.ValidID(r.AccountID) || !domain.ValidID(r.RequestedBy) || !domain.ValidID(r.PreflightID) || r.PrincipalID != "" || r.InstanceID != "" || r.InstanceEpoch != "" || !r.ExpiresAt.Equal(r.CreatedAt.Add(24*time.Hour)) {
			return integrity()
		}
		var receipt channelv1.PreflightCreated
		if channelv1.Decode("preflight-created.schema.json", r.Response, &receipt) != nil || receipt.PreflightID != r.PreflightID || receipt.TenantID != r.TenantID || receipt.AccountID != r.AccountID {
			return integrity()
		}
		return nil
	}
	if r.Kind != "claim" || !validPreflightPrincipal(r.PrincipalID) || !domain.ValidID(r.InstanceID) || !domain.ValidEpoch(r.InstanceEpoch) || !r.ExpiresAt.Equal(r.CreatedAt.Add(5*time.Minute)) {
		return integrity()
	}
	if len(r.Response) == 0 || string(r.Response) == "null" {
		if r.TenantID != "" || r.AccountID != "" || r.PreflightID != "" || r.RequestedBy != "" {
			return integrity()
		}
		return nil
	}
	var grant channelv1.PreflightGrant
	if channelv1.Decode("preflight-grant.schema.json", r.Response, &grant) != nil || grant.ScopeID != scope || grant.PreflightID != r.PreflightID || grant.TenantID != r.TenantID || grant.AccountID != r.AccountID || !domain.ValidID(r.RequestedBy) {
		return integrity()
	}
	return nil
}

func (t *preflightTx) SaveRequest(ctx context.Context, r application.PreflightRequest) error {
	if err := validatePreflightRequest(r, t.store.base.options.ScopeID); err != nil {
		return err
	}
	if r.PreflightID != "" {
		if t.account == nil || t.account.Account.TenantID != r.TenantID || t.account.Account.ID != r.AccountID {
			return integrity()
		}
		stored, found := t.loaded[r.PreflightID]
		if !found {
			var err error
			stored, found, err = t.Load(ctx, r.PreflightID)
			if err != nil {
				return err
			}
		}
		if !found || stored.View.RequestedBy != r.RequestedBy {
			return integrity()
		}
		if r.Kind == "create" {
			var created channelv1.PreflightCreated
			if json.Unmarshal(r.Response, &created) != nil || !created.RequestedAt.Equal(stored.View.RequestedAt) || !created.JobDeadlineAt.Equal(stored.View.JobDeadlineAt) {
				return integrity()
			}
		} else {
			var grant channelv1.PreflightGrant
			if json.Unmarshal(r.Response, &grant) != nil || stored.LeaseExpiresAt == nil || stored.View.GatewayConfigDigest == nil || stored.PrincipalID != r.PrincipalID || stored.InstanceID != r.InstanceID || stored.InstanceEpoch != r.InstanceEpoch || grant.AllowConnectionProbe != stored.View.AllowConnectionProbe || grant.DiagnosticPolicy != stored.View.DiagnosticPolicy || grant.ReceiveMode != stored.View.ReceiveMode || grant.EffectiveConfigDigest != stored.View.EffectiveConfigDigest || grant.SourceEpoch != stored.SourceEpoch || grant.Provider != stored.View.Provider || grant.ProviderAccountID != stored.View.ProviderAccountID || grant.AccountRevision != stored.View.AccountRevision || grant.ConnectionRevision != stored.View.ConnectionRevision || grant.WebhookPath != stored.WebhookPath || grant.Credentials.CredentialID != stored.CredentialID || grant.Credentials.CredentialVersion != stored.View.CredentialVersion() || grant.Credentials.Configured != stored.CredentialConfigured() || grant.WebhookSecretConfigured != stored.WebhookSecretConfigured || grant.LeaseEpoch != stored.LeaseEpoch || !grant.LeaseExpiresAt.Equal(*stored.LeaseExpiresAt) || !grant.JobDeadlineAt.Equal(stored.View.JobDeadlineAt) || grant.GatewayConfigDigest != *stored.View.GatewayConfigDigest {
				return integrity()
			}
		}
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > maxPreflightRecord {
		return integrity()
	}
	// Expired receipts can be replaced without first deleting them. A retained
	// response is never silently overwritten, even if an Application caller
	// accidentally bypassed its FindRequest/idempotency branch.
	tag, err := t.tx.Exec(ctx, `INSERT INTO channel_preflight_requests(scope_id,kind,key_hash,request_digest,tenant_id,account_id,preflight_id,principal_id,instance_id,instance_epoch,created_at,expires_at,record_jsonb)
	 VALUES($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''),NULLIF($7,''),$8,$9,$10,$11,$12,$13)
	 ON CONFLICT(scope_id,kind,key_hash) DO UPDATE SET request_digest=EXCLUDED.request_digest,tenant_id=EXCLUDED.tenant_id,account_id=EXCLUDED.account_id,preflight_id=EXCLUDED.preflight_id,principal_id=EXCLUDED.principal_id,instance_id=EXCLUDED.instance_id,instance_epoch=EXCLUDED.instance_epoch,created_at=EXCLUDED.created_at,expires_at=EXCLUDED.expires_at,record_jsonb=EXCLUDED.record_jsonb
	 WHERE channel_preflight_requests.expires_at<=clock_timestamp()`, t.store.base.options.ScopeID, r.Kind, r.Key, r.Digest, r.TenantID, r.AccountID, r.PreflightID, r.PrincipalID, r.InstanceID, r.InstanceEpoch, r.CreatedAt, r.ExpiresAt, raw)
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() != 1 {
		return application.ErrIdempotencyConflict
	}
	return nil
}

func (t *preflightTx) ClaimCount(ctx context.Context, principal, instance string, since time.Time) (int, error) {
	if !validPreflightPrincipal(principal) || !domain.ValidID(instance) || since.IsZero() {
		return 0, integrity()
	}
	var count int
	err := t.tx.QueryRow(ctx, `SELECT count(*) FROM channel_preflight_requests WHERE scope_id=$1 AND kind='claim' AND principal_id=$2 AND instance_id=$3 AND created_at>$4`, t.store.base.options.ScopeID, principal, instance, since).Scan(&count)
	if err != nil {
		return 0, dbError(err)
	}
	return count, nil
}
