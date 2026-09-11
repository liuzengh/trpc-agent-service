package postgresadapter

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func (s *Store) SaveObservations(ctx context.Context, scope string, values []application.Observation) error {
	if scope != s.options.ScopeID || len(values) == 0 || len(values) > 100 {
		return application.ErrWorkloadDenied
	}
	values = slices.Clone(values)
	slices.SortFunc(values, func(a, b application.Observation) int {
		return strings.Compare(a.TenantID+"\x00"+a.AccountID+"\x00"+a.InstanceID+"\x00"+a.InstanceEpoch, b.TenantID+"\x00"+b.AccountID+"\x00"+b.InstanceID+"\x00"+b.InstanceEpoch)
	})
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return dbError(err)
	}
	defer rollback(tx)
	c, err := s.catalog(ctx, tx, "FOR SHARE")
	if err != nil {
		return err
	}
	checked := map[string]domain.Account{}
	for _, o := range values {
		if o.ScopeID != scope || o.SourceEpoch != c.epoch {
			return application.ErrEpochMismatch
		}
		key := o.TenantID + "\x00" + o.AccountID
		a, ok := checked[key]
		if !ok {
			allowed, err := s.auth.AuthorizeActiveTenant(ctx, tx, o.TenantID)
			if err != nil {
				return dbError(err)
			}
			if !allowed {
				return application.ErrAccountNotFound
			}
			a, err = scanAccount(tx.QueryRow(ctx, `SELECT `+accountColumns+` FROM channel_accounts WHERE tenant_id=$1 AND id=$2 AND scope_id=$3 FOR SHARE`, o.TenantID, o.AccountID, scope))
			if err != nil {
				return err
			}
			checked[key] = a
		}
		mode := o.ReceiveMode
		if o.Provider == domain.Telegram && mode == "" {
			mode = domain.Webhook
		}
		if mode != a.Config.ReceiveMode {
			return &domain.Error{Code: domain.CredentialVersionConflict}
		}
		if a.Provider != o.Provider {
			return application.ErrAccountNotFound
		}
		if a.ConnectionRevision != o.ConnectionRevision {
			return &domain.Error{Code: domain.CredentialVersionConflict}
		}
		if o.State == "READY" && !a.Enabled {
			return &domain.Error{Code: domain.AccountDisabled}
		}
	}
	for _, o := range values {
		var mode *string
		if o.Provider == domain.Telegram {
			v := o.ReceiveMode
			if v == "" {
				v = domain.Webhook
			}
			mode = &v
		}
		_, digest, err := domain.CanonicalJSON(o)
		if err != nil {
			return err
		}
		var sequence int64
		var oldDigest string
		err = tx.QueryRow(ctx, `SELECT report_sequence,observation_digest FROM channel_account_observations WHERE tenant_id=$1 AND account_id=$2 AND instance_id=$3 AND instance_epoch=$4 FOR UPDATE`, o.TenantID, o.AccountID, o.InstanceID, o.InstanceEpoch).Scan(&sequence, &oldDigest)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return dbError(err)
		}
		if err == nil && o.ReportSequence <= sequence {
			if o.ReportSequence == sequence && digest != oldDigest {
				return &domain.Error{Code: domain.RevisionConflict}
			}
			continue
		}
		tag, err := tx.Exec(ctx, `INSERT INTO channel_account_observations(tenant_id,account_id,scope_id,source_epoch,provider,connection_revision,instance_id,instance_epoch,report_sequence,state,reason_code,owner_epoch,observed_at,received_at,observation_digest,receive_mode) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,clock_timestamp(),$14,$15) ON CONFLICT(tenant_id,account_id,instance_id,instance_epoch) DO UPDATE SET connection_revision=EXCLUDED.connection_revision,report_sequence=EXCLUDED.report_sequence,state=EXCLUDED.state,reason_code=EXCLUDED.reason_code,owner_epoch=EXCLUDED.owner_epoch,observed_at=EXCLUDED.observed_at,received_at=EXCLUDED.received_at,observation_digest=EXCLUDED.observation_digest,receive_mode=EXCLUDED.receive_mode WHERE channel_account_observations.report_sequence<EXCLUDED.report_sequence`, o.TenantID, o.AccountID, o.ScopeID, o.SourceEpoch, o.Provider, o.ConnectionRevision, o.InstanceID, o.InstanceEpoch, o.ReportSequence, o.State, o.ReasonCode, o.OwnerEpoch, o.ObservedAt, digest, mode)
		if err != nil {
			return dbError(err)
		}
		if tag.RowsAffected() == 0 {
			// A concurrent first insert may have won after our initial read.
			var currentSequence int64
			var currentDigest string
			if err = tx.QueryRow(ctx, `SELECT report_sequence,observation_digest FROM channel_account_observations WHERE tenant_id=$1 AND account_id=$2 AND instance_id=$3 AND instance_epoch=$4 FOR UPDATE`, o.TenantID, o.AccountID, o.InstanceID, o.InstanceEpoch).Scan(&currentSequence, &currentDigest); err != nil {
				return dbError(err)
			}
			if currentSequence == o.ReportSequence && currentDigest != digest {
				return &domain.Error{Code: domain.RevisionConflict}
			}
		}
	}
	return sqlResult(tx.Commit(ctx))
}
func (s *Store) PruneObservations(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `WITH expired AS (SELECT ctid FROM channel_account_observations WHERE scope_id=$1 AND received_at < clock_timestamp()-interval '24 hours' ORDER BY received_at LIMIT 1000 FOR UPDATE SKIP LOCKED) DELETE FROM channel_account_observations o USING expired e WHERE o.ctid=e.ctid`, s.options.ScopeID)
	return sqlResult(err)
}

var _ application.RuntimeStore = (*Store)(nil)
