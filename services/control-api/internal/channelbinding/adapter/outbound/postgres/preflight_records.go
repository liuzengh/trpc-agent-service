package postgresadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	"github.com/jackc/pgx/v5"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

const preflightColumns = `id,scope_id,tenant_id,account_id,requested_by,state,requested_at,job_deadline_at,lease_expires_at,last_checked_at,record_jsonb`
const maxPreflightRecord = 64 * 1024

func strictPreflightJSON(raw []byte, value any) error {
	if len(raw) == 0 || len(raw) > maxPreflightRecord {
		return integrity()
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return integrity()
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return integrity()
	}
	// encoding/json otherwise accepts case-folded field names. Compare the
	// canonical typed round trip so persisted casing/field presence cannot be
	// silently normalized (and duplicate keys are rejected by JCS as well).
	original, err := jcs.Transform(raw)
	if err != nil {
		return integrity()
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return integrity()
	}
	normalized, err := jcs.Transform(encoded)
	if err != nil || !bytes.Equal(original, normalized) {
		return integrity()
	}
	return nil
}

func scanPreflight(row pgx.Row) (application.PreflightRecord, bool, error) {
	var record application.PreflightRecord
	var id, scope, tenant, account, requestedBy, state string
	var requestedAt, deadline, lastChecked time.Time
	var lease *time.Time
	var raw []byte
	err := row.Scan(&id, &scope, &tenant, &account, &requestedBy, &state, &requestedAt, &deadline, &lease, &lastChecked, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return record, false, nil
	}
	if err != nil {
		return record, false, dbError(err)
	}
	if err = strictPreflightJSON(raw, &record); err != nil {
		return record, false, err
	}
	v := record.View
	if v.PreflightID != id || record.ScopeID != scope || v.TenantID != tenant || v.AccountID != account || v.RequestedBy != requestedBy || v.State != state || !v.RequestedAt.Equal(requestedAt) || !v.JobDeadlineAt.Equal(deadline) || !samePreflightTime(record.LeaseExpiresAt, lease) || !record.LastCheckedAt.Equal(lastChecked) {
		return record, false, integrity()
	}
	if err = validatePreflightRecord(record); err != nil {
		return record, false, err
	}
	return record, true, nil
}

func validatePreflightRecord(r application.PreflightRecord) error {
	v := r.View
	if !domain.ValidID(r.ScopeID) || !domain.ValidEpoch(r.SourceEpoch) || !domain.ValidID(r.CredentialID) || v.ConnectionRevision > v.AccountRevision || v.CredentialVersion() > v.ConnectionRevision || v.RequestedAt.IsZero() || !v.JobDeadlineAt.Equal(v.RequestedAt.Add(120*time.Second)) || r.LastCheckedAt.Before(v.RequestedAt) {
		return integrity()
	}
	if v.Provider == "wecom" {
		if !v.AllowConnectionProbe || r.WebhookPath != "" || r.WebhookSecretConfigured || r.BotTokenConfigured {
			return integrity()
		}
	} else if r.WebhookPath != "/v1/telegram/"+v.AccountID || v.AllowConnectionProbe || r.BotSecretConfigured {
		return integrity()
	}
	raw, err := json.Marshal(v)
	if err != nil || channelv1.Validate("preflight-view.schema.json", raw) != nil {
		return integrity()
	}
	if r.LeaseEpoch < 0 || r.LeaseEpoch > 2 {
		return integrity()
	}
	if r.LeaseEpoch == 0 {
		if r.LeaseExpiresAt != nil || r.PrincipalID != "" || r.InstanceID != "" || r.InstanceEpoch != "" || r.ClaimTokenHash != "" || r.OriginStatus != "" || r.GatewayPublicOrigin != nil || v.EffectiveConfigDigest != "" || v.StartedAt != nil || v.GatewayConfigDigest != nil || v.GatewayConfigFreshness != nil || v.ExpectedPublicOrigin != nil || v.State == "RUNNING" || v.State == "COMPLETED" {
			return integrity()
		}
	} else {
		if r.LeaseExpiresAt == nil || !r.LeaseExpiresAt.After(v.RequestedAt) || !validPreflightPrincipal(r.PrincipalID) || !domain.ValidID(r.InstanceID) || !domain.ValidEpoch(r.InstanceEpoch) || !domain.ValidDigest(r.ClaimTokenHash) || v.StartedAt == nil || v.GatewayConfigDigest == nil || v.GatewayConfigFreshness == nil || *v.GatewayConfigFreshness != "UNCONFIRMED" || v.State == "QUEUED" {
			return integrity()
		}
		origin := v.ExpectedPublicOrigin
		if v.DiagnosticPolicy != "" {
			origin = r.GatewayPublicOrigin
			effective, err := channelv1.PreflightEffectiveConfigDigest(r.ScopeID, r.SourceEpoch, v.ReceiveMode, v.ConnectionRevision, origin, r.OriginStatus, v.EndpointProfile)
			if err != nil || effective != v.EffectiveConfigDigest || (v.ReceiveMode == domain.Webhook && !reflect.DeepEqual(origin, v.ExpectedPublicOrigin)) {
				return integrity()
			}
		} else if r.GatewayPublicOrigin != nil {
			return integrity()
		}
		digest, err := channelv1.PreflightConfigDigestForPolicy(v.DiagnosticPolicy, r.ScopeID, r.SourceEpoch, origin, r.OriginStatus)
		if err != nil || digest != *v.GatewayConfigDigest {
			return integrity()
		}
	}
	if v.State == "COMPLETED" {
		if !domain.ValidDigest(r.CompleteDigest) || v.CheckedAt == nil || v.ExpiresAt == nil || !v.ExpiresAt.Equal(v.CheckedAt.Add(5*time.Minute)) || v.CheckedAt.Before(*v.StartedAt) {
			return integrity()
		}
	} else if r.CompleteDigest != "" || v.CheckedAt != nil || v.ExpiresAt != nil || len(v.Checks) != 0 || v.Outcome != "UNKNOWN" {
		return integrity()
	}
	return nil
}

func samePreflightTime(a, b *time.Time) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Equal(*b)
}

func clonePreflight(r application.PreflightRecord) application.PreflightRecord {
	raw, _ := json.Marshal(r)
	var copy application.PreflightRecord
	_ = json.Unmarshal(raw, &copy)
	return copy
}

func preflightActive(state string) bool { return state == "QUEUED" || state == "RUNNING" }

func validatePreflightChange(old, next application.PreflightRecord) error {
	a, b := old.View, next.View
	if a.AllowConnectionProbe != b.AllowConnectionProbe || a.BotSecretVersion != b.BotSecretVersion || old.BotSecretConfigured != next.BotSecretConfigured || a.ReceiveMode != b.ReceiveMode || a.DiagnosticPolicy != b.DiagnosticPolicy || old.ScopeID != next.ScopeID || old.SourceEpoch != next.SourceEpoch || old.CredentialID != next.CredentialID || old.BotTokenConfigured != next.BotTokenConfigured || old.WebhookSecretConfigured != next.WebhookSecretConfigured || old.WebhookPath != next.WebhookPath || a.PreflightID != b.PreflightID || a.TenantID != b.TenantID || a.AccountID != b.AccountID || a.RequestedBy != b.RequestedBy || a.Provider != b.Provider || a.ProviderAccountID != b.ProviderAccountID || a.AccountRevision != b.AccountRevision || a.ConnectionRevision != b.ConnectionRevision || a.BotTokenVersion != b.BotTokenVersion || !a.RequestedAt.Equal(b.RequestedAt) || !a.JobDeadlineAt.Equal(b.JobDeadlineAt) || next.LastCheckedAt.Before(old.LastCheckedAt) {
		return integrity()
	}
	if !preflightActive(a.State) {
		// The stored terminal fact is immutable. Freshness is derived in the
		// public read model rather than overwriting this historical result.
		x, y := clonePreflight(old), clonePreflight(next)
		x.LastCheckedAt, y.LastCheckedAt = time.Time{}, time.Time{}
		if !reflect.DeepEqual(x, y) {
			return integrity()
		}
		return nil
	}
	if a.StartedAt != nil && !samePreflightTime(a.StartedAt, b.StartedAt) || a.EffectiveConfigDigest != "" && a.EffectiveConfigDigest != b.EffectiveConfigDigest {
		return integrity()
	}
	if a.GatewayConfigDigest != nil {
		refreshedLease := a.DiagnosticPolicy != "" && a.ReceiveMode == domain.LongPolling && next.LeaseEpoch == old.LeaseEpoch+1 && a.EffectiveConfigDigest == b.EffectiveConfigDigest
		if !refreshedLease && (!reflect.DeepEqual(a.GatewayConfigDigest, b.GatewayConfigDigest) || !reflect.DeepEqual(a.ExpectedPublicOrigin, b.ExpectedPublicOrigin) || !reflect.DeepEqual(old.GatewayPublicOrigin, next.GatewayPublicOrigin) || old.OriginStatus != next.OriginStatus) {
			return integrity()
		}
	}
	if next.LeaseEpoch < old.LeaseEpoch || next.LeaseEpoch > old.LeaseEpoch+1 || next.LeaseEpoch != old.LeaseEpoch && b.State != "RUNNING" {
		return integrity()
	}
	if next.LeaseEpoch == old.LeaseEpoch && (old.PrincipalID != next.PrincipalID || old.InstanceID != next.InstanceID || old.InstanceEpoch != next.InstanceEpoch || old.ClaimTokenHash != next.ClaimTokenHash || !samePreflightTime(old.LeaseExpiresAt, next.LeaseExpiresAt)) {
		return integrity()
	}
	return nil
}

func (t *preflightTx) Load(ctx context.Context, id string) (application.PreflightRecord, bool, error) {
	if t.account == nil || !domain.ValidID(id) {
		return application.PreflightRecord{}, false, integrity()
	}
	a := t.account.Account
	r, found, err := scanPreflight(t.tx.QueryRow(ctx, `SELECT `+preflightColumns+` FROM channel_preflights WHERE scope_id=$1 AND tenant_id=$2 AND account_id=$3 AND id=$4 FOR UPDATE`, t.store.base.options.ScopeID, a.TenantID, a.ID, id))
	if err == nil && found {
		t.loaded[id] = clonePreflight(r)
	}
	return r, found, err
}

func (t *preflightTx) Save(ctx context.Context, r application.PreflightRecord) error {
	if t.account == nil || r.ScopeID != t.store.base.options.ScopeID || r.View.TenantID != t.account.Account.TenantID || r.View.AccountID != t.account.Account.ID {
		return integrity()
	}
	if err := validatePreflightRecord(r); err != nil {
		return err
	}
	old, exists := t.loaded[r.View.PreflightID]
	if exists {
		if err := validatePreflightChange(old, r); err != nil {
			return err
		}
	} else if r.View.State != "QUEUED" || r.LeaseEpoch != 0 || r.SourceEpoch != t.epoch || r.View.RequestedBy != t.user {
		return integrity()
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > maxPreflightRecord {
		return integrity()
	}
	v := r.View
	args := []any{v.PreflightID, r.ScopeID, v.TenantID, v.AccountID, v.RequestedBy, v.State, v.RequestedAt, v.JobDeadlineAt, r.LeaseExpiresAt, r.LastCheckedAt, raw}
	if exists {
		tag, err := t.tx.Exec(ctx, `UPDATE channel_preflights SET state=$6,lease_expires_at=$9,last_checked_at=$10,record_jsonb=$11 WHERE id=$1 AND scope_id=$2 AND tenant_id=$3 AND account_id=$4 AND requested_by=$5 AND requested_at=$7 AND job_deadline_at=$8`, args...)
		if err != nil {
			return dbError(err)
		}
		if tag.RowsAffected() != 1 {
			return integrity()
		}
	} else {
		if _, err = t.tx.Exec(ctx, `INSERT INTO channel_preflights(`+preflightColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, args...); err != nil {
			return dbError(err)
		}
	}
	t.loaded[v.PreflightID] = clonePreflight(r)
	return nil
}

func (t *preflightTx) Active(ctx context.Context, tenant, account string) ([]application.PreflightRecord, error) {
	if t.account == nil || tenant != t.account.Account.TenantID || account != t.account.Account.ID {
		return nil, integrity()
	}
	rows, err := t.tx.Query(ctx, `SELECT `+preflightColumns+` FROM channel_preflights WHERE scope_id=$1 AND tenant_id=$2 AND account_id=$3 AND state IN ('QUEUED','RUNNING') ORDER BY requested_at,id FOR UPDATE`, t.store.base.options.ScopeID, tenant, account)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	var result []application.PreflightRecord
	for rows.Next() {
		r, _, err := scanPreflight(rows)
		if err != nil {
			return nil, err
		}
		t.loaded[r.View.PreflightID] = clonePreflight(r)
		result = append(result, r)
	}
	if err = rows.Err(); err != nil {
		return nil, dbError(err)
	}
	return result, nil
}

func (t *preflightTx) Counts(ctx context.Context, tenant, account string, since time.Time) (int, int, int, error) {
	if t.account == nil || tenant != t.account.Account.TenantID || account != t.account.Account.ID || since.IsZero() {
		return 0, 0, 0, integrity()
	}
	var ac, tc, active int
	err := t.tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE account_id=$3 AND requested_at>$4),count(*) FILTER (WHERE requested_at>$4),count(*) FILTER (WHERE state IN ('QUEUED','RUNNING')) FROM channel_preflights WHERE scope_id=$1 AND tenant_id=$2`, t.store.base.options.ScopeID, tenant, account, since).Scan(&ac, &tc, &active)
	if err != nil {
		return 0, 0, 0, dbError(err)
	}
	return ac, tc, active, nil
}

// Principal IDs are trusted workload identities (usually URI SANs), not domain
// IDs. Preserve the configured identity exactly while rejecting unbounded or
// control-bearing storage input.
func validPreflightPrincipal(value string) bool {
	if len(value) == 0 || len(value) > 2048 || !utf8.ValidString(value) {
		return false
	}
	for _, c := range value {
		if unicode.IsControl(c) || unicode.IsSpace(c) {
			return false
		}
	}
	return true
}
