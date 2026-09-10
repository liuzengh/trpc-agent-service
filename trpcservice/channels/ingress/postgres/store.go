package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) PutBindingRoute(ctx context.Context, route ingress.BindingRoute) error {
	if s == nil || s.db == nil || route.OpaqueBindingID == "" || route.Channel == "" || route.RouteKeyDigest == "" ||
		route.TenantID == "" || route.AgentAppID == "" || route.ChannelBindingID == "" || route.ExternalAccountID == "" || route.TenantVersion < 1 ||
		route.BindingVersion < 1 || route.SecretRef.Ref == "" || route.SecretRef.Version < 1 ||
		route.IdentitySecretRef.Ref == "" || route.IdentitySecretRef.Version < 1 || route.SessionSecretRef.Ref == "" || route.SessionSecretRef.Version < 1 {
		return runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var tenantVersion, secretVersion int64
	var channel, agentAppID, externalAccountID, secretRef string
	err = tx.QueryRowContext(ctx, `SELECT t.version,cb.channel,cb.agent_app_id,cb.external_account_id,cb.secret_ref,cb.secret_version
FROM channel_binding cb JOIN tenant t ON t.tenant_id=cb.tenant_id
WHERE cb.tenant_id=$1 AND cb.config_version=$2 AND cb.binding_id=$3`,
		route.TenantID, route.BindingVersion, route.ChannelBindingID).
		Scan(&tenantVersion, &channel, &agentAppID, &externalAccountID, &secretRef, &secretVersion)
	if err != nil {
		return classify(err)
	}
	if tenantVersion != route.TenantVersion || channel != route.Channel || agentAppID != route.AgentAppID || externalAccountID != route.ExternalAccountID ||
		secretRef != route.SecretRef.Ref || secretVersion != route.SecretRef.Version {
		return runtime.ErrVersionMismatch
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO channel_public_route(channel,route_key_digest,opaque_binding_id,binding_version,enabled)
VALUES($1,$2,$3,$4,$5) ON CONFLICT (channel,route_key_digest) DO NOTHING`, route.Channel, route.RouteKeyDigest,
		route.OpaqueBindingID, route.BindingVersion, route.Enabled); err != nil {
		return classify(err)
	}
	var opaqueID string
	var bindingVersion int64
	var enabled bool
	if err = tx.QueryRowContext(ctx, `SELECT opaque_binding_id,binding_version,enabled FROM channel_public_route
WHERE channel=$1 AND route_key_digest=$2 FOR UPDATE`, route.Channel, route.RouteKeyDigest).Scan(&opaqueID, &bindingVersion, &enabled); err != nil {
		return classify(err)
	}
	if opaqueID != route.OpaqueBindingID || bindingVersion != route.BindingVersion || enabled != route.Enabled {
		if opaqueID != route.OpaqueBindingID || bindingVersion >= route.BindingVersion {
			return runtime.ErrIdempotencyCollision
		}
		if _, err = tx.ExecContext(ctx, `UPDATE channel_public_route SET binding_version=$3,enabled=$4,updated_at=now()
WHERE channel=$1 AND route_key_digest=$2 AND opaque_binding_id=$5 AND binding_version=$6`, route.Channel, route.RouteKeyDigest,
			route.BindingVersion, route.Enabled, route.OpaqueBindingID, bindingVersion); err != nil {
			return classify(err)
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO channel_binding_locator(opaque_binding_id,tenant_id,config_version,binding_id,
identity_secret_ref,identity_secret_version,session_secret_ref,session_secret_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (opaque_binding_id) DO NOTHING`, route.OpaqueBindingID, route.TenantID,
		route.BindingVersion, route.ChannelBindingID, route.IdentitySecretRef.Ref, route.IdentitySecretRef.Version,
		route.SessionSecretRef.Ref, route.SessionSecretRef.Version); err != nil {
		return classify(err)
	}
	var tenantID, bindingID, identityRef, sessionRef string
	var configVersion, identityVersion, sessionVersion int64
	if err = tx.QueryRowContext(ctx, `SELECT tenant_id,config_version,binding_id,identity_secret_ref,identity_secret_version,
session_secret_ref,session_secret_version FROM channel_binding_locator WHERE opaque_binding_id=$1 FOR UPDATE`,
		route.OpaqueBindingID).Scan(&tenantID, &configVersion, &bindingID, &identityRef, &identityVersion, &sessionRef, &sessionVersion); err != nil {
		return classify(err)
	}
	if tenantID != route.TenantID || bindingID != route.ChannelBindingID || configVersion != route.BindingVersion ||
		identityRef != route.IdentitySecretRef.Ref || identityVersion != route.IdentitySecretRef.Version ||
		sessionRef != route.SessionSecretRef.Ref || sessionVersion != route.SessionSecretRef.Version {
		if tenantID != route.TenantID || bindingID != route.ChannelBindingID || configVersion >= route.BindingVersion {
			return runtime.ErrIdempotencyCollision
		}
		if _, err = tx.ExecContext(ctx, `UPDATE channel_binding_locator SET config_version=$2,identity_secret_ref=$6,
identity_secret_version=$7,session_secret_ref=$8,session_secret_version=$9
WHERE opaque_binding_id=$1 AND tenant_id=$3 AND binding_id=$4 AND config_version=$5`, route.OpaqueBindingID,
			route.BindingVersion, route.TenantID, route.ChannelBindingID, configVersion, route.IdentitySecretRef.Ref,
			route.IdentitySecretRef.Version, route.SessionSecretRef.Ref, route.SessionSecretRef.Version); err != nil {
			return classify(err)
		}
	}
	return tx.Commit()
}

func (s *Store) ResolveBindingRoute(ctx context.Context, channel, routeDigest string) (ingress.BindingRoute, error) {
	if s == nil || s.db == nil || channel == "" || routeDigest == "" {
		return ingress.BindingRoute{}, runtime.ErrInvariantViolation
	}
	return getRoute(ctx, s.db, channel, routeDigest, "")
}

func (s *Store) IssueCandidate(ctx context.Context, record ingress.CandidateRecord) error {
	if s == nil || s.db == nil || record.TokenDigest == "" || record.OpaqueBindingID == "" || record.State != ingress.CandidateIssued ||
		record.Version != 0 || !record.ExpiresAt.After(record.IssuedAt) {
		return runtime.ErrInvariantViolation
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO channel_ingress_candidate(
candidate_token_digest,opaque_binding_id,channel,route_key_digest,purpose,binding_version,state,issued_at,expires_at,version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,0)`, record.TokenDigest, record.OpaqueBindingID, record.Channel,
		record.RouteKeyDigest, record.Purpose, record.BindingVersion, record.State, record.IssuedAt, record.ExpiresAt)
	return classify(err)
}

func (s *Store) AcquireCandidate(ctx context.Context, tokenDigest string, bindingVersion int64, now time.Time) (ingress.CandidateRecord, ingress.BindingRoute, error) {
	tx, err := s.begin(ctx, tokenDigest, bindingVersion, now)
	if err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	defer func() { _ = tx.Rollback() }()
	record, err := getCandidateForUpdate(ctx, tx, tokenDigest)
	if err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	if record.State != ingress.CandidateIssued || record.BindingVersion != bindingVersion || !record.ExpiresAt.After(now) {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionConflict
	}
	route, err := getRoute(ctx, tx, record.Channel, record.RouteKeyDigest, record.OpaqueBindingID)
	if err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	if !route.Enabled || route.BindingVersion != bindingVersion {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	if err = tx.QueryRowContext(ctx, `UPDATE channel_ingress_candidate SET state='verifier_acquired',version=version+1
WHERE candidate_token_digest=$1 AND version=$2 RETURNING version`, tokenDigest, record.Version).Scan(&record.Version); err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, classify(err)
	}
	record.State = ingress.CandidateVerifierAcquired
	if err = tx.Commit(); err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	return record, route, nil
}

func (s *Store) MarkCandidateVerified(ctx context.Context, tokenDigest string, version int64, receiptDigest, identityDigest string, verifiedAt time.Time) (ingress.CandidateRecord, error) {
	if s == nil || s.db == nil || tokenDigest == "" || receiptDigest == "" || identityDigest == "" || verifiedAt.IsZero() {
		return ingress.CandidateRecord{}, runtime.ErrInvariantViolation
	}
	row := s.db.QueryRowContext(ctx, `UPDATE channel_ingress_candidate SET state='verified',receipt_token_digest=$3,
protocol_identity_digest=$4,verified_at=$5,version=version+1
WHERE candidate_token_digest=$1 AND version=$2 AND state='verifier_acquired'
RETURNING candidate_token_digest,opaque_binding_id,channel,route_key_digest,purpose,binding_version,state,
COALESCE(receipt_token_digest,''),COALESCE(protocol_identity_digest,''),issued_at,expires_at,COALESCE(verified_at,'epoch'),version`,
		tokenDigest, version, receiptDigest, identityDigest, verifiedAt)
	record, err := scanCandidate(row)
	if errors.Is(err, runtime.ErrNotFound) {
		return ingress.CandidateRecord{}, runtime.ErrVersionConflict
	}
	return record, err
}

func (s *Store) PromoteCandidate(ctx context.Context, tokenDigest string, bindingVersion int64, receiptDigest, identityDigest string, verifiedAt, now time.Time) (ingress.CandidateRecord, ingress.BindingRoute, error) {
	tx, err := s.begin(ctx, tokenDigest, bindingVersion, now)
	if err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	defer func() { _ = tx.Rollback() }()
	record, err := getCandidateForUpdate(ctx, tx, tokenDigest)
	if err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	if record.State != ingress.CandidateVerified || record.BindingVersion != bindingVersion || record.ReceiptDigest != receiptDigest ||
		record.ProtocolIdentityDigest != identityDigest || record.VerifiedAt.UTC().UnixMicro() != verifiedAt.UTC().UnixMicro() || !record.ExpiresAt.After(now) {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionConflict
	}
	route, err := getRoute(ctx, tx, record.Channel, record.RouteKeyDigest, record.OpaqueBindingID)
	if err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	if !route.Enabled || route.BindingVersion != bindingVersion {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, runtime.ErrVersionMismatch
	}
	if err = tx.QueryRowContext(ctx, `UPDATE channel_ingress_candidate SET state='promoted',version=version+1
WHERE candidate_token_digest=$1 AND version=$2 RETURNING version`, tokenDigest, record.Version).Scan(&record.Version); err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, classify(err)
	}
	record.State = ingress.CandidatePromoted
	if err = tx.Commit(); err != nil {
		return ingress.CandidateRecord{}, ingress.BindingRoute{}, err
	}
	return record, route, nil
}

func (s *Store) BurnCandidate(ctx context.Context, tokenDigest string, version int64) error {
	if s == nil || s.db == nil || tokenDigest == "" {
		return runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `UPDATE channel_ingress_candidate SET state='burned',version=version+1
WHERE candidate_token_digest=$1 AND version=$2 AND state IN ('issued','verifier_acquired')`, tokenDigest, version)
	if err != nil {
		return classify(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}

func (s *Store) BurnExpiredCandidates(ctx context.Context, now time.Time, limit int) (int, error) {
	if s == nil || s.db == nil || now.IsZero() {
		return 0, runtime.ErrInvariantViolation
	}
	if limit <= 0 {
		limit = 100
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `WITH expired AS (
SELECT candidate_token_digest FROM channel_ingress_candidate
WHERE expires_at <= $1 AND state IN ('issued','verifier_acquired','verified')
ORDER BY expires_at,candidate_token_digest
FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE channel_ingress_candidate c
SET state='burned',receipt_token_digest=NULL,protocol_identity_digest=NULL,verified_at=NULL,version=c.version+1
FROM expired
WHERE c.candidate_token_digest=expired.candidate_token_digest`, now.UTC(), limit)
	if err != nil {
		return 0, classify(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int(count), nil
}

func (s *Store) begin(ctx context.Context, tokenDigest string, bindingVersion int64, now time.Time) (*sql.Tx, error) {
	if s == nil || s.db == nil || tokenDigest == "" || bindingVersion < 1 || now.IsZero() {
		return nil, runtime.ErrInvariantViolation
	}
	return s.db.BeginTx(ctx, nil)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getRoute(ctx context.Context, q queryer, channel, routeDigest, opaqueID string) (ingress.BindingRoute, error) {
	var route ingress.BindingRoute
	var enabled bool
	err := q.QueryRowContext(ctx, `SELECT pr.opaque_binding_id,pr.channel,pr.route_key_digest,l.tenant_id,cb.agent_app_id,
cb.binding_id,cb.external_account_id,t.version,pr.binding_version,cb.secret_ref,cb.secret_version,
COALESCE(l.identity_secret_ref,''),COALESCE(l.identity_secret_version,0),COALESCE(l.session_secret_ref,''),COALESCE(l.session_secret_version,0),
(pr.enabled AND t.active_config_version=l.config_version)
FROM channel_public_route pr JOIN channel_binding_locator l ON l.opaque_binding_id=pr.opaque_binding_id
JOIN channel_binding cb ON cb.tenant_id=l.tenant_id AND cb.config_version=l.config_version AND cb.binding_id=l.binding_id
JOIN tenant t ON t.tenant_id=l.tenant_id
WHERE pr.channel=$1 AND pr.route_key_digest=$2 AND ($3='' OR pr.opaque_binding_id=$3)`, channel, routeDigest, opaqueID).
		Scan(&route.OpaqueBindingID, &route.Channel, &route.RouteKeyDigest, &route.TenantID, &route.AgentAppID,
			&route.ChannelBindingID, &route.ExternalAccountID, &route.TenantVersion, &route.BindingVersion, &route.SecretRef.Ref,
			&route.SecretRef.Version, &route.IdentitySecretRef.Ref, &route.IdentitySecretRef.Version,
			&route.SessionSecretRef.Ref, &route.SessionSecretRef.Version, &enabled)
	if err != nil {
		return ingress.BindingRoute{}, classify(err)
	}
	route.Enabled = enabled
	return route, nil
}

func getCandidateForUpdate(ctx context.Context, tx *sql.Tx, tokenDigest string) (ingress.CandidateRecord, error) {
	return scanCandidate(tx.QueryRowContext(ctx, `SELECT candidate_token_digest,opaque_binding_id,channel,route_key_digest,purpose,
binding_version,state,COALESCE(receipt_token_digest,''),COALESCE(protocol_identity_digest,''),issued_at,expires_at,
COALESCE(verified_at,'epoch'),version FROM channel_ingress_candidate WHERE candidate_token_digest=$1 FOR UPDATE`, tokenDigest))
}

type scanner interface{ Scan(...any) error }

func scanCandidate(row scanner) (ingress.CandidateRecord, error) {
	var record ingress.CandidateRecord
	err := row.Scan(&record.TokenDigest, &record.OpaqueBindingID, &record.Channel, &record.RouteKeyDigest, &record.Purpose,
		&record.BindingVersion, &record.State, &record.ReceiptDigest, &record.ProtocolIdentityDigest, &record.IssuedAt,
		&record.ExpiresAt, &record.VerifiedAt, &record.Version)
	if err != nil {
		return ingress.CandidateRecord{}, classify(err)
	}
	return record, nil
}

type sqlStater interface{ SQLState() string }

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "23503", "23514", "22023":
			return runtime.ErrInvariantViolation
		case "40001":
			return runtime.ErrVersionConflict
		}
	}
	return err
}

var _ ingress.Store = (*Store)(nil)
