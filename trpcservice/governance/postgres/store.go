package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct {
	db  *sql.DB
	now func() time.Time
}

func New(db *sql.DB) *Store                                { return &Store{db: db} }
func NewWithClock(db *sql.DB, now func() time.Time) *Store { return &Store{db: db, now: now} }

func (s *Store) PublishPolicy(ctx context.Context, value governance.PolicySnapshot) error {
	digest, payload, err := governance.PolicyDigest(value.Policy)
	if err != nil {
		return err
	}
	if value.TenantID == "" || value.Version < 1 || value.ContentDigest != digest {
		return runtime.ErrInvariantViolation
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO policy_snapshot(tenant_id,policy_version,schema_version,payload,content_digest,pricing_version,state,published_at)
VALUES($1,$2,$3,$4::jsonb,$5,$6,'published',$7) ON CONFLICT (tenant_id,policy_version) DO NOTHING`, value.TenantID, value.Version, value.SchemaVersion, payload, value.ContentDigest, nullableInt(value.Policy.PricingVersion), value.PublishedAt)
	if err != nil {
		return translate(err)
	}
	var stored string
	if err = s.db.QueryRowContext(ctx, `SELECT content_digest FROM policy_snapshot WHERE tenant_id=$1 AND policy_version=$2`, value.TenantID, value.Version).Scan(&stored); err != nil {
		return translate(err)
	}
	if stored != value.ContentDigest {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}
func (s *Store) PublishPricing(ctx context.Context, value governance.PricingSnapshot) error {
	digest, payload, err := governance.PricingDigest(value.Pricing)
	if err != nil {
		return err
	}
	if value.TenantID == "" || value.Version < 1 || value.ContentDigest != digest {
		return runtime.ErrInvariantViolation
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO pricing_snapshot(tenant_id,pricing_version,schema_version,payload,content_digest,state,published_at)
VALUES($1,$2,$3,$4::jsonb,$5,'published',$6) ON CONFLICT (tenant_id,pricing_version) DO NOTHING`, value.TenantID, value.Version, value.SchemaVersion, payload, value.ContentDigest, value.PublishedAt)
	if err != nil {
		return translate(err)
	}
	var stored string
	if err = s.db.QueryRowContext(ctx, `SELECT content_digest FROM pricing_snapshot WHERE tenant_id=$1 AND pricing_version=$2`, value.TenantID, value.Version).Scan(&stored); err != nil {
		return translate(err)
	}
	if stored != value.ContentDigest {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}
func (s *Store) GetPolicy(ctx context.Context, tenantID string, version int64) (governance.PolicySnapshot, error) {
	var value governance.PolicySnapshot
	var raw []byte
	value.TenantID, value.Version = tenantID, version
	err := s.db.QueryRowContext(ctx, `SELECT schema_version,payload,content_digest,published_at FROM policy_snapshot WHERE tenant_id=$1 AND policy_version=$2 AND state='published'`, tenantID, version).
		Scan(&value.SchemaVersion, &raw, &value.ContentDigest, &value.PublishedAt)
	if err != nil {
		return governance.PolicySnapshot{}, translate(err)
	}
	policy, err := governance.DecodePolicyV1(raw)
	if err != nil {
		return governance.PolicySnapshot{}, err
	}
	digest, _, err := governance.PolicyDigest(policy)
	if err != nil || digest != value.ContentDigest || value.SchemaVersion != governance.CurrentPolicySchemaVersion {
		return governance.PolicySnapshot{}, runtime.ErrInvariantViolation
	}
	value.Policy = policy
	return value, nil
}
func (s *Store) GetPricing(ctx context.Context, tenantID string, version int64) (governance.PricingSnapshot, error) {
	var value governance.PricingSnapshot
	var raw []byte
	value.TenantID, value.Version = tenantID, version
	err := s.db.QueryRowContext(ctx, `SELECT schema_version,payload,content_digest,published_at FROM pricing_snapshot WHERE tenant_id=$1 AND pricing_version=$2 AND state='published'`, tenantID, version).
		Scan(&value.SchemaVersion, &raw, &value.ContentDigest, &value.PublishedAt)
	if err != nil {
		return governance.PricingSnapshot{}, translate(err)
	}
	if err := json.Unmarshal(raw, &value.Pricing); err != nil {
		return governance.PricingSnapshot{}, runtime.ErrInvariantViolation
	}
	digest, _, err := governance.PricingDigest(value.Pricing)
	if err != nil || digest != value.ContentDigest || value.SchemaVersion != governance.CurrentPolicySchemaVersion {
		return governance.PricingSnapshot{}, runtime.ErrInvariantViolation
	}
	value.Pricing = governance.NormalizePricing(value.Pricing)
	return value, nil
}

func (s *Store) Reserve(ctx context.Context, in governance.ReserveRequest) (governance.Reservation, error) {
	id, err := governance.StableReservationID(in)
	if err != nil {
		return governance.Reservation{}, err
	}
	if in.PolicyVersion < 1 || in.MaxCostMicros < 0 || in.MaxTokens < 0 {
		return governance.Reservation{}, runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return governance.Reservation{}, err
	}
	defer tx.Rollback()
	if existing, getErr := getReservation(ctx, tx, in.TenantID, id); getErr == nil {
		if existing.RequestID != in.RequestID || existing.ResourceID != in.ResourceID || existing.AttemptClass != in.AttemptClass || existing.PolicyVersion != in.PolicyVersion || existing.PricingVersion != in.PricingVersion || existing.ReservedCostMicros != in.MaxCostMicros || existing.ReservedTokens != in.MaxTokens {
			return governance.Reservation{}, runtime.ErrIdempotencyCollision
		}
		return existing, nil
	} else if !errors.Is(getErr, runtime.ErrNotFound) {
		return governance.Reservation{}, getErr
	}
	var maxCost, maxTokens sql.NullInt64
	if err = tx.QueryRowContext(ctx, `SELECT monthly_cost_budget_micros,monthly_token_budget FROM tenant WHERE tenant_id=$1 FOR UPDATE`, in.TenantID).Scan(&maxCost, &maxTokens); err != nil {
		return governance.Reservation{}, translate(err)
	}
	if (maxCost.Valid && in.MaxCostMicros == 0) || (maxTokens.Valid && in.MaxTokens == 0) {
		return governance.Reservation{}, runtime.ErrCapabilityUnsupported
	}
	now := s.current()
	period := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var usedCost, usedTokens int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(sum(CASE WHEN state='settled' THEN actual_cost_micros ELSE reserved_cost_micros END),0),COALESCE(sum(CASE WHEN state='settled' THEN input_tokens+output_tokens ELSE reserved_tokens END),0) FROM budget_reservation WHERE tenant_id=$1 AND budget_period=$2 AND state IN ('reserved','settled')`, in.TenantID, period).Scan(&usedCost, &usedTokens); err != nil {
		return governance.Reservation{}, err
	}
	if (maxCost.Valid && in.MaxCostMicros > maxCost.Int64-usedCost) || (maxTokens.Valid && in.MaxTokens > maxTokens.Int64-usedTokens) {
		return governance.Reservation{}, runtime.ErrCapabilityUnsupported
	}
	value := governance.Reservation{ReservationID: id, TenantID: in.TenantID, RequestID: in.RequestID, ResourceID: in.ResourceID, AttemptClass: in.AttemptClass, PolicyVersion: in.PolicyVersion, PricingVersion: in.PricingVersion, ReservedCostMicros: in.MaxCostMicros, ReservedTokens: in.MaxTokens, State: governance.ReservationReserved, Version: 1}
	_, err = tx.ExecContext(ctx, `INSERT INTO budget_reservation(tenant_id,reservation_id,request_id,resource_id,attempt_class,policy_version,pricing_version,budget_period,reserved_cost_micros,reserved_tokens,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'reserved')`, in.TenantID, id, in.RequestID, in.ResourceID, in.AttemptClass, in.PolicyVersion, nullableInt(in.PricingVersion), period, in.MaxCostMicros, in.MaxTokens)
	if err != nil {
		return governance.Reservation{}, translate(err)
	}
	if err = tx.Commit(); err != nil {
		return governance.Reservation{}, err
	}
	return value, nil
}
func (s *Store) Settle(ctx context.Context, in governance.SettleRequest) (governance.Reservation, error) {
	if in.TenantID == "" || in.ReservationID == "" || in.RequestID == "" || in.Stage == "" || in.UsageKind == "" || in.ExpectedVersion < 1 || in.ActualCostMicros < 0 || in.Usage.InputTokens < 0 || in.Usage.OutputTokens < 0 || in.Usage.CachedInputTokens < 0 || in.Usage.CachedInputTokens > in.Usage.InputTokens {
		return governance.Reservation{}, runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return governance.Reservation{}, err
	}
	defer tx.Rollback()
	var priorID string
	var pi, po, pc, pcost int64
	err = tx.QueryRowContext(ctx, `SELECT reservation_id,input_tokens,output_tokens,cached_input_tokens,cost_micros FROM usage_ledger WHERE tenant_id=$1 AND request_id=$2 AND stage=$3 AND usage_kind=$4`, in.TenantID, in.RequestID, in.Stage, in.UsageKind).Scan(&priorID, &pi, &po, &pc, &pcost)
	if err == nil {
		if priorID != in.ReservationID || pi != in.Usage.InputTokens || po != in.Usage.OutputTokens || pc != in.Usage.CachedInputTokens || pcost != in.ActualCostMicros {
			return governance.Reservation{}, runtime.ErrIdempotencyCollision
		}
		return getReservation(ctx, tx, in.TenantID, in.ReservationID)
	}
	if err != sql.ErrNoRows {
		return governance.Reservation{}, err
	}
	value, getErr := getReservationForUpdate(ctx, tx, in.TenantID, in.ReservationID)
	if getErr != nil {
		return governance.Reservation{}, getErr
	}
	if value.RequestID != in.RequestID || value.State != governance.ReservationReserved || value.Version != in.ExpectedVersion || in.ActualCostMicros > value.ReservedCostMicros || (value.ReservedTokens > 0 && in.Usage.InputTokens+in.Usage.OutputTokens > value.ReservedTokens) {
		return governance.Reservation{}, runtime.ErrVersionConflict
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO usage_ledger(tenant_id,request_id,stage,usage_kind,reservation_id,input_tokens,output_tokens,cached_input_tokens,cost_micros) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, in.TenantID, in.RequestID, in.Stage, in.UsageKind, in.ReservationID, in.Usage.InputTokens, in.Usage.OutputTokens, in.Usage.CachedInputTokens, in.ActualCostMicros)
	if err != nil {
		return governance.Reservation{}, translate(err)
	}
	value.State = governance.ReservationSettled
	value.ActualCostMicros = in.ActualCostMicros
	value.InputTokens = in.Usage.InputTokens
	value.OutputTokens = in.Usage.OutputTokens
	value.Version++
	_, err = tx.ExecContext(ctx, `UPDATE budget_reservation SET state='settled',actual_cost_micros=$3,input_tokens=$4,output_tokens=$5,version=version+1,updated_at=now() WHERE tenant_id=$1 AND reservation_id=$2 AND state='reserved' AND version=$6`, in.TenantID, in.ReservationID, in.ActualCostMicros, in.Usage.InputTokens, in.Usage.OutputTokens, in.ExpectedVersion)
	if err != nil {
		return governance.Reservation{}, translate(err)
	}
	if err = tx.Commit(); err != nil {
		return governance.Reservation{}, err
	}
	return value, nil
}
func (s *Store) Refund(ctx context.Context, tenantID, id string, expected int64, reason string) (governance.Reservation, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return governance.Reservation{}, err
	}
	defer tx.Rollback()
	value, err := getReservationForUpdate(ctx, tx, tenantID, id)
	if err != nil {
		return governance.Reservation{}, err
	}
	if value.State == governance.ReservationRefunded {
		return value, nil
	}
	if value.State != governance.ReservationReserved || value.Version != expected {
		return governance.Reservation{}, runtime.ErrVersionConflict
	}
	value.State = governance.ReservationRefunded
	value.Version++
	_, err = tx.ExecContext(ctx, `UPDATE budget_reservation SET state='refunded',refund_reason=$3,version=version+1,updated_at=now() WHERE tenant_id=$1 AND reservation_id=$2 AND state='reserved' AND version=$4`, tenantID, id, reason, expected)
	if err != nil {
		return governance.Reservation{}, translate(err)
	}
	if err = tx.Commit(); err != nil {
		return governance.Reservation{}, err
	}
	return value, nil
}
func (s *Store) GetReservation(ctx context.Context, tenantID, id string) (governance.Reservation, error) {
	return getReservation(ctx, s.db, tenantID, id)
}
func (s *Store) RecordDecision(ctx context.Context, value governance.Decision) error {
	value.RuleIDs = append([]string{}, value.RuleIDs...)
	rules, err := json.Marshal(value.RuleIDs)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO governance_decision(tenant_id,decision_id,request_id,stage,action,reason_code,policy_version,rule_ids,reservation_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9) ON CONFLICT (tenant_id,decision_id) DO NOTHING`, value.TenantID, value.DecisionID, value.RequestID, value.Stage, value.Action, value.ReasonCode, value.PolicyVersion, rules, nullableString(value.ReservationID))
	if err != nil {
		return translate(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
VALUES($1,$2,'audit',$3,$4,$5,$6) ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING`, value.TenantID, "audit-"+value.DecisionID,
		value.RequestID, value.PolicyVersion, "governance:"+value.DecisionID, "governance://"+value.TenantID+"/"+value.DecisionID)
	if err != nil {
		return translate(err)
	}
	var stored governance.Decision
	var raw []byte
	var reservation sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT request_id,stage,action,reason_code,policy_version,rule_ids,reservation_id FROM governance_decision WHERE tenant_id=$1 AND decision_id=$2`, value.TenantID, value.DecisionID).Scan(&stored.RequestID, &stored.Stage, &stored.Action, &stored.ReasonCode, &stored.PolicyVersion, &raw, &reservation)
	if err != nil {
		return translate(err)
	}
	stored.TenantID, stored.DecisionID, stored.ReservationID = value.TenantID, value.DecisionID, reservation.String
	if err = json.Unmarshal(raw, &stored.RuleIDs); err != nil {
		return runtime.ErrInvariantViolation
	}
	if !reflect.DeepEqual(stored, value) {
		return runtime.ErrIdempotencyCollision
	}
	return tx.Commit()
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getReservation(ctx context.Context, q queryer, tenantID, id string) (governance.Reservation, error) {
	var value governance.Reservation
	var pricing sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT reservation_id,tenant_id,request_id,resource_id,attempt_class,policy_version,pricing_version,reserved_cost_micros,reserved_tokens,actual_cost_micros,input_tokens,output_tokens,state,version FROM budget_reservation WHERE tenant_id=$1 AND reservation_id=$2`, tenantID, id).Scan(&value.ReservationID, &value.TenantID, &value.RequestID, &value.ResourceID, &value.AttemptClass, &value.PolicyVersion, &pricing, &value.ReservedCostMicros, &value.ReservedTokens, &value.ActualCostMicros, &value.InputTokens, &value.OutputTokens, &value.State, &value.Version)
	if err != nil {
		return governance.Reservation{}, translate(err)
	}
	value.PricingVersion = pricing.Int64
	return value, nil
}
func getReservationForUpdate(ctx context.Context, tx *sql.Tx, tenantID, id string) (governance.Reservation, error) {
	var value governance.Reservation
	var pricing sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT reservation_id,tenant_id,request_id,resource_id,attempt_class,policy_version,pricing_version,reserved_cost_micros,reserved_tokens,actual_cost_micros,input_tokens,output_tokens,state,version FROM budget_reservation WHERE tenant_id=$1 AND reservation_id=$2 FOR UPDATE`, tenantID, id).Scan(&value.ReservationID, &value.TenantID, &value.RequestID, &value.ResourceID, &value.AttemptClass, &value.PolicyVersion, &pricing, &value.ReservedCostMicros, &value.ReservedTokens, &value.ActualCostMicros, &value.InputTokens, &value.OutputTokens, &value.State, &value.Version)
	if err != nil {
		return governance.Reservation{}, translate(err)
	}
	value.PricingVersion = pricing.Int64
	return value, nil
}
func (s *Store) current() time.Time {
	if s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}
func nullableInt(value int64) any {
	if value == 0 {
		return nil
	}
	return value
}
func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "23503", "42501":
			return runtime.ErrTenantScope
		case "40001":
			return runtime.ErrVersionConflict
		case "55000":
			return runtime.ErrInvariantViolation
		}
	}
	if strings.Contains(strings.ToLower(err.Error()), "immutable") {
		return runtime.ErrInvariantViolation
	}
	return err
}

var _ governance.Repository = (*Store)(nil)
var _ governance.Ledger = (*Store)(nil)
var _ governance.DecisionRecorder = (*Store)(nil)
