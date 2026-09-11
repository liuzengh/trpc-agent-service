package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/usagepolicy/application"
)

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }
func decode(raw []byte) (governancev1.Policy, error) {
	var p governancev1.Policy
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || p.Validate() != nil {
		return p, application.ErrUnavailable
	}
	return p, nil
}
func (s *Store) Get(ctx context.Context, tenant string) (governancev1.Policy, bool, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT policy_jsonb FROM tenant_usage_policies WHERE tenant_id=$1`, tenant).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return governancev1.Policy{}, false, nil
	}
	if err != nil {
		return governancev1.Policy{}, false, err
	}
	p, err := decode(raw)
	return p, err == nil, err
}
func (s *Store) Replace(ctx context.Context, user, key string, expected int64, digest string, candidate governancev1.Policy) (governancev1.Policy, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return governancev1.Policy{}, err
	}
	defer tx.Rollback(ctx)
	var priorDigest string
	var priorRaw []byte
	err = tx.QueryRow(ctx, `SELECT request_digest,result_jsonb FROM tenant_usage_policy_receipts WHERE tenant_id=$1 AND idempotency_key=$2`, candidate.TenantID, key).Scan(&priorDigest, &priorRaw)
	if err == nil {
		if priorDigest != digest {
			return governancev1.Policy{}, application.ErrConflict
		}
		p, e := decode(priorRaw)
		if e != nil {
			return governancev1.Policy{}, e
		}
		return p, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return governancev1.Policy{}, err
	}
	var current int64
	err = tx.QueryRow(ctx, `SELECT revision FROM tenant_usage_policies WHERE tenant_id=$1 FOR UPDATE`, candidate.TenantID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		current = 0
	} else if err != nil {
		return governancev1.Policy{}, err
	}
	if current != expected {
		return governancev1.Policy{}, application.ErrConflict
	}
	raw, _ := json.Marshal(candidate)
	if current == 0 {
		_, err = tx.Exec(ctx, `INSERT INTO tenant_usage_policies(tenant_id,revision,policy_jsonb,updated_by) VALUES($1,$2,$3,$4)`, candidate.TenantID, candidate.Revision, raw, user)
	} else {
		_, err = tx.Exec(ctx, `UPDATE tenant_usage_policies SET revision=$2,policy_jsonb=$3,updated_by=$4,updated_at=clock_timestamp() WHERE tenant_id=$1 AND revision=$5`, candidate.TenantID, candidate.Revision, raw, user, current)
	}
	if err != nil {
		return governancev1.Policy{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO tenant_usage_policy_receipts(tenant_id,idempotency_key,request_digest,result_jsonb) VALUES($1,$2,$3,$4)`, candidate.TenantID, key, digest, raw)
	if err != nil {
		return governancev1.Policy{}, err
	}
	return candidate, tx.Commit(ctx)
}
