package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type rowQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func findReceipt(ctx context.Context, q rowQuery, k application.ReceiptKey) (application.Receipt, bool, error) {
	r := application.Receipt{Key: k}
	err := q.QueryRow(ctx, `SELECT mac_key_id,request_mac,result_jsonb,created_by,created_at FROM channel_command_receipts WHERE tenant_id=$1 AND operation=$2 AND scope_id=$3 AND key_hash=$4`, k.TenantID, k.Operation, k.ScopeID, k.KeyHash).Scan(&r.MACKeyID, &r.RequestMAC, &r.Result, &r.CreatedBy, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, dbError(err)
	}
	if err = validateReceipt(r); err != nil {
		return r, false, err
	}
	return r, true, nil
}
func (s *Store) FindReceipt(ctx context.Context, k application.ReceiptKey) (application.Receipt, bool, error) {
	return findReceipt(ctx, s.db, k)
}
func (t *writeTx) FindReceipt(ctx context.Context, k application.ReceiptKey) (application.Receipt, bool, error) {
	if k.TenantID != t.scope.Actor.TenantID {
		return application.Receipt{}, false, application.ErrPermissionDenied
	}
	return findReceipt(ctx, t.tx, k)
}
func (t *writeTx) SaveReceipt(ctx context.Context, r application.Receipt) error {
	if r.Key.TenantID != t.scope.Actor.TenantID || r.CreatedBy != t.scope.Actor.UserID {
		return application.ErrPermissionDenied
	}
	if err := validateReceipt(r); err != nil {
		return err
	}
	_, err := t.tx.Exec(ctx, `INSERT INTO channel_command_receipts(tenant_id,operation,scope_id,key_hash,mac_key_id,request_mac,result_jsonb,created_by,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.Key.TenantID, r.Key.Operation, r.Key.ScopeID, r.Key.KeyHash, r.MACKeyID, r.RequestMAC, []byte(r.Result), r.CreatedBy, r.CreatedAt)
	return sqlResult(err)
}
func validateReceipt(r application.Receipt) error {
	if !domain.ValidID(r.Key.TenantID) || !domain.ValidID(r.Key.ScopeID) || !domain.ValidID(r.CreatedBy) || r.MACKeyID == "" || len(r.Key.KeyHash) != 64 || len(r.RequestMAC) != 64 || len(r.Result) > 64*1024 {
		return integrity()
	}
	var v any
	if json.Unmarshal(r.Result, &v) != nil {
		return integrity()
	}
	var walk func(any) bool
	walk = func(v any) bool {
		switch value := v.(type) {
		case map[string]any:
			for k, v := range value {
				switch strings.ToLower(k) {
				case "value", "ciphertext", "credential_id", "key_id", "request_mac", "request_digest":
					return false
				}
				if !walk(v) {
					return false
				}
			}
		case []any:
			for _, v := range value {
				if !walk(v) {
					return false
				}
			}
		}
		return true
	}
	if !walk(v) {
		return integrity()
	}
	return nil
}
func sqlResult(err error) error {
	if err != nil {
		return dbError(err)
	}
	return nil
}
