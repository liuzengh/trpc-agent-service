// Package postgres persists WebUI provider effects without duplicating result
// plaintext. The browser API resolves content through the encrypted ResultStore.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) PutMessage(ctx context.Context, value webui.Message) (webui.Message, error) {
	if s == nil || s.db == nil || invalid(value) {
		return webui.Message{}, runtime.ErrInvariantViolation
	}
	var bindingAccount string
	err := s.db.QueryRowContext(ctx, `SELECT external_account_id FROM channel_binding
WHERE tenant_id=$1 AND config_version=$2 AND binding_id=$3 AND channel='webui'`,
		value.TenantID, value.ConfigVersion, value.ChannelBindingID).Scan(&bindingAccount)
	if errors.Is(err, sql.ErrNoRows) {
		return webui.Message{}, runtime.ErrNotFound
	}
	if err != nil {
		return webui.Message{}, classify(ctx, err)
	}
	if bindingAccount != value.ExternalAccountID {
		return webui.Message{}, runtime.ErrVersionMismatch
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO webui_message(
tenant_id,config_version,channel_binding_id,external_account_id,external_user_id,external_chat_id,
request_id,client_request_id,provider_message_id,content_ref,content_digest)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (tenant_id,client_request_id) DO NOTHING`, value.TenantID, value.ConfigVersion, value.ChannelBindingID,
		value.ExternalAccountID, value.ExternalUserID, value.ExternalChatID, value.RequestID, value.ClientRequestID,
		value.ProviderMessageID, value.ContentRef, value.ContentDigest)
	if err != nil {
		return webui.Message{}, classify(ctx, err)
	}
	stored, err := s.GetMessageByClientRequestID(ctx, value.TenantID, value.ClientRequestID)
	if err != nil {
		return webui.Message{}, err
	}
	value.CreatedAt = stored.CreatedAt
	if stored != value {
		return webui.Message{}, runtime.ErrIdempotencyCollision
	}
	return stored, nil
}

func (s *Store) GetMessageByClientRequestID(ctx context.Context, tenantID, clientRequestID string) (webui.Message, error) {
	if s == nil || s.db == nil || tenantID == "" || clientRequestID == "" {
		return webui.Message{}, runtime.ErrTenantScope
	}
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id,config_version,channel_binding_id,external_account_id,
external_user_id,external_chat_id,request_id,client_request_id,provider_message_id,content_ref,content_digest,created_at
FROM webui_message WHERE tenant_id=$1 AND client_request_id=$2`, tenantID, clientRequestID)
	return scan(row)
}

func (s *Store) ListMessages(ctx context.Context, query webui.MessageQuery) ([]webui.Message, error) {
	if s == nil || s.db == nil || query.TenantID == "" || query.ChannelBindingID == "" || query.ExternalAccountID == "" ||
		query.ExternalUserID == "" || query.ExternalChatID == "" {
		return nil, runtime.ErrTenantScope
	}
	limit := query.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	after := query.After
	if after.IsZero() {
		after = time.Unix(0, 0).UTC()
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id,config_version,channel_binding_id,external_account_id,
external_user_id,external_chat_id,request_id,client_request_id,provider_message_id,content_ref,content_digest,created_at
FROM webui_message
WHERE tenant_id=$1 AND channel_binding_id=$2 AND external_account_id=$3 AND external_user_id=$4
  AND external_chat_id=$5 AND created_at>$6
ORDER BY created_at,provider_message_id LIMIT $7`, query.TenantID, query.ChannelBindingID, query.ExternalAccountID,
		query.ExternalUserID, query.ExternalChatID, after, limit)
	if err != nil {
		return nil, classify(ctx, err)
	}
	defer rows.Close()
	result := make([]webui.Message, 0, limit)
	for rows.Next() {
		value, scanErr := scan(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(ctx, err)
	}
	return result, nil
}

type scanner interface{ Scan(...any) error }

func scan(row scanner) (webui.Message, error) {
	var value webui.Message
	err := row.Scan(&value.TenantID, &value.ConfigVersion, &value.ChannelBindingID, &value.ExternalAccountID,
		&value.ExternalUserID, &value.ExternalChatID, &value.RequestID, &value.ClientRequestID,
		&value.ProviderMessageID, &value.ContentRef, &value.ContentDigest, &value.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return webui.Message{}, runtime.ErrNotFound
	}
	if err != nil {
		return webui.Message{}, runtime.ErrBackendUnavailable
	}
	return value, nil
}

func invalid(value webui.Message) bool {
	return value.TenantID == "" || value.ConfigVersion < 1 || value.ChannelBindingID == "" || value.ExternalAccountID == "" ||
		value.ExternalUserID == "" || value.ExternalChatID == "" || value.RequestID == "" || value.ClientRequestID == "" ||
		value.ProviderMessageID == "" || value.ContentRef == "" || len(value.ContentDigest) != 64
}

func classify(ctx context.Context, _ error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return runtime.ErrBackendUnavailable
}

var _ webui.Mailbox = (*Store)(nil)
var _ webui.MessageReader = (*Store)(nil)
