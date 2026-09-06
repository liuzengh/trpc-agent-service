package wecommcp

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type PostgresStore struct{ db *sql.DB }

func (s *PostgresStore) Checkpoint(ctx context.Context, key PollKey, hash string, start time.Time) (Checkpoint, error) {
	_, err := s.db.ExecContext(ctx, `INSERT INTO channel_poll_checkpoint(tenant_id,channel_binding_id,chat_hash,config_hash,through_at,floor_at) VALUES($1,$2,$3,$4,$5,$5) ON CONFLICT DO NOTHING`, key.TenantID, key.BindingID, key.ChatHash, hash, start)
	if err != nil {
		return Checkpoint{}, errors.New("cannot initialize channel checkpoint")
	}
	var value Checkpoint
	var floor sql.NullTime
	err = s.db.QueryRowContext(ctx, `SELECT config_hash,through_at,version,floor_at FROM channel_poll_checkpoint WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3`, key.TenantID, key.BindingID, key.ChatHash).Scan(&value.ConfigHash, &value.Through, &value.Version, &floor)
	if err != nil {
		return value, errors.New("cannot read channel checkpoint")
	}
	if value.ConfigHash != hash {
		return Checkpoint{}, ErrStateConflict
	}
	if floor.Valid {
		value.Floor = floor.Time
	} else {
		value.Floor = start
	}
	return value, nil
}
func (s *PostgresStore) Advance(ctx context.Context, key PollKey, previous Checkpoint, through time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE channel_poll_checkpoint SET through_at=$6,floor_at=COALESCE(floor_at,$7),version=version+1,updated_at=now() WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3 AND config_hash=$4 AND version=$5 AND through_at<=$6`, key.TenantID, key.BindingID, key.ChatHash, previous.ConfigHash, previous.Version, through, previous.Floor)
	return changed(result, err)
}
func (s *PostgresStore) Seen(ctx context.Context, key PollKey, id string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_poll_seen WHERE tenant_id=$1 AND channel_binding_id=$2 AND chat_hash=$3 AND message_fingerprint=$4)`, key.TenantID, key.BindingID, key.ChatHash, id).Scan(&exists)
	if err != nil {
		return false, errors.New("cannot read channel deduplication state")
	}
	return exists, nil
}
func (s *PostgresStore) MarkSeen(ctx context.Context, key PollKey, id string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO channel_poll_seen(tenant_id,channel_binding_id,chat_hash,message_fingerprint) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, key.TenantID, key.BindingID, key.ChatHash, id)
	if err != nil {
		return errors.New("cannot persist channel deduplication state")
	}
	return nil
}
func (s *PostgresStore) BeginDelivery(ctx context.Context, key DeliveryKey, hash string) (DeliveryState, bool, error) {
	result, err := s.db.ExecContext(ctx, `INSERT INTO channel_delivery_attempt(tenant_id,channel_binding_id,outbound_id,input_hash,status) VALUES($1,$2,$3,$4,'attempting') ON CONFLICT DO NOTHING`, key.TenantID, key.BindingID, key.OutboundID, hash)
	if err != nil {
		return DeliveryState{}, false, errors.New("cannot record channel delivery attempt")
	}
	n, err := result.RowsAffected()
	if err != nil {
		return DeliveryState{}, false, errors.New("cannot confirm channel delivery claim")
	}
	var value DeliveryState
	err = s.db.QueryRowContext(ctx, `SELECT input_hash,status,updated_at FROM channel_delivery_attempt WHERE tenant_id=$1 AND channel_binding_id=$2 AND outbound_id=$3`, key.TenantID, key.BindingID, key.OutboundID).Scan(&value.InputHash, &value.Status, &value.UpdatedAt)
	if err != nil {
		return value, false, errors.New("cannot read channel delivery attempt")
	}
	if value.InputHash != hash {
		return value, false, ErrStateConflict
	}
	return value, n == 1, nil
}
func (s *PostgresStore) FinishDelivery(ctx context.Context, key DeliveryKey, hash, status string) error {
	if status != "sent" && status != "rejected" && status != "unknown" {
		return ErrStateConflict
	}
	result, err := s.db.ExecContext(ctx, `UPDATE channel_delivery_attempt SET status=$5,updated_at=now() WHERE tenant_id=$1 AND channel_binding_id=$2 AND outbound_id=$3 AND input_hash=$4 AND status='attempting'`, key.TenantID, key.BindingID, key.OutboundID, hash, status)
	return changed(result, err)
}
func changed(result sql.Result, err error) error {
	if err != nil {
		return errors.New("cannot persist channel state")
	}
	n, err := result.RowsAffected()
	if err != nil {
		return errors.New("cannot confirm channel state write")
	}
	if n != 1 {
		return ErrStateConflict
	}
	return nil
}
func (s *PostgresStore) Ready(ctx context.Context) error {
	var ok bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('channel_poll_checkpoint') IS NOT NULL AND to_regclass('channel_poll_seen') IS NOT NULL AND to_regclass('channel_delivery_attempt') IS NOT NULL AND to_regclass('channel_message_rejection') IS NOT NULL`).Scan(&ok); err != nil || !ok {
		return errors.New("channel state schema unavailable")
	}
	return nil
}
