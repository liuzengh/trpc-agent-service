package wecommcp

import (
	"context"
	"errors"
	"slices"
	"time"
)

func validRejection(r RejectedMessage) bool {
	if r.Fingerprint == "" || len(r.Fingerprint) > 128 || r.WindowFrom.IsZero() || !r.WindowTo.After(r.WindowFrom) {
		return false
	}
	switch r.Reason {
	case "invalid_identity", "invalid_message", "invalid_timestamp_or_type", "unsupported_type", "oversized_text":
		return true
	}
	return false
}
func (s *MemoryStore) RecordRejection(ctx context.Context, key PollKey, r RejectedMessage) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !validRejection(r) {
		return false, errors.New("invalid rejection metadata")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rejections[key] == nil {
		s.rejections[key] = map[string]RejectedMessage{}
	}
	if _, ok := s.rejections[key][r.Fingerprint]; ok {
		return false, nil
	}
	r.ObservedAt = time.Now().UTC()
	s.rejections[key][r.Fingerprint] = r
	return true, nil
}
func (s *MemoryStore) ListRejections(ctx context.Context, tenant, binding string, limit int) ([]RejectedMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []RejectedMessage{}
	for key, items := range s.rejections {
		if key.TenantID == tenant && key.BindingID == binding {
			for _, r := range items {
				result = append(result, r)
			}
		}
	}
	slices.SortFunc(result, func(a, b RejectedMessage) int { return b.ObservedAt.Compare(a.ObservedAt) })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}
func (s *PostgresStore) RecordRejection(ctx context.Context, key PollKey, r RejectedMessage) (bool, error) {
	if !validRejection(r) {
		return false, errors.New("invalid rejection metadata")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO channel_message_rejection(tenant_id,channel_binding_id,chat_hash,fingerprint,reason,window_from,window_to,trace_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`, key.TenantID, key.BindingID, key.ChatHash, r.Fingerprint, r.Reason, r.WindowFrom, r.WindowTo, r.TraceID)
	if err != nil {
		return false, errors.New("cannot persist rejected source message")
	}
	n, err := result.RowsAffected()
	return n == 1, err
}
func (s *PostgresStore) ListRejections(ctx context.Context, tenant, binding string, limit int) ([]RejectedMessage, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT fingerprint,reason,observed_at,window_from,window_to,trace_id FROM channel_message_rejection WHERE tenant_id=$1 AND channel_binding_id=$2 ORDER BY observed_at DESC LIMIT $3`, tenant, binding, limit)
	if err != nil {
		return nil, errors.New("cannot read channel rejection metadata")
	}
	defer rows.Close()
	result := []RejectedMessage{}
	for rows.Next() {
		var r RejectedMessage
		if rows.Scan(&r.Fingerprint, &r.Reason, &r.ObservedAt, &r.WindowFrom, &r.WindowTo, &r.TraceID) != nil {
			return nil, errors.New("cannot decode rejection metadata")
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
