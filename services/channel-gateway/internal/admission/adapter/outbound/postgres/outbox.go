package postgresadapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

func (s *Store) Claim(ctx context.Context) (application.OutboxMessage, bool, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return application.OutboxMessage{}, false, err
	}
	token := hex.EncodeToString(random[:])
	var msg application.OutboxMessage
	err := s.pool.QueryRow(ctx, `WITH due AS (
 SELECT event_id FROM gateway_outbox WHERE published_at IS NULL AND next_attempt_at<=clock_timestamp()
 AND (claimed_until IS NULL OR claimed_until<=clock_timestamp()) ORDER BY created_at,event_id FOR UPDATE SKIP LOCKED LIMIT 1
 ) UPDATE gateway_outbox o SET claim_token=$1,claimed_until=clock_timestamp()+interval '30 seconds',attempts=attempts+1
 FROM due WHERE o.event_id=due.event_id RETURNING o.event_id,o.subject,o.payload,o.claim_token,COALESCE(o.traceparent,''),COALESCE(o.tracestate,'')`, token).Scan(&msg.EventID, &msg.Subject, &msg.Payload, &msg.ClaimToken, &msg.Carrier.Traceparent, &msg.Carrier.Tracestate)
	if errors.Is(err, pgx.ErrNoRows) {
		return msg, false, nil
	}
	msg.Carrier = msg.Carrier.Normalize()
	return msg, err == nil, err
}
func (s *Store) Published(ctx context.Context, msg application.OutboxMessage) error {
	if msg.EventID == "" || msg.ClaimToken == "" {
		return domain.ErrClaimLost
	}
	tag, err := s.pool.Exec(ctx, `UPDATE gateway_outbox SET published_at=clock_timestamp(),claim_token=NULL,claimed_until=NULL WHERE event_id=$1 AND claim_token=$2 AND claimed_until>clock_timestamp() AND published_at IS NULL`, msg.EventID, msg.ClaimToken)
	if err == nil && tag.RowsAffected() != 1 {
		return domain.ErrClaimLost
	}
	return err
}
func (s *Store) Retry(ctx context.Context, msg application.OutboxMessage) error {
	if msg.EventID == "" || msg.ClaimToken == "" {
		return domain.ErrClaimLost
	}
	tag, err := s.pool.Exec(ctx, `UPDATE gateway_outbox SET next_attempt_at=clock_timestamp()+interval '1 second',claim_token=NULL,claimed_until=NULL WHERE event_id=$1 AND claim_token=$2 AND claimed_until>clock_timestamp() AND published_at IS NULL`, msg.EventID, msg.ClaimToken)
	if err == nil && tag.RowsAffected() != 1 {
		return domain.ErrClaimLost
	}
	return err
}
