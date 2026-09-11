package postgresadapter

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

func (s *Store) FindTransportReceipt(ctx context.Context, p d.TransportPosition) (d.TransportReceipt, bool, error) {
	if p.Validate() != nil {
		return d.TransportReceipt{}, false, d.ErrInvalid
	}
	r := d.TransportReceipt{Position: p}
	var digest string
	var carrier tracecontext.Carrier
	err := s.pool.QueryRow(ctx, `SELECT r.raw_digest,r.outcome,r.reason,r.intent_id,r.run_id,COALESCE(i.traceparent,''),COALESCE(i.tracestate,'') FROM gateway_reply_transport_receipts r LEFT JOIN gateway_delivery_intents i ON i.intent_id=r.intent_id WHERE r.stream_name=$1 AND r.stream_id=$2 AND r.stream_sequence=$3`, p.StreamName, p.StreamID, int64(p.Sequence)).Scan(&digest, &r.Outcome, &r.Reason, &r.IntentID, &r.RunID, &carrier.Traceparent, &carrier.Tracestate)
	if errors.Is(err, pgx.ErrNoRows) {
		return d.TransportReceipt{}, false, nil
	}
	if err != nil {
		return d.TransportReceipt{}, false, databaseError(ctx, err)
	}
	if digest != p.RawDigest {
		return d.TransportReceipt{}, false, d.ErrConflict
	}
	linkCarrier(ctx, carrier)
	return r, true, nil
}
func (s *Store) RecordTransportReceipt(ctx context.Context, r d.TransportReceipt) error {
	if r.Validate() != nil {
		return d.ErrInvalid
	}
	p := r.Position
	_, err := s.pool.Exec(ctx, `INSERT INTO gateway_reply_transport_receipts(stream_name,stream_id,stream_sequence,raw_digest,outcome,reason,intent_id,run_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`, p.StreamName, p.StreamID, int64(p.Sequence), p.RawDigest, r.Outcome, r.Reason, r.IntentID, r.RunID)
	if err != nil {
		return databaseError(ctx, err)
	}
	old, found, err := s.FindTransportReceipt(ctx, p)
	if err != nil {
		return err
	}
	if !found {
		return d.ErrUnavailable
	}
	if old != r {
		return d.ErrConflict
	}
	return nil
}
