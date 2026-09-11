package postgresadapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// ClaimRoute changes only delivery metadata of this module's committed events.
// SKIP LOCKED permits multiple replicas. Each claim gets a unique fencing token.
func (s *Store) ClaimRoute(ctx context.Context) (application.RouteClaim, bool, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return application.RouteClaim{}, false, application.ErrDependencyUnavailable
	}
	claim := application.RouteClaim{ClaimToken: hex.EncodeToString(token)}
	err := s.db.QueryRow(ctx, `WITH next AS (
 SELECT o.tenant_id,o.id FROM control_outbox o
 JOIN channel_accounts a ON a.tenant_id=o.tenant_id AND a.id=o.aggregate_id
 WHERE a.scope_id=$1 AND o.event_type=$2 AND o.aggregate_type='ChannelAccountRoute'
 AND ((o.status='PENDING' AND o.available_at<=clock_timestamp()) OR (o.status='IN_FLIGHT' AND o.claimed_until<=clock_timestamp()))
 AND o.attempt_count<2147483647
 ORDER BY o.available_at,o.created_at,o.id LIMIT 1 FOR UPDATE OF o SKIP LOCKED
 ) UPDATE control_outbox o SET status='IN_FLIGHT',attempt_count=o.attempt_count+1,claimed_by=$3,claimed_until=clock_timestamp()+interval '15 seconds',updated_at=clock_timestamp()
 FROM next WHERE o.tenant_id=next.tenant_id AND o.id=next.id
 RETURNING o.tenant_id,o.id,o.aggregate_id,o.aggregate_revision,o.schema_version,o.payload_jsonb,o.payload_digest,o.attempt_count`, s.options.ScopeID, domain.RouteEventType, claim.ClaimToken).Scan(&claim.TenantID, &claim.EventID, &claim.AccountID, &claim.Generation, &claim.SchemaVersion, &claim.Payload, &claim.PayloadDigest, &claim.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.RouteClaim{}, false, nil
	}
	if err != nil {
		return application.RouteClaim{}, false, dbError(err)
	}
	return claim, true, nil
}
func (s *Store) FinishRoute(ctx context.Context, claim application.RouteClaim, published bool, code string, retry time.Duration) error {
	status := "PENDING"
	var errorCode any = code
	if published {
		if code != "" || retry != 0 {
			return integrity()
		}
		status = "PUBLISHED"
		errorCode = nil
	} else if code == "CHANNEL_OUTBOX_INTEGRITY" && retry == 0 {
		status = "FAILED"
	} else if code != "CHANNEL_ROUTE_PUBLISH_UNAVAILABLE" || retry <= 0 || retry > time.Minute {
		return integrity()
	}
	tag, err := s.db.Exec(ctx, `UPDATE control_outbox SET status=$5,claimed_by=NULL,claimed_until=NULL,last_error=$6,
 published_at=CASE WHEN $5='PUBLISHED' THEN clock_timestamp() ELSE NULL END,
 available_at=CASE WHEN $5='PENDING' THEN clock_timestamp()+($7::bigint*interval '1 millisecond') ELSE available_at END,updated_at=clock_timestamp()
 WHERE tenant_id=$1 AND id=$2 AND claimed_by=$3 AND attempt_count=$4 AND status='IN_FLIGHT' AND claimed_until>clock_timestamp() AND event_type=$8`, claim.TenantID, claim.EventID, claim.ClaimToken, claim.Attempt, status, errorCode, retry.Milliseconds(), domain.RouteEventType)
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() != 1 {
		return application.ErrOutboxLeaseLost
	}
	return nil
}

var _ application.RouteOutbox = (*Store)(nil)
