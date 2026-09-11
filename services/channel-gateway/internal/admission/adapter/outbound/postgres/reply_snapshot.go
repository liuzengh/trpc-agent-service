package postgresadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

// ReadReplySnapshot reads only Admission-owned immutable rows, never the latest
// Routing projection. Wire validation detects inconsistent/corrupt stored values;
// local ReplyOrigin is decoded separately and never enters the Execution event.
func (s *Store) ReadReplySnapshot(ctx context.Context, admissionID, runID string) (domain.ReplySnapshot, error) {
	zero := domain.ReplySnapshot{}
	if ctx == nil || (domain.Receipt{Decision: "admit-run", AdmissionID: admissionID, RunID: runID}).Validate() != nil {
		return zero, domain.ErrInvalidInput
	}
	var tenant, provider, account string
	var route, input, origin []byte
	err := s.pool.QueryRow(ctx, `SELECT tenant_id,provider,account_id,route,input,reply_origin FROM gateway_admissions WHERE admission_id=$1 AND run_id=$2`, admissionID, runID).Scan(&tenant, &provider, &account, &route, &input, &origin)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, domain.ErrReplyNotFound
	}
	if err != nil {
		return zero, domain.ErrUnavailable
	}
	raw, err := json.Marshal(struct {
		SchemaVersion int             `json:"schema_version"`
		EventID       string          `json:"event_id"`
		AdmissionID   string          `json:"admission_id"`
		RunID         string          `json:"run_id"`
		Route         json.RawMessage `json:"route"`
		Input         json.RawMessage `json:"input"`
	}{1, admissionID, admissionID, runID, route, input})
	if err != nil {
		return zero, domain.ErrUnavailable
	}
	event, err := wire.DecodeRunRequested(raw)
	if err != nil || event.Route.TenantID != tenant || event.Route.Provider != provider || event.Route.AccountID != account {
		return zero, domain.ErrUnavailable
	}
	received, err := time.Parse(time.RFC3339Nano, event.Input.ReceivedAt)
	if err != nil {
		return zero, domain.ErrUnavailable
	}
	reply := event.Input.ReplyContext
	out := domain.ReplySnapshot{AdmissionID: admissionID, RunID: runID, TenantID: tenant, Provider: provider, AccountID: account, ManifestDigest: event.Route.ManifestDigest, ConversationID: event.Input.ConversationID, ThreadID: event.Input.ThreadID, SourceMessageID: reply.SourceMessageID, SourceEventID: event.Input.Key.EventID, CallbackRequestID: reply.CallbackReqID, ReceivedAt: received}
	if len(origin) > 0 {
		if provider != "wecom" || bytes.Equal(origin, []byte("null")) {
			return zero, domain.ErrUnavailable
		}
		var value domain.ReplyOrigin
		decoder := json.NewDecoder(bytes.NewReader(origin))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&value); err != nil || value.Validate() != nil {
			return zero, domain.ErrUnavailable
		}
		out.Origin = &value
	}
	return out, nil
}
