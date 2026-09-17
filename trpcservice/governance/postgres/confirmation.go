package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

func (s *Store) Suspend(ctx context.Context, commit sessionstore.CommitTurnRequest, in governance.SuspensionRequest) (governance.Confirmation, error) {
	if commit.Outcome != runtime.OutcomeWaitingConfirmation || commit.TenantID != in.TenantID || commit.RequestID != in.RequestID ||
		commit.AgentAppID != in.AgentAppID || commit.SessionID != in.SessionID || commit.InputSeq != in.InputSeq || commit.Fence != in.Fence ||
		in.ConfirmationID == "" || in.SubjectID == "" || in.ChannelBindingID == "" || in.Tool.ID == "" || in.Tool.Version < 1 ||
		in.ToolCallID == "" || len(in.ArgsDigest) != 64 || in.PolicyVersion < 1 || in.CheckpointRef == "" || !in.ExpiresAt.After(s.current()) ||
		in.Usage.InputTokens < 0 || in.Usage.OutputTokens < 0 || in.Usage.CachedInputTokens < 0 || in.Usage.CachedInputTokens > in.Usage.InputTokens {
		return governance.Confirmation{}, runtime.ErrInvariantViolation
	}
	digest, err := sessionstore.CommitDigest(commit)
	if err != nil {
		return governance.Confirmation{}, err
	}
	events := make([]map[string]any, 0, len(commit.Events))
	for _, value := range commit.Events {
		wrapper, marshalErr := json.Marshal(map[string]any{"ref": value.PayloadRef, "payload": value.Payload})
		if marshalErr != nil {
			return governance.Confirmation{}, marshalErr
		}
		events = append(events, map[string]any{"event_id": value.EventID, "event_type": value.EventType, "payload_ref": string(wrapper), "event_seq": value.EventSeq})
	}
	eventBytes, err := json.Marshal(events)
	if err != nil {
		return governance.Confirmation{}, err
	}
	state := commit.StateDelta
	if state == nil {
		state = sessionstore.StateDelta{}
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return governance.Confirmation{}, err
	}
	outboxes := make([]map[string]any, 0, len(commit.Outbox))
	for _, value := range commit.Outbox {
		outboxes = append(outboxes, map[string]any{"kind": value.Kind, "idempotency_key": value.IdempotencyKey, "payload_ref": value.PayloadRef,
			"traceparent": value.TraceParent, "event_seq": value.EventSeq})
	}
	outboxBytes, err := json.Marshal(outboxes)
	if err != nil {
		return governance.Confirmation{}, err
	}
	var id, stateValue string
	var version int64
	err = s.db.QueryRowContext(ctx, `SELECT confirmation_id,state,version FROM suspend_turn($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)`,
		in.TenantID, in.AgentAppID, in.SessionID, in.RequestID, commit.CommitID, digest, in.InputSeq, in.Fence, commit.ExpectedVersion,
		eventBytes, stateBytes, in.ConfirmationID, in.SubjectID, in.ChannelBindingID, in.Tool.ID, in.Tool.Version, in.ToolCallID,
		in.ArgsDigest, in.PolicyVersion, in.CheckpointRef, in.Usage.InputTokens, in.Usage.OutputTokens, in.Usage.CachedInputTokens,
		in.ExpiresAt.UTC(), outboxBytes).Scan(&id, &stateValue, &version)
	if err != nil {
		return governance.Confirmation{}, translate(err)
	}
	return s.GetConfirmation(ctx, in.TenantID, id)
}

func (s *Store) Decide(ctx context.Context, in governance.ConfirmationDecision) (governance.Confirmation, error) {
	if in.TenantID == "" || in.ConfirmationID == "" || in.SubjectID == "" || in.ExpectedVersion < 1 || in.DecidedAt.IsZero() {
		return governance.Confirmation{}, runtime.ErrInvariantViolation
	}
	var id, state string
	var version int64
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return governance.Confirmation{}, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT confirmation_id,state,version FROM decide_confirmation($1,$2,$3,$4,$5,$6)`, in.TenantID,
		in.ConfirmationID, in.SubjectID, in.Approve, in.ExpectedVersion, in.DecidedAt.UTC()).Scan(&id, &state, &version)
	if err != nil {
		return governance.Confirmation{}, translate(err)
	}
	if state == string(governance.ConfirmationApproved) || state == string(governance.ConfirmationDenied) || state == string(governance.ConfirmationExpired) {
		if err := insertConfirmationAudit(ctx, tx, in.TenantID, id, state, version, in.DecidedAt.UTC()); err != nil {
			return governance.Confirmation{}, err
		}
	}
	value, err := getConfirmation(ctx, tx, in.TenantID, "confirmation_id", id)
	if err != nil {
		return governance.Confirmation{}, err
	}
	if err := tx.Commit(); err != nil {
		return governance.Confirmation{}, err
	}
	return value, nil
}

func (s *Store) ExpireDue(ctx context.Context, now time.Time, limit int) ([]governance.Confirmation, error) {
	if now.IsZero() || limit < 1 || limit > 1000 {
		return nil, runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id,confirmation_id FROM expire_confirmations($1,$2)`, now.UTC(), limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	type key struct{ tenantID, confirmationID string }
	var keys []key
	for rows.Next() {
		var value key
		if err := rows.Scan(&value.tenantID, &value.confirmationID); err != nil {
			return nil, err
		}
		keys = append(keys, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	result := make([]governance.Confirmation, 0, len(keys))
	for _, value := range keys {
		confirmation, getErr := getConfirmation(ctx, tx, value.tenantID, "confirmation_id", value.confirmationID)
		if getErr != nil {
			return nil, getErr
		}
		if insertErr := insertConfirmationAudit(ctx, tx, value.tenantID, value.confirmationID, string(governance.ConfirmationExpired), confirmation.Version, now.UTC()); insertErr != nil {
			return nil, insertErr
		}
		result = append(result, confirmation)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) GetConfirmation(ctx context.Context, tenantID, id string) (governance.Confirmation, error) {
	return getConfirmation(ctx, s.db, tenantID, "confirmation_id", id)
}

func (s *Store) GetConfirmationByRequest(ctx context.Context, tenantID, requestID string) (governance.Confirmation, error) {
	return getConfirmation(ctx, s.db, tenantID, "request_id", requestID)
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getConfirmation(ctx context.Context, db queryRower, tenantID, field, valueID string) (governance.Confirmation, error) {
	var value governance.Confirmation
	value.TenantID = tenantID
	var decision sql.NullTime
	query := `SELECT confirmation_id,request_id,agent_app_id,session_id,input_seq,subject_id,channel_binding_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,checkpoint_ref,input_tokens,output_tokens,cached_input_tokens,state,expires_at,decision_at,version FROM confirmation WHERE tenant_id=$1 AND ` + field + `=$2 ORDER BY created_at DESC LIMIT 1`
	err := db.QueryRowContext(ctx, query, tenantID, valueID).
		Scan(&value.ConfirmationID, &value.RequestID, &value.AgentAppID, &value.SessionID, &value.InputSeq, &value.SubjectID, &value.ChannelBindingID, &value.Tool.ID, &value.Tool.Version,
			&value.ToolCallID, &value.ArgsDigest, &value.PolicyVersion, &value.CheckpointRef, &value.Usage.InputTokens, &value.Usage.OutputTokens,
			&value.Usage.CachedInputTokens, &value.State, &value.ExpiresAt, &decision, &value.Version)
	if err != nil {
		return governance.Confirmation{}, translate(err)
	}
	value.DecisionAt = decision.Time
	return value, nil
}

func (s *Store) GetGrantByConfirmation(ctx context.Context, tenantID, confirmationID string) (governance.Grant, error) {
	var value governance.Grant
	var consumed sql.NullTime
	value.TenantID, value.ConfirmationID = tenantID, confirmationID
	err := s.db.QueryRowContext(ctx, `SELECT grant_id,request_id,subject_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,version,consumed_at FROM confirmation_grant WHERE tenant_id=$1 AND confirmation_id=$2`, tenantID, confirmationID).
		Scan(&value.GrantID, &value.RequestID, &value.SubjectID, &value.Tool.ID, &value.Tool.Version, &value.ToolCallID, &value.ArgsDigest, &value.PolicyVersion, &value.Version, &consumed)
	if err != nil {
		return governance.Grant{}, translate(err)
	}
	value.ConsumedAt = consumed.Time
	return value, nil
}

func (s *Store) ConsumeGrant(ctx context.Context, in governance.GrantClaim) (governance.Grant, error) {
	if in.TenantID == "" || in.GrantID == "" || in.RequestID == "" || in.SubjectID == "" || in.Tool.ID == "" || in.Tool.Version < 1 ||
		in.ToolCallID == "" || len(in.ArgsDigest) != 64 || in.PolicyVersion < 1 || in.ExpectedVersion < 1 {
		return governance.Grant{}, runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return governance.Grant{}, err
	}
	defer tx.Rollback()
	var value governance.Grant
	var state string
	var consumed sql.NullTime
	value.TenantID, value.GrantID = in.TenantID, in.GrantID
	err = tx.QueryRowContext(ctx, `SELECT confirmation_id,request_id,subject_id,tool_id,tool_version,tool_call_id,args_digest,policy_version,state,version,consumed_at FROM confirmation_grant WHERE tenant_id=$1 AND grant_id=$2 FOR UPDATE`, in.TenantID, in.GrantID).
		Scan(&value.ConfirmationID, &value.RequestID, &value.SubjectID, &value.Tool.ID, &value.Tool.Version, &value.ToolCallID, &value.ArgsDigest, &value.PolicyVersion, &state, &value.Version, &consumed)
	if err != nil {
		return governance.Grant{}, translate(err)
	}
	if value.RequestID != in.RequestID || value.SubjectID != in.SubjectID || value.Tool != in.Tool || value.ToolCallID != in.ToolCallID || value.ArgsDigest != in.ArgsDigest || value.PolicyVersion != in.PolicyVersion {
		return governance.Grant{}, runtime.ErrTenantScope
	}
	if state != "available" || value.Version != in.ExpectedVersion {
		return governance.Grant{}, runtime.ErrVersionConflict
	}
	var cancelRequested bool
	var outcome runtime.Outcome
	if err := tx.QueryRowContext(ctx, `SELECT cancel_requested_at IS NOT NULL,outcome FROM execution_record WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE`, in.TenantID, in.RequestID).
		Scan(&cancelRequested, &outcome); err != nil {
		return governance.Grant{}, translate(err)
	}
	if cancelRequested {
		return governance.Grant{}, runtime.ErrCancelRequested
	}
	if outcome.Terminal() {
		return governance.Grant{}, runtime.ErrAlreadyTerminal
	}
	now := s.current()
	result, err := tx.ExecContext(ctx, `UPDATE confirmation_grant SET state='consumed',consumed_at=$3,version=version+1 WHERE tenant_id=$1 AND grant_id=$2 AND state='available' AND version=$4`, in.TenantID, in.GrantID, now, in.ExpectedVersion)
	if err != nil {
		return governance.Grant{}, translate(err)
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return governance.Grant{}, runtime.ErrVersionConflict
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO tool_attempt(tenant_id,grant_id,request_id,tool_call_id,state) VALUES($1,$2,$3,$4,'effect_unknown')`, in.TenantID, in.GrantID, in.RequestID, in.ToolCallID)
	if err != nil {
		return governance.Grant{}, translate(err)
	}
	var confirmationVersion int64
	err = tx.QueryRowContext(ctx, `UPDATE confirmation SET state='consumed',version=version+1,updated_at=now()
WHERE tenant_id=$1 AND confirmation_id=$2 AND state='approved' RETURNING version`, in.TenantID, value.ConfirmationID).Scan(&confirmationVersion)
	if err != nil {
		return governance.Grant{}, translate(err)
	}
	if err := insertConfirmationAudit(ctx, tx, in.TenantID, value.ConfirmationID, string(governance.ConfirmationConsumed), confirmationVersion, now); err != nil {
		return governance.Grant{}, err
	}
	if err = tx.Commit(); err != nil {
		return governance.Grant{}, err
	}
	value.Version++
	value.ConsumedAt = now
	return value, nil
}

func insertConfirmationAudit(ctx context.Context, tx *sql.Tx, tenantID, confirmationID, decision string, version int64, occurredAt time.Time) error {
	if tenantID == "" || confirmationID == "" || decision == "" || version < 1 || occurredAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	idempotencyKey := "confirmation-audit:" + confirmationID + ":" + decision + ":" + fmt.Sprint(version)
	prefix := "confirmation-decision://"
	if decision == string(governance.ConfirmationConsumed) {
		prefix = "confirmation-consumed://"
	}
	payloadRef := prefix + tenantID + "/" + confirmationID + "/" + decision + "/" + fmt.Sprint(version)
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
SELECT $1,$2,'audit',request_id,$3,$4,$5 FROM confirmation WHERE tenant_id=$1 AND confirmation_id=$6
ON CONFLICT (tenant_id,kind,idempotency_key) DO NOTHING`, tenantID, "confirmation-audit:"+confirmationID+":"+fmt.Sprint(version), version,
		idempotencyKey, payloadRef, confirmationID)
	if err != nil {
		return translate(err)
	}
	return nil
}

func (s *Store) FinishToolAttempt(ctx context.Context, in governance.FinishToolAttemptRequest) (governance.ToolAttempt, error) {
	if in.TenantID == "" || in.GrantID == "" || (in.State != governance.ToolAttemptSucceeded && in.State != governance.ToolAttemptFailed) ||
		(in.State == governance.ToolAttemptSucceeded && in.ResultRef == "") {
		return governance.ToolAttempt{}, runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return governance.ToolAttempt{}, err
	}
	defer tx.Rollback()
	var value governance.ToolAttempt
	value.TenantID, value.GrantID = in.TenantID, in.GrantID
	err = tx.QueryRowContext(ctx, `SELECT request_id,tool_call_id,state,COALESCE(result_ref,'') FROM tool_attempt WHERE tenant_id=$1 AND grant_id=$2 FOR UPDATE`, in.TenantID, in.GrantID).
		Scan(&value.RequestID, &value.ToolCallID, &value.State, &value.ResultRef)
	if err != nil {
		return governance.ToolAttempt{}, translate(err)
	}
	if value.State != governance.ToolAttemptEffectUnknown {
		if value.State == in.State && value.ResultRef == in.ResultRef {
			return value, nil
		}
		return governance.ToolAttempt{}, runtime.ErrIdempotencyCollision
	}
	_, err = tx.ExecContext(ctx, `UPDATE tool_attempt SET state=$3,result_ref=$4,updated_at=now() WHERE tenant_id=$1 AND grant_id=$2 AND state='effect_unknown'`, in.TenantID, in.GrantID, in.State, nullableString(in.ResultRef))
	if err != nil {
		return governance.ToolAttempt{}, translate(err)
	}
	if err = tx.Commit(); err != nil {
		return governance.ToolAttempt{}, err
	}
	value.State, value.ResultRef = in.State, in.ResultRef
	return value, nil
}

func (s *Store) GetToolAttempt(ctx context.Context, tenantID, grantID string) (governance.ToolAttempt, error) {
	if tenantID == "" || grantID == "" {
		return governance.ToolAttempt{}, runtime.ErrTenantScope
	}
	value := governance.ToolAttempt{TenantID: tenantID, GrantID: grantID}
	err := s.db.QueryRowContext(ctx, `SELECT request_id,tool_call_id,state,COALESCE(result_ref,'') FROM tool_attempt WHERE tenant_id=$1 AND grant_id=$2`, tenantID, grantID).
		Scan(&value.RequestID, &value.ToolCallID, &value.State, &value.ResultRef)
	if err != nil {
		return governance.ToolAttempt{}, translate(err)
	}
	return value, nil
}

var _ governance.ConfirmationCoordinator = (*Store)(nil)
