package sessioncoord

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// SQLWriteStore is the PostgreSQL atomic implementation of the fenced write contract.
type SQLWriteStore struct {
	DB  *sql.DB
	Now func() time.Time
}

var _ WriteStore = (*SQLWriteStore)(nil)
var _ CommittedTurnReader = (*SQLWriteStore)(nil)
var _ FenceValidator = (*SQLWriteStore)(nil)

func (store *SQLWriteStore) AdvanceFence(ctx context.Context, key gateway.SessionKey, token uint64) error {
	if err := store.valid(key); err != nil {
		return err
	}
	result, err := store.DB.ExecContext(ctx, `INSERT INTO session_heads (tenant_id,app_id,user_id,session_id,last_fence) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,app_id,user_id,session_id) DO UPDATE SET last_fence=EXCLUDED.last_fence,updated_at=NOW() WHERE session_heads.last_fence<EXCLUDED.last_fence`, key.TenantID, key.AppID, key.UserID, key.SessionID, token)
	return exactFenceResult(result, err)
}

func (store *SQLWriteStore) CurrentFence(ctx context.Context, key gateway.SessionKey) (uint64, error) {
	if err := store.valid(key); err != nil {
		return 0, err
	}
	var fence uint64
	err := store.DB.QueryRowContext(ctx, `SELECT last_fence FROM session_heads WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4`, key.TenantID, key.AppID, key.UserID, key.SessionID).Scan(&fence)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return fence, err
}

func (store *SQLWriteStore) ValidateFence(ctx context.Context, key gateway.SessionKey, fence uint64) error {
	if err := store.valid(key); err != nil {
		return err
	}
	var one int
	err := store.DB.QueryRowContext(ctx, `SELECT 1 FROM session_heads WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND last_fence=$5`, key.TenantID, key.AppID, key.UserID, key.SessionID, fence).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStaleFence
	}
	return err
}

// WithFence holds a row lock on the session ownership record while an
// upstream Session service performs one mutation on its own connection.
// AdvanceFence updates the same row, so a takeover either happens before this
// validation (and is rejected) or after the mutation has finished.
func (store *SQLWriteStore) WithFence(ctx context.Context, key gateway.SessionKey, fence uint64, operation func(context.Context) error) error {
	if err := store.valid(key); err != nil {
		return err
	}
	if operation == nil {
		return errors.New("sessioncoord: fenced operation is required")
	}
	tx, err := store.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current uint64
	if err := tx.QueryRowContext(ctx, `SELECT last_fence FROM session_heads WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 FOR UPDATE`, key.TenantID, key.AppID, key.UserID, key.SessionID).Scan(&current); err != nil {
		return err
	}
	if current != fence {
		return ErrStaleFence
	}
	if err := operation(ctx); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *SQLWriteStore) ValidateTurn(ctx context.Context, key gateway.SessionKey, inboxSeq uint64) error {
	if err := store.valid(key); err != nil {
		return err
	}
	if inboxSeq == 0 {
		return errors.New("sessioncoord: inbox sequence is required")
	}
	var last uint64
	if err := store.DB.QueryRowContext(ctx, `SELECT last_event_seq FROM session_heads WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4`, key.TenantID, key.AppID, key.UserID, key.SessionID).Scan(&last); err != nil {
		return err
	}
	if inboxSeq != last+1 {
		return ErrOutOfOrder
	}
	return nil
}

func (store *SQLWriteStore) CommitTurn(ctx context.Context, write TurnWrite) (uint64, error) {
	if err := store.valid(write.Key); err != nil {
		return 0, err
	}
	if write.InboxID == "" || write.EventID == "" || write.InboxSeq == 0 {
		return 0, errors.New("sessioncoord: inbox sequence, inbox ID, and event ID are required")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var lastSeq, currentFence uint64
	var stateBytes []byte
	err = tx.QueryRowContext(ctx, `SELECT last_event_seq,last_fence,state_json FROM session_heads WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 FOR UPDATE`, write.Key.TenantID, write.Key.AppID, write.Key.UserID, write.Key.SessionID).Scan(&lastSeq, &currentFence, &stateBytes)
	if err != nil {
		return 0, err
	}
	if currentFence != write.Fence {
		return 0, ErrStaleFence
	}
	var existingSeq uint64
	err = tx.QueryRowContext(ctx, `SELECT event_seq FROM message_events WHERE tenant_id=$1 AND inbox_id=$2`, write.Key.TenantID, write.InboxID).Scan(&existingSeq)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return 0, err
		}
		return existingSeq, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if write.InboxSeq != lastSeq+1 {
		return 0, ErrOutOfOrder
	}
	state := make(map[string]string)
	if len(stateBytes) > 0 {
		if err := json.Unmarshal(stateBytes, &state); err != nil {
			return 0, err
		}
	}
	if state == nil {
		state = make(map[string]string)
	}
	for key, value := range write.StateDelta {
		state[key] = value
	}
	stateJSON, _ := json.Marshal(state)
	deltaJSON, _ := json.Marshal(write.StateDelta)
	payloadJSON, _ := json.Marshal(write.Payload)
	now := store.now().UTC()
	_, err = tx.ExecContext(ctx, `INSERT INTO message_events (tenant_id,app_id,user_id,session_id,event_id,inbox_id,event_seq,event_type,payload_json,state_delta_json,trace_id,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, write.Key.TenantID, write.Key.AppID, write.Key.UserID, write.Key.SessionID, write.EventID, write.InboxID, write.InboxSeq, write.EventType, payloadJSON, deltaJSON, write.TraceID, now)
	if err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE session_heads SET last_event_seq=$6,state_version=state_version+1,state_json=$7,updated_at=$8 WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND last_fence=$5 AND last_event_seq=$9`, write.Key.TenantID, write.Key.AppID, write.Key.UserID, write.Key.SessionID, write.Fence, write.InboxSeq, stateJSON, now, lastSeq)
	if err := exactFenceResult(result, err); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return write.InboxSeq, nil
}

func (store *SQLWriteStore) CommittedTurn(ctx context.Context, key gateway.SessionKey, inboxID string) (Event, bool, error) {
	if err := store.valid(key); err != nil {
		return Event{}, false, err
	}
	if inboxID == "" {
		return Event{}, false, errors.New("sessioncoord: inbox ID is required")
	}
	var committed Event
	err := store.DB.QueryRowContext(ctx, `SELECT event_id,event_seq,event_type,payload_json,COALESCE(trace_id,''),created_at FROM message_events WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND inbox_id=$5`, key.TenantID, key.AppID, key.UserID, key.SessionID, inboxID).Scan(&committed.EventID, &committed.Seq, &committed.Type, &committed.Payload, &committed.TraceID, &committed.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, false, nil
	}
	if err != nil {
		return Event{}, false, err
	}
	committed.InboxID = inboxID
	return committed, true, nil
}

func (store *SQLWriteStore) PublishSummary(ctx context.Context, key gateway.SessionKey, fence uint64, summary Summary) error {
	if summary.Version == 0 || summary.CutoffEventSeq == 0 || summary.Content == "" {
		return errors.New("sessioncoord: summary version, cutoff, and content are required")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var lastSeq, currentFence uint64
	if err := tx.QueryRowContext(ctx, `SELECT last_event_seq,last_fence FROM session_heads WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 FOR UPDATE`, key.TenantID, key.AppID, key.UserID, key.SessionID).Scan(&lastSeq, &currentFence); err != nil {
		return err
	}
	if currentFence != fence {
		return ErrStaleFence
	}
	if summary.CutoffEventSeq > lastSeq {
		return errors.New("sessioncoord: summary cutoff exceeds session head")
	}
	var oldVersion, oldCutoff uint64
	var oldContent string
	err = tx.QueryRowContext(ctx, `SELECT summary_version,cutoff_event_seq,content FROM session_summaries WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 ORDER BY summary_version DESC LIMIT 1`, key.TenantID, key.AppID, key.UserID, key.SessionID).Scan(&oldVersion, &oldCutoff, &oldContent)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		if summary.Version == oldVersion && summary.CutoffEventSeq == oldCutoff && summary.Content == oldContent {
			return tx.Commit()
		}
		if summary.Version <= oldVersion || summary.CutoffEventSeq <= oldCutoff {
			return errors.New("sessioncoord: stale summary")
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO session_summaries (tenant_id,app_id,user_id,session_id,summary_version,cutoff_event_seq,content,status,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,'active',$8)`, key.TenantID, key.AppID, key.UserID, key.SessionID, summary.Version, summary.CutoffEventSeq, summary.Content, store.now().UTC())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (store *SQLWriteStore) UpsertMemory(ctx context.Context, key gateway.SessionKey, fence uint64, memory Memory) error {
	if memory.MemoryID == "" || memory.SourceEventID == "" || memory.SourceEventSeq == 0 || memory.Version == 0 {
		return errors.New("sessioncoord: memory identity, source, and version are required")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateFenceTx(ctx, tx, key, fence); err != nil {
		return err
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT memory_id FROM memory_entries WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND source_event_id=$4`, key.TenantID, key.AppID, key.UserID, memory.SourceEventID).Scan(&existing)
	if err == nil {
		if existing != memory.MemoryID {
			return errors.New("sessioncoord: source event already mapped")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO memory_entries (tenant_id,app_id,user_id,memory_id,source_session_id,source_event_id,source_event_seq,version,status,content,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, key.TenantID, key.AppID, key.UserID, memory.MemoryID, key.SessionID, memory.SourceEventID, memory.SourceEventSeq, memory.Version, memory.Status, memory.Content, store.now().UTC())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (store *SQLWriteStore) PublishOutbox(ctx context.Context, key gateway.SessionKey, fence uint64, outbound gateway.OutboundMessage) error {
	if outbound.DedupeKey == "" || outbound.SourceInboxID == "" || outbound.SourceEventID == "" {
		return errors.New("sessioncoord: outbox dedupe and source IDs are required")
	}
	if outbound.TenantID != key.TenantID || outbound.AppID != key.AppID || outbound.UserID != key.UserID || outbound.SessionID != key.SessionID {
		return errors.New("sessioncoord: outbox scope does not match session")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateFenceTx(ctx, tx, key, fence); err != nil {
		return err
	}
	var inboxID string
	err = tx.QueryRowContext(ctx, `SELECT inbox_id FROM message_events WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND event_id=$5`, key.TenantID, key.AppID, key.UserID, key.SessionID, outbound.SourceEventID).Scan(&inboxID)
	if err != nil {
		return err
	}
	if inboxID != outbound.SourceInboxID {
		return errors.New("sessioncoord: outbox source event does not match committed turn")
	}
	payload, _ := json.Marshal(outbound)
	result, err := tx.ExecContext(ctx, `INSERT INTO outbox_messages (tenant_id,outbox_id,dedupe_key,binding_id,app_id,user_id,session_id,source_inbox_id,source_event_id,fence,status,payload_json,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11,$12) ON CONFLICT (tenant_id,dedupe_key) DO NOTHING`, key.TenantID, outbound.OutboxID, outbound.DedupeKey, outbound.BindingID, key.AppID, key.UserID, key.SessionID, outbound.SourceInboxID, outbound.SourceEventID, fence, payload, store.now().UTC())
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		var sourceInbox, sourceEvent string
		err = tx.QueryRowContext(ctx, `SELECT source_inbox_id,source_event_id FROM outbox_messages WHERE tenant_id=$1 AND dedupe_key=$2`, key.TenantID, outbound.DedupeKey).Scan(&sourceInbox, &sourceEvent)
		if err != nil {
			return err
		}
		if sourceInbox != outbound.SourceInboxID || sourceEvent != outbound.SourceEventID {
			return errors.New("sessioncoord: duplicate outbox key has different source")
		}
	}
	return tx.Commit()
}

func validateFenceTx(ctx context.Context, tx *sql.Tx, key gateway.SessionKey, fence uint64) error {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM session_heads WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4 AND last_fence=$5 FOR UPDATE`, key.TenantID, key.AppID, key.UserID, key.SessionID, fence).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStaleFence
	}
	return err
}
func exactFenceResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrStaleFence
	}
	return nil
}
func (store *SQLWriteStore) valid(key gateway.SessionKey) error {
	if store == nil || store.DB == nil {
		return errors.New("sessioncoord: nil SQL database")
	}
	if key.TenantID == "" || key.AppID == "" || key.UserID == "" || key.SessionID == "" {
		return fmt.Errorf("sessioncoord: complete session scope is required")
	}
	return nil
}
func (store *SQLWriteStore) now() time.Time {
	if store.Now != nil {
		return store.Now()
	}
	return time.Now()
}
