// Package inbox turns "a message arrived" into a durable fact before the
// HTTP layer acknowledges it, which is the whole point of the second batch's
// message path (approved plan, "接收后才 ACK").
//
// Before this, an adapter wrote its protocol ACK first and only then tried to
// de-duplicate, lock, and dispatch in a goroutine: if the process died after
// the ACK, the message was gone with no record it had ever existed. Accept
// inverts that. Nothing here may be reached before signature verification and
// identity mapping — this package assumes its caller already did that, and it
// says so at the boundary rather than re-checking something it cannot see.
package inbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// ErrConflict means the same (tenant, binding, platform message id) arrived
// twice with different content. It is not a duplicate to silently absorb; it
// is either a caller bug or an attempt to reuse an id, and either way a human
// needs to be able to tell it apart from a plain resend.
var ErrConflict = errors.New("inbox: same message id, different content")

// ErrConcurrentDuplicate is returned by AcceptInTx when the insert lost a
// unique-key race to another transaction that accepted the same message id
// between this transaction's pre-check and its insert. The caller must roll
// its transaction back: the winner's row is committed and authoritative, and
// this transaction's optimistic in_seq increment must not survive alongside
// it. Re-running the same acceptance afterwards succeeds via the pre-check,
// so a retry converges instead of looping.
//
// Accept never surfaces this error — it retries by reading the winner's row
// directly — but a caller that owns a larger transaction (the WeChat KF
// page-pull, which must commit a whole page plus its cursor atomically) has
// to decide for itself, because rolling back that transaction is the
// caller's to do.
var ErrConcurrentDuplicate = errors.New("inbox: lost the race to accept this message")

// Accepted is what Accept returns: enough to route the durable work to the
// right session and to correlate everything downstream, and nothing that
// leaks a message body to a caller that does not need it.
type Accepted struct {
	ExecutionID string
	SessionPK   int64
	InSeq       uint32
	// Duplicate is true when this call found an already-accepted row rather
	// than inserting a new one. The HTTP layer ACKs either way — an IM
	// platform resending a delivery it already accepted is normal, not an
	// error — but a caller that needs to know "did this trigger any work"
	// (metrics, a drill asserting only-once execution) can ask.
	Duplicate bool
}

// Request is one message at the moment of acceptance. Identity, channel, and
// authorization are the caller's job; this is the point where a message stops
// being "in flight inside one process" and becomes a row.
type Request struct {
	AppID                 int64
	ChannelType           string
	BindingID             int64
	ActorKey              string
	IsGroup               bool
	RevisionID            int64
	ModelProfileID        int64
	BackendProfileID      int64
	ModelProfileVersion   uint32
	BackendProfileVersion uint32

	PlatformMessageID string
	Text              string
	// Traceparent is the W3C trace context captured now, not later: a run
	// that happens after a restart has to join the trace the callback started,
	// or the "full-link trace_id" claim stops being true exactly when it
	// matters most (after a failure).
	Traceparent string
}

// Service accepts messages into the durable inbox for one tenant.
type Service struct {
	db *controlplane.DB
}

// NewService wires the inbox to the control plane's database.
func NewService(db *controlplane.DB) *Service { return &Service{db: db} }

// Accept records one inbound message and the execution it triggers, in one
// transaction of its own.
//
// Duplicate detection has two layers. The pre-check inside acceptInTx makes
// the common re-delivery case (an IM platform resending, a puller re-fetching
// a page after a crash) cost nothing: it returns the existing row and never
// touches the session's sequence counter. The inbox_messages primary key is
// the authoritative guard behind it, because the pre-check is a plain read
// and two concurrent deliveries of the same message id can both miss it; the
// insert then fails on the key, this transaction rolls back (undoing the
// in_seq increment the loser optimistically took), and the row the winner
// committed is read back below.
func (s *Service) Accept(ctx context.Context, tenantID string, req Request) (*Accepted, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}
	contentHash := hashContent(req.Text)

	var out *Accepted
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		acc, err := acceptInTx(ctx, tx, tenantID, req, contentHash)
		if err != nil {
			return err
		}
		out = acc
		return nil
	})
	if errors.Is(err, ErrConcurrentDuplicate) {
		return s.resolveRace(ctx, scope, tenantID, req.BindingID, req.PlatformMessageID, contentHash)
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AcceptInTx runs one acceptance inside a caller-owned transaction. A caller
// uses this instead of Accept when it has more to commit than the message
// itself — the WeChat KF puller commits a whole fetched page plus the sync
// cursor in the same commit, so a crash can never leave the cursor ahead of
// the messages it claims to have covered.
//
// The error contract is Accept's plus ErrConcurrentDuplicate: on that error
// the caller must roll back and retry, not swallow it.
func (s *Service) AcceptInTx(ctx context.Context, tx *controlplane.TxScope, tenantID string, req Request) (*Accepted, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}
	return acceptInTx(ctx, tx, tenantID, req, hashContent(req.Text))
}

func (r Request) validate() error {
	if r.PlatformMessageID == "" {
		return errors.New("inbox: acceptance needs a platform message id")
	}
	if r.BindingID == 0 || r.AppID == 0 || r.RevisionID == 0 {
		return errors.New("inbox: acceptance needs a bound app, a binding, and a revision")
	}
	return nil
}

// acceptInTx is the pre-check-then-insert body both entry points share.
func acceptInTx(
	ctx context.Context,
	tx *controlplane.TxScope,
	tenantID string,
	req Request,
	contentHash string,
) (*Accepted, error) {
	existing, found, err := findInboxRow(ctx, tx, tenantID, req.BindingID, req.PlatformMessageID)
	if err != nil {
		return nil, err
	}
	if found {
		if existing.contentHash != contentHash {
			return nil, ErrConflict
		}
		return &Accepted{
			ExecutionID: existing.executionID,
			SessionPK:   existing.sessionPK,
			InSeq:       existing.inSeq,
			Duplicate:   true,
		}, nil
	}

	sessionPK, nextInSeq, err := upsertSessionAndAdvance(ctx, tx, tenantID, req)
	if err != nil {
		return nil, err
	}
	executionID := uuid.NewString()
	if err := insertInboxRow(ctx, tx, tenantID, req, sessionPK, nextInSeq, executionID, contentHash); err != nil {
		if tasmysql.IsDuplicateKey(err) {
			return nil, ErrConcurrentDuplicate
		}
		return nil, err
	}
	return &Accepted{ExecutionID: executionID, SessionPK: sessionPK, InSeq: nextInSeq}, nil
}

// findInboxRow is the pre-check: a plain read of the row that would collide,
// returning found=false when this really is a first delivery.
func findInboxRow(
	ctx context.Context,
	tx *controlplane.TxScope,
	tenantID string,
	bindingID int64,
	platformMessageID string,
) (existingRow, bool, error) {
	var e existingRow
	err := tx.QueryRow(ctx, `
		SELECT execution_id, content_hash, session_pk, in_seq
		FROM inbox_messages
		WHERE tenant_id = ? AND binding_id = ? AND platform_message_id = ?`,
		tenantID, bindingID, platformMessageID).
		Scan(&e.executionID, &e.contentHash, &e.sessionPK, &e.inSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return existingRow{}, false, nil
	}
	if err != nil {
		return existingRow{}, false, fmt.Errorf("inbox: look up prior acceptance: %w", err)
	}
	return e, true, nil
}

// upsertSessionAndAdvance creates the session row if it does not exist and
// returns the in_seq number this message should take.
//
// "SELECT ... FOR UPDATE" on a missing row locks nothing — measured directly,
// two concurrent first messages both walked past a not-found check into a
// duplicate-key error. Doing the create and the increment as one
// INSERT ... ON DUPLICATE KEY UPDATE is what avoids that: the statement
// takes the row's exclusive lock whether it is inserting or updating, and
// there is no window in which a second transaction thinks it also gets the
// same number.
func upsertSessionAndAdvance(
	ctx context.Context,
	tx *controlplane.TxScope,
	tenantID string,
	req Request,
) (int64, uint32, error) {
	isGroup := boolToInt(req.IsGroup)
	res, err := tx.Exec(ctx, `
		INSERT INTO sessions
			(tenant_id, app_id, channel_type, binding_id, actor_key, is_group, generation,
			 revision_id, model_profile_version, backend_profile_version, in_seq, head_seq)
		VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, 1, 1)
		ON DUPLICATE KEY UPDATE in_seq = in_seq + 1, session_pk = LAST_INSERT_ID(session_pk)`,
		tenantID, req.AppID, req.ChannelType, req.BindingID, req.ActorKey, isGroup,
		req.RevisionID, req.ModelProfileVersion, req.BackendProfileVersion)
	if err != nil {
		return 0, 0, fmt.Errorf("inbox: upsert session: %w", err)
	}
	sessionPK, err := res.LastInsertId()
	if err != nil {
		return 0, 0, fmt.Errorf("inbox: session id: %w", err)
	}

	var currentInSeq uint32
	// The upsert above already holds this row's write lock until commit, so
	// there is no FOR UPDATE to race on: the value read here is guaranteed to
	// be the one this transaction just produced, and no other transaction can
	// advance it before this one commits.
	if err := tx.QueryRow(ctx,
		"SELECT in_seq FROM sessions WHERE tenant_id = ? AND session_pk = ?", tenantID, sessionPK).
		Scan(&currentInSeq); err != nil {
		return 0, 0, fmt.Errorf("inbox: read session sequence: %w", err)
	}
	return sessionPK, currentInSeq, nil
}

// resolveRace is the second phase: the winner's committed row is the answer.
// It is a plain read, not a transaction — nothing here needs to lock anything
// the winner already committed.
func (s *Service) resolveRace(
	ctx context.Context,
	scope controlplane.Scope,
	tenantID string,
	bindingID int64,
	platformMessageID, contentHash string,
) (*Accepted, error) {
	var e existingRow
	row, err := scope.QueryRow(ctx, `
		SELECT execution_id, content_hash, session_pk, in_seq
		FROM inbox_messages
		WHERE tenant_id = ? AND binding_id = ? AND platform_message_id = ?`,
		tenantID, bindingID, platformMessageID)
	if err != nil {
		return nil, err
	}
	if err := row.Scan(&e.executionID, &e.contentHash, &e.sessionPK, &e.inSeq); err != nil {
		return nil, fmt.Errorf("inbox: resolve a losing race: %w", err)
	}
	if e.contentHash != contentHash {
		return nil, ErrConflict
	}
	return &Accepted{ExecutionID: e.executionID, SessionPK: e.sessionPK, InSeq: e.inSeq, Duplicate: true}, nil
}

type existingRow struct {
	executionID string
	contentHash string
	sessionPK   int64
	inSeq       uint32
}

func insertInboxRow(ctx context.Context, tx *controlplane.TxScope, tenantID string, req Request, sessionPK int64, inSeq uint32, executionID, contentHash string) error {
	// inbox_messages first: executions has a foreign key pointing back at it,
	// which is the direction that says "an execution only exists because a
	// message was durably accepted", and it fixes the insert order.
	if _, err := tx.Exec(ctx, `
		INSERT INTO inbox_messages
			(tenant_id, binding_id, platform_message_id, content_hash, session_pk, in_seq, execution_id, traceparent, text)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		tenantID, req.BindingID, req.PlatformMessageID, contentHash, sessionPK, inSeq, executionID, req.Traceparent, req.Text); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO executions
			(execution_id, tenant_id, session_pk, in_seq, revision_id, model_profile_id, backend_profile_id, traceparent)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		executionID, tenantID, sessionPK, inSeq, req.RevisionID, req.ModelProfileID, req.BackendProfileID, req.Traceparent); err != nil {
		return fmt.Errorf("inbox: insert execution: %w", err)
	}
	return nil
}

func hashContent(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
