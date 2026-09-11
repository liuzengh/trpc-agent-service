// Package outbox runs the delivery side of the message path: it takes rows
// execution.Commit created in reply_outbox, sends them through a channel
// Adapter, and records what happened well enough that a later crash can be
// distinguished from a downstream rejection.
//
// The difference between "failed" and "unknown" is the point of this whole
// package. A Send that returns an error is normal, retryable, and safe to try
// again with a backoff. A Send whose caller never learns the outcome — the
// process died mid-request, the connection timed out after the request went
// out — is not the same thing, and retrying it automatically can double-send
// to a real user. Those go to a separate state a human looks at.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// DefaultBackoff is the retry schedule approved plan §"投递、重试与人工处置"
// describes: five attempts, doubling, with jitter so a whole fleet recovering
// at once does not synchronize on the same retry second.
var DefaultBackoff = []time.Duration{
	1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
}

// ErrNothingToSend is returned by ClaimNext when no row is currently due.
var ErrNothingToSend = errors.New("outbox: nothing is due")

// Outcome is what a Sender reports back. It has to distinguish "the channel
// said no, safely retryable" from "we may never know".
type Outcome int

const (
	// Sent means the channel accepted the message.
	Sent Outcome = iota
	// Rejected means the channel refused it for a reason we can see; a
	// 4xx-level "user does not exist" is Rejected, not Unknown.
	Rejected
	// Unknown means "this may or may not have arrived": a timeout, a dropped
	// connection mid-write, or a process that died between recording the
	// intent and recording the result. Automatic retry is not allowed.
	Unknown
)

// Sender delivers one claimed reply. It is an interface so the delivery loop
// does not depend on which channel a reply is addressed to.
//
// The whole ClaimedReply is handed over, not a flat argument list, because a
// channel may need more than "send this text to that user": the WeChat
// customer-service sender has to resolve the open_kfid the reply goes out on,
// and it does that by session_pk. Flattening the parameters would force every
// future channel to have its extra needs anticipated here.
type Sender interface {
	Send(ctx context.Context, reply *ClaimedReply) (Outcome, error)
}

// ClaimedReply is one reply_outbox row this worker now owns.
type ClaimedReply struct {
	WorkerID    string
	TenantID    string
	OutboxID    int64
	ExecutionID string
	ChannelType string
	BindingID   int64
	SessionPK   int64
	Target      string
	Text        string
	IsDone      bool
	PartSeq     uint32
	Attempts    uint32
	FenceToken  uint64
	LeaseTTL    time.Duration
}

// Service claims and delivers reply_outbox rows for one tenant.
type Service struct {
	db       *controlplane.DB
	sender   Sender
	leaseTTL time.Duration
	backoff  []time.Duration
	now      func() time.Time
}

// NewService wires delivery to the control plane's database and a channel
// sender.
func NewService(db *controlplane.DB, sender Sender) *Service {
	return &Service{
		db:       db,
		sender:   sender,
		leaseTTL: 30 * time.Second,
		backoff:  DefaultBackoff,
		now:      time.Now,
	}
}

// ClaimNext takes the next due reply. Unlike the session lease, delivery does
// not serialize per conversation — the head-ordering rule lives on sessions,
// not on the outbox — so this is a plain "grab any pending row whose next
// attempt time has arrived", with SKIP LOCKED so several delivery workers
// divide the queue between themselves instead of contending for the same row.
func (s *Service) ClaimNext(ctx context.Context, tenantID, workerID string) (*ClaimedReply, error) {
	scope, err := s.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}

	var out *ClaimedReply
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		var (
			r         ClaimedReply
			bindingID sql.NullInt64
			nextAt    time.Time
		)
		row := tx.QueryRow(ctx, `
			SELECT ro.outbox_id, ro.execution_id, ro.channel_type, ro.binding_id, ro.session_pk, ro.target,
			       ro.text, ro.is_done, ro.part_seq, ro.attempts, ro.delivery_fencing, ro.next_attempt_at
			FROM reply_outbox ro
			JOIN sessions s ON s.tenant_id = ro.tenant_id AND s.session_pk = ro.session_pk
			WHERE ro.tenant_id = ?
			  AND ro.status IN ('pending', 'sending')
			  AND ro.next_attempt_at <= UTC_TIMESTAMP(6)
			  AND s.lease_owner IS NULL
			ORDER BY ro.outbox_id
			LIMIT 1
			FOR UPDATE SKIP LOCKED`, tenantID)
		switch err := row.Scan(&r.OutboxID, &r.ExecutionID, &r.ChannelType, &bindingID, &r.SessionPK, &r.Target,
			&r.Text, &r.IsDone, &r.PartSeq, &r.Attempts, &r.FenceToken, &nextAt); {
		case errors.Is(err, sql.ErrNoRows):
			return ErrNothingToSend
		case err != nil:
			return fmt.Errorf("outbox: claim reply: %w", err)
		}
		r.TenantID = tenantID
		r.BindingID = bindingID.Int64
		r.WorkerID = workerID
		r.LeaseTTL = s.leaseTTL

		nextFence := r.FenceToken + 1
		if _, err := tx.Exec(ctx, `
			UPDATE reply_outbox
			SET delivery_fencing = ?, status = 'sending', lease_owner = ?,
			    lease_until = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
			WHERE tenant_id = ? AND outbox_id = ? AND delivery_fencing = ?`,
			nextFence, workerID, s.leaseTTL.Microseconds(), tenantID, r.OutboxID, r.FenceToken); err != nil {
			return fmt.Errorf("outbox: take delivery lease: %w", err)
		}
		r.FenceToken = nextFence
		if _, err := tx.Exec(ctx, `
			INSERT INTO delivery_attempts (outbox_id, attempt_no, worker_id, fencing_token, result)
			VALUES (?, ?, ?, ?, 'sending')`,
			r.OutboxID, r.Attempts+1, workerID, r.FenceToken); err != nil {
			return fmt.Errorf("outbox: open delivery attempt: %w", err)
		}
		out = &r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Deliver sends one claimed reply and records the outcome. A caller that
// cannot classify the failure should pass Unknown: that is the difference
// between "retry later" and "a human needs to look at this before anything
// else happens in this conversation."
func (s *Service) Deliver(ctx context.Context, c *ClaimedReply) (Outcome, error) {
	scope, err := s.db.Scope(c.TenantID)
	if err != nil {
		return Unknown, err
	}

	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	outcome, sendErr := s.sender.Send(sendCtx, c)
	cancel()
	if sendErr != nil && outcome == Sent {
		// A sender that claims success while also returning an error is
		// misbehaving; the safe reading is that the send is not established.
		outcome = Unknown
		sendErr = fmt.Errorf("outbox: sender returned Sent with an error: %w", sendErr)
	}

	switch outcome {
	case Sent:
		if err := s.recordSent(ctx, scope, c); err != nil {
			return outcome, err
		}
	case Rejected:
		if err := s.recordRetryOrDead(ctx, scope, c, "rejected", sendErr); err != nil {
			return outcome, err
		}
	case Unknown:
		if err := s.recordUnknown(ctx, scope, c, sendErr); err != nil {
			return outcome, err
		}
	}
	return outcome, sendErr
}

func (s *Service) recordSent(ctx context.Context, scope controlplane.Scope, c *ClaimedReply) error {
	_, err := scope.Exec(ctx, `
		UPDATE reply_outbox
		SET status = 'sent', sent_at = UTC_TIMESTAMP(6), lease_owner = NULL, lease_until = NULL, last_error = ''
		WHERE tenant_id = ? AND outbox_id = ? AND delivery_fencing = ?`,
		c.TenantID, c.OutboxID, c.FenceToken)
	if err != nil {
		return fmt.Errorf("outbox: mark sent: %w", err)
	}
	_, err = scope.Exec(ctx, `
		UPDATE delivery_attempts
		SET result = 'sent', finished_at = UTC_TIMESTAMP(6)
		WHERE outbox_id = ? AND fencing_token = ?`,
		c.OutboxID, c.FenceToken)
	if err != nil {
		return fmt.Errorf("outbox: close delivery attempt: %w", err)
	}
	return nil
}

// recordRetryOrDead is the path where the answer came back and it was "no".
// Retrying is safe: nothing was delivered, the platform said so.
func (s *Service) recordRetryOrDead(ctx context.Context, scope controlplane.Scope, c *ClaimedReply, reason string, cause error) error {
	attempt := int(c.Attempts) + 1
	if attempt > len(s.backoff) {
		if _, err := scope.Exec(ctx, `
			UPDATE reply_outbox
			SET status = 'dead', lease_owner = NULL, lease_until = NULL, last_error = ?
			WHERE tenant_id = ? AND outbox_id = ? AND delivery_fencing = ?`,
			errText(cause), c.TenantID, c.OutboxID, c.FenceToken); err != nil {
			return fmt.Errorf("outbox: dead-letter: %w", err)
		}
		slog.Warn("outbox: reply exhausted retries",
			"tenant", c.TenantID, "outbox_id", c.OutboxID, "attempts", attempt, "reason", reason)
		return nil
	}
	delay := s.backoff[attempt-1] + jitter()
	if _, err := scope.Exec(ctx, `
		UPDATE reply_outbox
		SET status = 'pending', attempts = ?, next_attempt_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND,
		    lease_owner = NULL, lease_until = NULL, last_error = ?
		WHERE tenant_id = ? AND outbox_id = ? AND delivery_fencing = ?`,
		attempt, delay.Microseconds(), errText(cause), c.TenantID, c.OutboxID, c.FenceToken); err != nil {
		return fmt.Errorf("outbox: schedule retry: %w", err)
	}
	return nil
}

// recordUnknown is the path a reply arrives in only if a human decides so.
// No next_attempt_at, no automatic retry: status = 'unknown' parks it exactly
// like an execution's unknown parks a session (approved plan, "不确定结果人工
// 核对"), and ClaimNext's status filter will never pick it up again.
func (s *Service) recordUnknown(ctx context.Context, scope controlplane.Scope, c *ClaimedReply, cause error) error {
	if _, err := scope.Exec(ctx, `
		UPDATE reply_outbox
		SET status = 'unknown', lease_owner = NULL, lease_until = NULL, last_error = ?
		WHERE tenant_id = ? AND outbox_id = ? AND delivery_fencing = ?`,
		errText(cause), c.TenantID, c.OutboxID, c.FenceToken); err != nil {
		return fmt.Errorf("outbox: mark unknown: %w", err)
	}
	if _, err := scope.Exec(ctx, `
		UPDATE delivery_attempts
		SET result = 'unknown', finished_at = UTC_TIMESTAMP(6), detail = ?
		WHERE outbox_id = ? AND fencing_token = ?`,
		errText(cause), c.OutboxID, c.FenceToken); err != nil {
		return fmt.Errorf("outbox: close unknown attempt: %w", err)
	}
	// A delivery that went unknown must block the conversation it belongs to,
	// not just this row: sending the *next* reply would paper over the fact
	// that nobody knows whether the last one arrived.
	if _, err := scope.Exec(ctx, `
		UPDATE sessions
		SET blocked_reason = 'reply delivery unknown'
		WHERE tenant_id = ? AND session_pk = (
			SELECT session_pk FROM reply_outbox WHERE tenant_id = ? AND outbox_id = ?)
		  AND blocked_reason IS NULL`,
		c.TenantID, c.TenantID, c.OutboxID); err != nil {
		return fmt.Errorf("outbox: block session after unknown delivery: %w", err)
	}
	return nil
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	r := []rune(msg)
	if len(r) > 512 {
		return string(r[:512])
	}
	return msg
}

// jitter spreads a fleet that all failed at the same instant apart on retry,
// so a shared upstream outage does not turn into a synchronized thundering
// herd. This is timing noise with no security content at all, so the global
// math/rand source — auto-seeded since Go 1.20 — is the right tool; a
// cryptographic RNG here would be spending the wrong budget on the wrong
// problem.
func jitter() time.Duration {
	return time.Duration(rand.Intn(500)) * time.Millisecond
}
