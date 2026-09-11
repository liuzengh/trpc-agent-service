package channels

// This file is the durable/reliable half of the WeChat customer-service
// channel (approved plan, "接收端"). The legacy in-process flow lives in
// wechatkf.go and stays for the legacy gateway; the split is deliberate:
//
//   - legacy: callback → verify → ACK → pull inline → hand messages to an
//     in-process goroutine. Fast, and it loses work when the process dies
//     mid-pull or mid-dispatch.
//   - durable: callback → verify → *record a notification row* → ACK. The
//     jobs role later claims that notification, pulls with a cursor stored in
//     MySQL, and admits each fetched page into the inbox in the same
//     transaction as the cursor advance. A crash at any point loses nothing:
//     either the cursor moved and the messages are in the inbox, or neither
//     happened and the notification is still pending.
//
// Everything protocol-level (signature, AES, sync_msg/send_msg wire shapes,
// token caching, rune-boundary splitting) is shared with the legacy adapter
// through the helpers in wecom.go/wechatkf.go rather than reimplemented.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrNothingToPull is what PullOnce returns when no notification is pending.
// It is the normal idle state, not a failure.
var ErrNothingToPull = errors.New("wechat_kf: no notification is pending")

// errScopeLeaseLost means another puller took over this binding+open_kfid
// while this one was mid-page. The page transaction must be rolled back: the
// new owner is authoritative and two writers must never advance one cursor.
var errScopeLeaseLost = errors.New("wechat_kf: the sync scope lease was taken over")

// kfDefaultLeaseTTL bounds both the notification claim and the per-scope sync
// lease. Kept equal on purpose: whichever a crashed puller was holding, both
// become reclaimable at the same moment, so recovery needs no special case.
const kfDefaultLeaseTTL = time.Minute

// KfPuller is the outbox.Sender for 微信客服 replies; the assertion keeps a
// signature drift (a wider Sender interface) from only surfacing at wiring
// time in main.
var _ outbox.Sender = (*KfPuller)(nil)

// KfPuller pulls WeChat KF messages durably for every binding of one tenant
// per call, into MySQL rather than into process memory.
type KfPuller struct {
	lookup   func(tenantID string) (*tenant.WeChatKfBinding, bool)
	db       *controlplane.DB
	inbox    *inbox.Service
	apiBase  string
	client   *http.Client
	leaseTTL time.Duration

	// tokens mirrors the legacy adapter's cache (per corp_id, because the KF
	// secret differs from the WeCom app secret). It is per-process only: a
	// cached token is a performance win, and fetching one again after a
	// restart is exactly what would happen anyway.
	mu     sync.Mutex
	tokens map[string]*wecomToken
}

// NewKfPuller wires the puller to the control plane and the inbox. lookup
// resolves credentials the same way the legacy adapter's does.
func NewKfPuller(
	lookup func(tenantID string) (*tenant.WeChatKfBinding, bool),
	db *controlplane.DB,
	inboxSvc *inbox.Service,
) *KfPuller {
	return &KfPuller{
		lookup:   lookup,
		db:       db,
		inbox:    inboxSvc,
		apiBase:  wecomDefaultAPIBase,
		client:   &http.Client{Timeout: 10 * time.Second},
		leaseTTL: kfDefaultLeaseTTL,
		tokens:   make(map[string]*wecomToken),
	}
}

// WithAPIBase points the puller at a different KF API host (tests).
func (p *KfPuller) WithAPIBase(base string) *KfPuller {
	p.apiBase = base
	return p
}

// WithLeaseTTL shortens the leases for tests that cannot wait out a minute.
func (p *KfPuller) WithLeaseTTL(ttl time.Duration) *KfPuller {
	if ttl > 0 {
		p.leaseTTL = ttl
	}
	return p
}

// BindingID resolves the tenant's active 微信客服 binding. The reliable
// callback URL keeps the legacy shape (/callback/wechat_kf/{tenant}), which
// names no binding, so a tenant answers on its active KF binding; a tenant
// with several would need the URL to carry the binding's public id, which is
// a deliberate later decision, not an accident here.
func (p *KfPuller) BindingID(ctx context.Context, tenantID string) (int64, bool, error) {
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return 0, false, err
	}
	row, err := scope.QueryRow(ctx, `
		SELECT binding_id FROM channel_bindings
		WHERE tenant_id = ? AND channel_type = 'wechat_kf' AND status = 'active'
		ORDER BY binding_id LIMIT 1`, tenantID)
	if err != nil {
		return 0, false, err
	}
	var id int64
	switch err := row.Scan(&id); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("wechat_kf: resolve binding: %w", err)
	}
	return id, true, nil
}

// RecordNotification makes the callback's fact durable before it is ACKed.
//
// It replaces the legacy flow's "ACK, then pull right here". WeChat retries a
// callback it believes was not ACKed, so the same event can legitimately
// arrive twice and produce two notification rows; that is harmless because
// both pull from the same persisted cursor and the inbox deduplicates by
// message id. What would not be harmless is losing the event, and that is
// what this row prevents.
func (p *KfPuller) RecordNotification(
	ctx context.Context,
	tenantID string,
	bindingID int64,
	eventToken, scopeKey string,
) (int64, error) {
	if bindingID == 0 || scopeKey == "" {
		return 0, errors.New("wechat_kf: notification needs a binding and an open_kfid")
	}
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return 0, err
	}
	res, err := scope.Exec(ctx, `
		INSERT INTO channel_notifications (tenant_id, binding_id, event_token, scope_key)
		VALUES (?, ?, ?, ?)`,
		tenantID, bindingID, eventToken, scopeKey)
	if err != nil {
		return 0, fmt.Errorf("wechat_kf: record notification: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("wechat_kf: notification id: %w", err)
	}
	return id, nil
}

// kfTarget is "which agent answers this binding", resolved once per pull.
//
// The inbox needs a published revision, and a session created by this pull
// must be pinned to the one current at the moment of acceptance — resolving
// it per message instead of per pull would let a publish landing mid-page
// split one fetched page across two revisions, which is exactly the kind of
// mid-conversation version change the fixed-version rule exists to prevent.
type kfTarget struct {
	appID                 int64
	revisionID            int64
	modelProfileID        int64
	backendProfileID      int64
	modelProfileVersion   uint32
	backendProfileVersion uint32
}

// resolveTarget reads the binding's app and that app's current revision,
// with both profile versions, in one statement.
//
// A binding whose app has nothing published cannot answer: that is not a
// transient condition, so callers treat it as terminal for the notification
// rather than retrying it forever.
func (p *KfPuller) resolveTarget(ctx context.Context, tenantID string, bindingID int64) (kfTarget, error) {
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return kfTarget{}, err
	}
	row, err := scope.QueryRow(ctx, `
		SELECT aa.app_id, ar.revision_id, mp.profile_id, mp.version, bp.profile_id, bp.version
		FROM channel_bindings cb
		JOIN agent_apps aa
		  ON aa.tenant_id = cb.tenant_id AND aa.app_id = cb.app_id
		JOIN agent_revisions ar
		  ON ar.revision_id = aa.current_revision_id AND ar.app_id = aa.app_id
		JOIN model_profiles mp ON mp.profile_id = ar.model_profile_id
		JOIN backend_profiles bp ON bp.profile_id = ar.backend_profile_id
		WHERE cb.tenant_id = ? AND cb.binding_id = ?`, tenantID, bindingID)
	if err != nil {
		return kfTarget{}, err
	}
	var t kfTarget
	switch err := row.Scan(&t.appID, &t.revisionID,
		&t.modelProfileID, &t.modelProfileVersion,
		&t.backendProfileID, &t.backendProfileVersion); {
	case errors.Is(err, sql.ErrNoRows):
		return kfTarget{}, errors.New("wechat_kf: the binding's app has no published revision")
	case err != nil:
		return kfTarget{}, fmt.Errorf("wechat_kf: resolve target: %w", err)
	}
	return t, nil
}

// kfNotification is one claimed notification row.
type kfNotification struct {
	id         int64
	bindingID  int64
	eventToken string
	scopeKey   string
	fence      uint64
}

// PullOnce claims at most one notification and drains it. It returns
// processed=true when it did any bookkeeping at all, so a caller loop can
// tell "queue empty" from "worked".
func (p *KfPuller) PullOnce(ctx context.Context, tenantID, workerID string) (bool, error) {
	n, err := p.claimNotification(ctx, tenantID, workerID)
	if errors.Is(err, ErrNothingToPull) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	b, ok := p.lookup(tenantID)
	if !ok {
		// No usable credentials: this notification can never succeed, so it
		// is failed rather than retried forever against a wall.
		p.finishNotification(ctx, tenantID, n.id, workerID, "failed", "tenant has no wechat_kf binding")
		return true, fmt.Errorf("wechat_kf: tenant %q has no binding for a pending notification", tenantID)
	}
	target, err := p.resolveTarget(ctx, tenantID, n.bindingID)
	if err != nil {
		p.finishNotification(ctx, tenantID, n.id, workerID, "failed", err.Error())
		return true, nil
	}

	lease, acquired, err := p.acquireScope(ctx, tenantID, n.bindingID, n.scopeKey, workerID)
	if err != nil {
		return true, err
	}
	if !acquired {
		// Another puller is draining this scope right now. Hand the
		// notification back untouched: it will be picked up again once that
		// puller's lease ends, and two pullers never share one cursor.
		if err := p.unclaimNotification(ctx, tenantID, n.id, workerID); err != nil {
			return true, err
		}
		return false, nil
	}
	defer lease.release(ctx)

	token, err := p.accessToken(ctx, b)
	if err != nil {
		p.unclaimNotification(ctx, tenantID, n.id, workerID)
		return true, fmt.Errorf("wechat_kf: access token: %w", err)
	}

	for round := 0; round < kfSyncMaxRounds; round++ {
		resp, err := p.syncMsg(ctx, token, lease.cursor, n)
		if err != nil {
			// Nothing has been committed for this round; leave the
			// notification pending so a later attempt resumes from the same
			// cursor.
			p.unclaimNotification(ctx, tenantID, n.id, workerID)
			return true, fmt.Errorf("wechat_kf: sync_msg: %w", err)
		}
		nextCursor := resp.NextCursor
		if nextCursor == "" {
			nextCursor = lease.cursor
		}
		done := resp.HasMore == 0
		requests := p.pageRequests(tenantID, n, target, resp.MsgList)

		err = p.commitPage(ctx, tenantID, n, workerID, lease, requests, nextCursor, done)
		switch {
		case errors.Is(err, inbox.ErrConflict):
			// The same message id exists with different content. That is not
			// a transient condition and retrying will not clear it: park the
			// notification for a human instead of looping on it.
			p.finishNotification(ctx, tenantID, n.id, workerID, "failed", "conflicting duplicate message id")
			return true, nil
		case errors.Is(err, errScopeLeaseLost):
			// We no longer own the scope; the page rolled back with us. Let
			// whoever took over proceed, and retry this notification later.
			p.unclaimNotification(ctx, tenantID, n.id, workerID)
			return true, nil
		case err != nil:
			p.unclaimNotification(ctx, tenantID, n.id, workerID)
			return true, err
		}
		if done {
			return true, nil
		}
	}

	// The server still has more after the round cap. Put the notification
	// back: the cursor is saved, so the next claim continues where this one
	// stopped. Dropping the remainder would silently lose the backlog.
	p.unclaimNotification(ctx, tenantID, n.id, workerID)
	return true, nil
}

// Run drains every active tenant until ctx is cancelled. Tenant enumeration
// is re-read on every sweep, so a tenant created through the Admin API starts
// being served without a restart.
func (p *KfPuller) Run(ctx context.Context, workerID string, idle time.Duration) error {
	if idle <= 0 {
		idle = time.Second
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tenants, err := p.db.ListActiveTenants(ctx)
		if err != nil {
			slog.Error("wechat_kf: list tenants", "err", err)
			if !sleepCtx(ctx, idle) {
				return ctx.Err()
			}
			continue
		}
		worked := false
		for _, tenantID := range tenants {
			ok, err := p.PullOnce(ctx, tenantID, workerID)
			if err != nil {
				slog.Warn("wechat_kf: pull", "tenant", tenantID, "err", err)
			}
			worked = worked || ok
		}
		if !worked && !sleepCtx(ctx, idle) {
			return ctx.Err()
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// claimNotification takes one pending notification for this tenant, skipping
// any whose lease is still alive.
//
// The join on channel_bindings pins two facts in one statement: the row
// belongs to a KF binding (this puller must never consume another channel's
// work), and that binding still exists.
func (p *KfPuller) claimNotification(ctx context.Context, tenantID, workerID string) (*kfNotification, error) {
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return nil, err
	}
	var out *kfNotification
	err = scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		var n kfNotification
		row := tx.QueryRow(ctx, `
			SELECT cn.notification_id, cn.binding_id, cn.event_token, cn.scope_key, cn.fencing_token
			FROM channel_notifications cn
			JOIN channel_bindings cb
			  ON cb.tenant_id = cn.tenant_id AND cb.binding_id = cn.binding_id
			 AND cb.channel_type = 'wechat_kf' AND cb.status = 'active'
			WHERE cn.tenant_id = ? AND cn.status = 'pending'
			  AND (cn.lease_until IS NULL OR cn.lease_until < UTC_TIMESTAMP(6))
			ORDER BY cn.notification_id
			LIMIT 1
			FOR UPDATE SKIP LOCKED`, tenantID)
		var token sql.NullString
		switch err := row.Scan(&n.id, &n.bindingID, &token, &n.scopeKey, &n.fence); {
		case errors.Is(err, sql.ErrNoRows):
			return ErrNothingToPull
		case err != nil:
			return fmt.Errorf("wechat_kf: claim notification: %w", err)
		}
		n.eventToken = token.String

		nextFence := n.fence + 1
		if _, err := tx.Exec(ctx, `
			UPDATE channel_notifications
			SET status = 'running', lease_owner = ?, fencing_token = ?,
			    lease_until = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
			WHERE tenant_id = ? AND notification_id = ?`,
			workerID, nextFence, p.leaseTTL.Microseconds(), tenantID, n.id); err != nil {
			return fmt.Errorf("wechat_kf: take notification lease: %w", err)
		}
		n.fence = nextFence
		out = &n
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (p *KfPuller) unclaimNotification(ctx context.Context, tenantID string, id int64, workerID string) error {
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return err
	}
	_, err = scope.Exec(ctx, `
		UPDATE channel_notifications
		SET status = 'pending', lease_owner = NULL, lease_until = NULL
		WHERE tenant_id = ? AND notification_id = ? AND lease_owner = ? AND status = 'running'`,
		tenantID, id, workerID)
	if err != nil {
		return fmt.Errorf("wechat_kf: requeue notification: %w", err)
	}
	return nil
}

func (p *KfPuller) finishNotification(ctx context.Context, tenantID string, id int64, workerID, status, detail string) {
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return
	}
	if _, err := scope.Exec(ctx, `
		UPDATE channel_notifications
		SET status = ?, lease_owner = NULL, lease_until = NULL
		WHERE tenant_id = ? AND notification_id = ? AND lease_owner = ?`,
		status, tenantID, id, workerID); err != nil {
		slog.Error("wechat_kf: close notification", "tenant", tenantID, "id", id, "err", err)
	}
	if detail != "" {
		slog.Warn("wechat_kf: notification closed",
			"tenant", tenantID, "id", id, "status", status, "detail", detail)
	}
}

// pageRequests normalises one sync_msg page into inbox requests. Only
// customer text messages become inbound (msgtype=text, origin=3), matching
// the legacy adapter's filter exactly — an event or a servicer reply is
// protocol noise, not a user message.
//
// Unlike the legacy flow there is no boot-time cutoff: a fresh deployment
// would otherwise drop the server's replayable history based on when a
// process happened to start, and the durable cursor is what makes that rule
// unnecessary — it persists across restarts, and an operator who wants to
// skip the backlog initialises the cursor explicitly instead.
func (p *KfPuller) pageRequests(tenantID string, n *kfNotification, target kfTarget, msgs []kfSyncMsg) []inbox.Request {
	var out []inbox.Request
	for _, m := range msgs {
		if m.MsgType != "text" || m.Origin != 3 || m.Text.Content == "" {
			continue
		}
		out = append(out, inbox.Request{
			AppID:                 target.appID,
			ChannelType:           string(TypeWeChatKF),
			BindingID:             n.bindingID,
			ActorKey:              m.ExternalUserID,
			RevisionID:            target.revisionID,
			ModelProfileID:        target.modelProfileID,
			BackendProfileID:      target.backendProfileID,
			ModelProfileVersion:   target.modelProfileVersion,
			BackendProfileVersion: target.backendProfileVersion,
			PlatformMessageID:     m.Msgid,
			Text:                  m.Text.Content,
		})
	}
	return out
}

// commitPage is the transactional heart: the whole page's inbox rows, the
// reply routes, the cursor advance, and (on the last page) the notification's
// completion all succeed together or none do.
func (p *KfPuller) commitPage(
	ctx context.Context,
	tenantID string,
	n *kfNotification,
	workerID string,
	lease *kfScopeLease,
	requests []inbox.Request,
	nextCursor string,
	done bool,
) error {
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return err
	}
	return scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		for _, req := range requests {
			acc, err := p.inbox.AcceptInTx(ctx, tx, tenantID, req)
			if err != nil {
				return err
			}
			// The reply route is how the delivery role, possibly in another
			// process hours later, learns which open_kfid to answer on. It
			// belongs in this transaction for the same reason the cursor
			// does: a committed message whose route was never recorded is a
			// message nobody can reply to.
			if _, err := tx.Exec(ctx, `
				INSERT INTO channel_reply_routes (tenant_id, session_pk, binding_id, scope_key)
				VALUES (?, ?, ?, ?)
				ON DUPLICATE KEY UPDATE binding_id = VALUES(binding_id), scope_key = VALUES(scope_key)`,
				tenantID, acc.SessionPK, n.bindingID, n.scopeKey); err != nil {
				return fmt.Errorf("wechat_kf: record reply route: %w", err)
			}
		}
		if err := lease.saveCursor(ctx, tx, nextCursor); err != nil {
			return err
		}
		if done {
			if _, err := tx.Exec(ctx, `
				UPDATE channel_notifications
				SET status = 'done', lease_owner = NULL, lease_until = NULL
				WHERE tenant_id = ? AND notification_id = ? AND lease_owner = ?`,
				tenantID, n.id, workerID); err != nil {
				return fmt.Errorf("wechat_kf: complete notification: %w", err)
			}
		}
		return nil
	})
}

// kfScopeLease is one holder's right to advance a binding+open_kfid cursor.
// The version is a compare-and-swap token: it moves on every acquisition and
// every save, so a puller that lost the scope mid-page finds out at its next
// save and rolls the page back rather than overwriting a newer cursor.
type kfScopeLease struct {
	puller    *KfPuller
	tenantID  string
	bindingID int64
	scopeKey  string
	owner     string
	version   uint64
	cursor    string
	held      bool
}

func (p *KfPuller) acquireScope(ctx context.Context, tenantID string, bindingID int64, scopeKey, owner string) (*kfScopeLease, bool, error) {
	scope, err := p.db.Scope(tenantID)
	if err != nil {
		return nil, false, err
	}
	// Ensure the checkpoint exists before trying to lock it: an UPDATE that
	// matches nothing reports zero rows, which is indistinguishable from
	// "someone else holds it" and would make a brand-new scope look busy.
	if _, err := scope.Exec(ctx, `
		INSERT IGNORE INTO channel_checkpoints (tenant_id, binding_id, scope_key, value, version)
		VALUES (?, ?, ?, '', 0)`, tenantID, bindingID, scopeKey); err != nil {
		return nil, false, fmt.Errorf("wechat_kf: ensure checkpoint: %w", err)
	}

	res, err := scope.Exec(ctx, `
		UPDATE channel_checkpoints
		SET lease_owner = ?, lease_until = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND,
		    version = version + 1
		WHERE tenant_id = ? AND binding_id = ? AND scope_key = ?
		  AND (lease_owner IS NULL OR lease_owner = ? OR lease_until < UTC_TIMESTAMP(6))`,
		owner, p.leaseTTL.Microseconds(), tenantID, bindingID, scopeKey, owner)
	if err != nil {
		return nil, false, fmt.Errorf("wechat_kf: take scope lease: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, false, nil
	}

	var (
		value   string
		version uint64
	)
	row, err := scope.QueryRow(ctx, `
		SELECT value, version FROM channel_checkpoints
		WHERE tenant_id = ? AND binding_id = ? AND scope_key = ? AND lease_owner = ?`,
		tenantID, bindingID, scopeKey, owner)
	if err != nil {
		return nil, false, err
	}
	if err := row.Scan(&value, &version); err != nil {
		return nil, false, fmt.Errorf("wechat_kf: read checkpoint after lease: %w", err)
	}
	return &kfScopeLease{
		puller:    p,
		tenantID:  tenantID,
		bindingID: bindingID,
		scopeKey:  scopeKey,
		owner:     owner,
		version:   version,
		cursor:    value,
		held:      true,
	}, true, nil
}

// saveCursor advances the cursor inside the caller's page transaction, but
// only if this lease is still the current holder at the version it read. A
// zero-row update means someone else moved the cursor already; the caller
// must roll the page back, which is exactly what returning an error achieves.
func (l *kfScopeLease) saveCursor(ctx context.Context, tx *controlplane.TxScope, cursor string) error {
	res, err := tx.Exec(ctx, `
		UPDATE channel_checkpoints
		SET value = ?, version = version + 1,
		    lease_until = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND
		WHERE tenant_id = ? AND binding_id = ? AND scope_key = ?
		  AND lease_owner = ? AND version = ?`,
		cursor, l.puller.leaseTTL.Microseconds(), l.tenantID, l.bindingID, l.scopeKey,
		l.owner, l.version)
	if err != nil {
		return fmt.Errorf("wechat_kf: save cursor: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return errScopeLeaseLost
	}
	l.version++
	l.cursor = cursor
	return nil
}

func (l *kfScopeLease) release(ctx context.Context) {
	if l == nil || !l.held {
		return
	}
	l.held = false
	scope, err := l.puller.db.Scope(l.tenantID)
	if err != nil {
		return
	}
	if _, err := scope.Exec(ctx, `
		UPDATE channel_checkpoints
		SET lease_owner = NULL, lease_until = NULL
		WHERE tenant_id = ? AND binding_id = ? AND scope_key = ? AND lease_owner = ?`,
		l.tenantID, l.bindingID, l.scopeKey, l.owner); err != nil {
		slog.Warn("wechat_kf: release scope lease", "err", err)
	}
}

// syncMsg is one kf/sync_msg round using the durable cursor.
func (p *KfPuller) syncMsg(ctx context.Context, token, cursor string, n *kfNotification) (kfSyncResponse, error) {
	body, err := json.Marshal(kfSyncRequest{
		Cursor: cursor, Token: n.eventToken, Limit: kfSyncLimit, OpenKfID: n.scopeKey,
	})
	if err != nil {
		return kfSyncResponse{}, err
	}
	u := p.apiBase + "/cgi-bin/kf/sync_msg?access_token=" + url.QueryEscape(token)
	var resp kfSyncResponse
	if err := apiDoJSON(ctx, p.client, http.MethodPost, u, body, &resp); err != nil {
		return kfSyncResponse{}, err
	}
	if resp.ErrCode != 0 {
		return kfSyncResponse{}, &wecomAPIError{ErrCode: resp.ErrCode, ErrMsg: resp.ErrMsg}
	}
	return resp, nil
}

// Send implements outbox.Sender for replies that belong to a KF conversation.
//
// Classification is the whole point of the outbox contract, so it is spelled
// out here: a transport failure (no answer at all) is Unknown and must never
// be retried automatically, because the message may have arrived; anything
// the KF API answered with is Rejected, because the platform told us what it
// thought.
func (p *KfPuller) Send(ctx context.Context, reply *outbox.ClaimedReply) (outbox.Outcome, error) {
	scope, err := p.db.Scope(reply.TenantID)
	if err != nil {
		return outbox.Unknown, err
	}
	var scopeKey string
	row, err := scope.QueryRow(ctx, `
		SELECT scope_key FROM channel_reply_routes
		WHERE tenant_id = ? AND session_pk = ?`, reply.TenantID, reply.SessionPK)
	if err != nil {
		return outbox.Unknown, err
	}
	switch err := row.Scan(&scopeKey); {
	case errors.Is(err, sql.ErrNoRows):
		// There is nowhere to send this reply, and no retry will create a
		// route: permanent, so the row belongs in the dead-letter state, not
		// in a loop.
		return outbox.Rejected, errors.New("wechat_kf: no reply route recorded for this session")
	case err != nil:
		return outbox.Unknown, fmt.Errorf("wechat_kf: load reply route: %w", err)
	}

	b, ok := p.lookup(reply.TenantID)
	if !ok {
		return outbox.Rejected, errors.New("wechat_kf: tenant has no binding")
	}
	token, err := p.accessToken(ctx, b)
	if err != nil {
		return classifyKfError(err)
	}
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, part := range splitUTF8(reply.Text, kfMaxTextBytes) {
		if err := p.postText(sendCtx, token, reply.Target, scopeKey, part); err != nil {
			if apiErr, ok := err.(*wecomAPIError); ok && apiErr.tokenInvalid() {
				// One refresh-and-retry, same as the legacy adapter: a token
				// can expire between cache checks and the send.
				p.mu.Lock()
				delete(p.tokens, b.CorpID)
				p.mu.Unlock()
				fresh, ferr := p.accessToken(ctx, b)
				if ferr != nil {
					return classifyKfError(ferr)
				}
				if err := p.postText(sendCtx, fresh, reply.Target, scopeKey, part); err != nil {
					return classifyKfError(err)
				}
				continue
			}
			return classifyKfError(err)
		}
	}
	return outbox.Sent, nil
}

// classifyKfError maps a send failure onto the outbox outcome vocabulary: an
// answer from the platform is Rejected; anything else means we do not know
// whether it arrived.
func classifyKfError(err error) (outbox.Outcome, error) {
	var apiErr *wecomAPIError
	if errors.As(err, &apiErr) {
		return outbox.Rejected, err
	}
	return outbox.Unknown, err
}

func (p *KfPuller) postText(ctx context.Context, token, toUser, kfID, content string) error {
	payload := struct {
		ToUser   string `json:"touser"`
		OpenKfID string `json:"open_kfid"`
		MsgType  string `json:"msgtype"`
		Text     struct {
			Content string `json:"content"`
		} `json:"text"`
	}{ToUser: toUser, OpenKfID: kfID, MsgType: "text"}
	payload.Text.Content = content
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	u := p.apiBase + "/cgi-bin/kf/send_msg?access_token=" + url.QueryEscape(token)
	var out struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
		MsgID   string `json:"msgid"`
	}
	if err := apiDoJSON(ctx, p.client, http.MethodPost, u, body, &out); err != nil {
		return err
	}
	if out.ErrCode != 0 {
		return &wecomAPIError{ErrCode: out.ErrCode, ErrMsg: out.ErrMsg}
	}
	return nil
}

// accessToken mirrors the legacy adapter's caching, and is the same code
// path deliberately: the two flows must agree on how a KF credential is
// fetched or a token fetch bug would only show up in one of them.
func (p *KfPuller) accessToken(ctx context.Context, b *tenant.WeChatKfBinding) (string, error) {
	p.mu.Lock()
	if t, ok := p.tokens[b.CorpID]; ok && time.Now().Before(t.expiresAt) {
		p.mu.Unlock()
		return t.token, nil
	}
	p.mu.Unlock()

	token, ttl, err := fetchAccessToken(ctx, p.client, p.apiBase, b.CorpID, b.Secret)
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	p.tokens[b.CorpID] = &wecomToken{token: token, expiresAt: time.Now().Add(ttl)}
	p.mu.Unlock()
	return token, nil
}
