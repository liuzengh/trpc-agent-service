package channels

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
)

// Receiver is the reliable-mode receive face: the same verified callbacks the
// legacy gateway mounts, but every message is persisted — a KF notification
// row or an inbox row — BEFORE the platform gets its ACK. That order is the
// whole point: a process that dies before the ACK leaves the platform
// retrying, while the legacy "ACK, then dispatch in memory" order turns the
// same crash into a silently lost message.
//
// The Receiver only receives. Execution belongs to the worker role and
// pushing replies to the browser belongs to the webchat mailbox, so this
// process holds no runner, no session state and no stream registry.
type Receiver struct {
	cdp      *controlplane.DB
	inbox    *inbox.Service
	kfPuller *KfPuller
	kf       *WeChatKf
	wecom    *WeCom
	web      *WebChat
}

// NewReceiver builds the durable receive face over the control plane.
func NewReceiver(cdp *controlplane.DB) *Receiver {
	rc := &Receiver{
		cdp:   cdp,
		inbox: inbox.NewService(cdp),
	}
	rc.kfPuller = NewKfPuller(KfBindingLookup(cdp), cdp, rc.inbox)
	rc.kf = NewWeChatKf(KfBindingLookup(cdp)).WithDurableNotifications(rc.recordNotification)
	rc.wecom = NewWeCom(WeComBindingLookup(cdp)).WithDurableAccept(rc.accept)
	rc.web = NewWebChat().WithDurableAccept(rc.accept)
	return rc
}

// Routes mounts the three verified callbacks under their legacy paths. Each
// handler answers with the adapter's own ACK on success; on failure it
// answers 500 so the platform sees "persist failed, retry me".
func (rc *Receiver) Routes() map[string]http.Handler {
	routes := make(map[string]http.Handler, 3)
	for _, a := range []Adapter{rc.kf, rc.wecom, rc.web} {
		a := a
		prefix := "/callback/" + string(a.Type()) + "/"
		routes[prefix] = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantID := strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/")
			if tenantID == "" {
				http.Error(w, "tenant id is required in the callback path", http.StatusBadRequest)
				return
			}
			batch, err := a.Callback(w, r)
			if err != nil {
				// The durable paths write no ACK before a failure, so a
				// non-"success" answer here is exactly "persist failed,
				// retry me". WeChat retries non-conforming responses; a
				// signature mismatch retries a few times and gives up,
				// which is harmless and visible in the logs.
				slog.Error("receiver: callback failed",
					"channel", a.Type(), "tenant", tenantID, "err", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(batch) > 0 {
				// In durable mode the adapters hand off through persistence
				// and return no batch; a batch here is a wiring bug.
				slog.Error("receiver: adapter returned messages outside durable mode",
					"channel", a.Type(), "count", len(batch))
			}
		})
	}
	return routes
}

// recordNotification is the KF callback's durable step: resolve the tenant's
// binding and land the notification row. That row is what the jobs role's
// puller turns into inbox messages, so nothing else in this path may
// acknowledge first.
func (rc *Receiver) recordNotification(ctx context.Context, tenantID, eventToken, scopeKey string) error {
	scope, err := rc.cdp.Scope(tenantID)
	if err != nil {
		return err
	}
	row, err := scope.QueryRow(ctx, `
		SELECT binding_id FROM channel_bindings
		WHERE tenant_id = ? AND channel_type = 'wechat_kf' AND status = 'active'
		ORDER BY binding_id LIMIT 1`, tenantID)
	if err != nil {
		return fmt.Errorf("wechat_kf: resolve binding: %w", err)
	}
	var bindingID int64
	switch err := row.Scan(&bindingID); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("wechat_kf: tenant %q has no active binding", tenantID)
	case err != nil:
		return fmt.Errorf("wechat_kf: scan binding: %w", err)
	}
	_, err = rc.kfPuller.RecordNotification(ctx, tenantID, bindingID, eventToken, scopeKey)
	return err
}

// accept is the wecom/webchat durable step: resolve the binding's target
// (app, revision, profile versions) and commit the message into the
// inbox. The inbox's per-(binding, message id) unique key is the dedup: a
// platform retry of the same delivery is a no-op here, not a second turn.
func (rc *Receiver) accept(ctx context.Context, in *InboundMessage) error {
	target, err := rc.resolveTarget(ctx, in.TenantID, string(in.Channel))
	if err != nil {
		return err
	}
	if _, err := rc.inbox.Accept(ctx, in.TenantID, inbox.Request{
		AppID:                 target.appID,
		ChannelType:           string(in.Channel),
		BindingID:             target.bindingID,
		ActorKey:              in.UserID,
		RevisionID:            target.revisionID,
		ModelProfileID:        target.modelProfileID,
		BackendProfileID:      target.backendProfileID,
		ModelProfileVersion:   target.modelProfileVersion,
		BackendProfileVersion: target.backendProfileVersion,
		PlatformMessageID:     in.MsgID,
		Text:                  in.Text,
	}); err != nil {
		return fmt.Errorf("receiver: accept %s message: %w", in.Channel, err)
	}
	return nil
}

// receiveTarget is "which agent answers this binding, at which revision",
// resolved once per accepted message. It mirrors KfPuller.resolveTarget (the
// puller resolves per fetched page, this resolves per callback); the two
// stay one shape and are unified by the rollout resolver when one exists.
type receiveTarget struct {
	bindingID             int64
	appID                 int64
	revisionID            int64
	modelProfileID        int64
	backendProfileID      int64
	modelProfileVersion   uint32
	backendProfileVersion uint32
}

func (rc *Receiver) resolveTarget(ctx context.Context, tenantID, channelType string) (receiveTarget, error) {
	scope, err := rc.cdp.Scope(tenantID)
	if err != nil {
		return receiveTarget{}, err
	}
	row, err := scope.QueryRow(ctx, `
		SELECT cb.binding_id, aa.app_id, ar.revision_id, mp.profile_id, mp.version, bp.profile_id, bp.version
		FROM channel_bindings cb
		JOIN agent_apps aa
		  ON aa.tenant_id = cb.tenant_id AND aa.app_id = cb.app_id
		JOIN agent_revisions ar
		  ON ar.revision_id = aa.current_revision_id AND ar.app_id = aa.app_id
		JOIN model_profiles mp ON mp.profile_id = ar.model_profile_id
		JOIN backend_profiles bp ON bp.profile_id = ar.backend_profile_id
		WHERE cb.tenant_id = ? AND cb.channel_type = ? AND cb.status = 'active'
		ORDER BY cb.binding_id LIMIT 1`, tenantID, channelType)
	if err != nil {
		return receiveTarget{}, fmt.Errorf("receiver: resolve %s target: %w", channelType, err)
	}
	var t receiveTarget
	switch err := row.Scan(&t.bindingID, &t.appID, &t.revisionID,
		&t.modelProfileID, &t.modelProfileVersion, &t.backendProfileID, &t.backendProfileVersion); {
	case errors.Is(err, sql.ErrNoRows):
		return receiveTarget{}, fmt.Errorf(
			"receiver: tenant %q has no active %s binding with a published revision", tenantID, channelType)
	case err != nil:
		return receiveTarget{}, fmt.Errorf("receiver: scan %s target: %w", channelType, err)
	}
	return t, nil
}
