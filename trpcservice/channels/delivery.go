package channels

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// DefaultKFAPIBase is the official 微信客服 API host. Exported so the role
// wiring (KF_API_BASE in cmd/trpc-service/roles.go) and this package agree on
// one value rather than repeating the literal.
const DefaultKFAPIBase = wecomDefaultAPIBase

// DefaultWeComAPIBase is the official 企业微信 API host, exported for the
// same reason as DefaultKFAPIBase.
const DefaultWeComAPIBase = wecomDefaultAPIBase

// KfBindingLookup resolves a tenant's 微信客服 credentials out of the control
// plane, in the shape the channel code already takes (a (binding, ok) closure
// — the legacy adapters resolve theirs from YAML the same way).
//
// The credentials live in channel_bindings.config as JSON. Errors deliberately
// collapse into "not found" because every caller of this closure is already
// inside a path that has to decide whether to fail a notification or a
// delivery; a loud per-lookup error would either be dropped or would need a
// second error channel nobody uses. The cause is logged here instead.
func KfBindingLookup(cdp *controlplane.DB) func(tenantID string) (*tenant.WeChatKfBinding, bool) {
	return func(tenantID string) (*tenant.WeChatKfBinding, bool) {
		scope, err := cdp.Scope(tenantID)
		if err != nil {
			slog.Error("wechat_kf: binding lookup scope", "tenant", tenantID, "err", err)
			return nil, false
		}
		row, err := scope.QueryRow(context.Background(), `
			SELECT config FROM channel_bindings
			WHERE tenant_id = ? AND channel_type = 'wechat_kf' AND status = 'active'
			ORDER BY binding_id LIMIT 1`, tenantID)
		if err != nil {
			slog.Error("wechat_kf: binding lookup", "tenant", tenantID, "err", err)
			return nil, false
		}
		var raw sql.NullString
		switch err := row.Scan(&raw); {
		case errors.Is(err, sql.ErrNoRows):
			return nil, false
		case err != nil:
			slog.Error("wechat_kf: binding scan", "tenant", tenantID, "err", err)
			return nil, false
		}
		if !raw.Valid || raw.String == "" {
			return nil, false
		}
		var b tenant.WeChatKfBinding
		if err := json.Unmarshal([]byte(raw.String), &b); err != nil {
			slog.Error("wechat_kf: binding decode", "tenant", tenantID, "err", err)
			return nil, false
		}
		return &b, true
	}
}

// WeComBindingLookup mirrors KfBindingLookup for the 企业微信 app binding:
// the caller decides what a failed delivery means, so errors collapse into
// "not found" and the cause is logged here.
func WeComBindingLookup(cdp *controlplane.DB) func(tenantID string) (*tenant.WeComBinding, bool) {
	return func(tenantID string) (*tenant.WeComBinding, bool) {
		scope, err := cdp.Scope(tenantID)
		if err != nil {
			slog.Error("wecom: binding lookup scope", "tenant", tenantID, "err", err)
			return nil, false
		}
		row, err := scope.QueryRow(context.Background(), `
			SELECT config FROM channel_bindings
			WHERE tenant_id = ? AND channel_type = 'wecom' AND status = 'active'
			ORDER BY binding_id LIMIT 1`, tenantID)
		if err != nil {
			slog.Error("wecom: binding lookup", "tenant", tenantID, "err", err)
			return nil, false
		}
		var raw sql.NullString
		switch err := row.Scan(&raw); {
		case errors.Is(err, sql.ErrNoRows):
			return nil, false
		case err != nil:
			slog.Error("wecom: binding scan", "tenant", tenantID, "err", err)
			return nil, false
		}
		if !raw.Valid || raw.String == "" {
			return nil, false
		}
		var b tenant.WeComBinding
		if err := json.Unmarshal([]byte(raw.String), &b); err != nil {
			slog.Error("wecom: binding decode", "tenant", tenantID, "err", err)
			return nil, false
		}
		return &b, true
	}
}

// DeliverySender routes one committed reply to the channel that owns it.
//
// All three first-class channels are wired: 微信客服 and 企业微信 deliver by
// calling their platform APIs, webchat by confirming the outbox row — the
// browser push itself belongs to the reliable gateway's mailbox, which reads
// the same rows and flips pushed_at only after the SSE write succeeded. Any
// other channel answers with a named rejection rather than a silent success:
// a reply that cannot be sent must be visible as such in reply_outbox, never
// quietly dropped.
type DeliverySender struct {
	KF      *KfPuller
	WeCom   *WeComSender
	WebChat outbox.Sender
}

// Send implements outbox.Sender.
func (d *DeliverySender) Send(ctx context.Context, reply *outbox.ClaimedReply) (outbox.Outcome, error) {
	switch Type(reply.ChannelType) {
	case TypeWeChatKF:
		if d.KF == nil {
			return outbox.Rejected, errors.New("channels: wechat_kf sender is not configured")
		}
		return d.KF.Send(ctx, reply)
	case TypeWeCom:
		if d.WeCom == nil {
			return outbox.Rejected, errors.New("channels: wecom sender is not configured")
		}
		return d.WeCom.Send(ctx, reply)
	case TypeWebChat:
		if d.WebChat == nil {
			return outbox.Rejected, errors.New("channels: webchat sender is not configured")
		}
		return d.WebChat.Send(ctx, reply)
	default:
		// Rejected, not Unknown: nothing external was attempted, so there is
		// no "it might have arrived" ambiguity to protect.
		return outbox.Rejected, fmt.Errorf(
			"channels: %s delivery is not wired into the reliable path yet", reply.ChannelType)
	}
}
