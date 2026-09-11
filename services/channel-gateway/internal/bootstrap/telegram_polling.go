package bootstrap

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	telegram "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/telegramadapter"
	admission "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	receptionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/telegramreceptionpostgres"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/telegramreception"
)

type telegramPollingIntake struct{ next telegram.Acceptor }

func (b telegramPollingIntake) Accept(ctx context.Context, p *c.Permit, l d.Lease, raw json.RawMessage) error {
	if p.Check() != nil || p.Binding().Kind != "telegram_receiver" {
		return c.ErrUnauthorized
	}
	in, e := telegram.Normalize(raw, l.AccountID, time.Now().UTC())
	if e != nil {
		return e
	}
	in.TelegramFence = &admission.TelegramFence{ScopeID: l.ScopeID, InstanceID: l.InstanceID, InstanceEpoch: l.InstanceEpoch, Epoch: l.Epoch, Revision: l.Revision}
	_, e = b.next.AcceptInbound(context.WithValue(ctx, permitKey{}, p), in)
	return e
}

type telegramGuardBridge struct{ store *receptionpg.Store }

func (b telegramGuardBridge) Required(ctx context.Context) bool {
	return permitFrom(ctx).Binding().Kind == "telegram_receiver"
}
func (b telegramGuardBridge) VerifyPolling(ctx context.Context, tx pgx.Tx, scope, account, instance, boot string, epoch, revision int64) error {
	p := permitFrom(ctx)
	bound := p.Binding()
	if p.Check() != nil || bound.Kind != "telegram_receiver" || bound.ScopeID != scope || bound.AccountID != account || bound.InstanceID != instance || bound.InstanceEpoch != boot || bound.ConnectionRevision != revision {
		return c.ErrUnauthorized
	}
	return b.store.VerifyPolling(ctx, tx, scope, account, instance, boot, epoch, revision)
}
