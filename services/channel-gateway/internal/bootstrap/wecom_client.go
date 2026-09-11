package bootstrap

import (
	"context"
	"errors"
	managed "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/wecomclient"
	"log/slog"

	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	inbound "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/wecomadapter"
	admission "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	connectiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

// wecomClientFactory only composes protocol and business adapters. Ownership,
// renewal, revision transitions and shutdown ordering remain in Connection.
type wecomClientFactory struct {
	acceptor inbound.Acceptor
	url      string
}

func (f wecomClientFactory) New(ctx context.Context, a connectiondomain.Account, g connectiondomain.OwnerGrant, credential connection.CredentialMaterial) (connection.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h, err := inbound.NewHandler(a.ID, a.BotID, admission.ConnectionFence{InstanceID: g.InstanceID, Epoch: g.Epoch, Revision: g.Revision}, f.acceptor)
	if err != nil {
		return nil, errors.New("construct WeCom admission adapter failed")
	}
	c, err := wecom.NewClient(wecom.Config{BotID: a.BotID, Secret: credential.Secret, URL: f.url, MaxReconnects: 3})
	if err != nil {
		return nil, errors.New("invalid WeCom protocol configuration")
	}
	return managed.NewClient(c, h.Handle, managed.ErrorPolicy{Temporary: inbound.IsRetryable, Rejected: inbound.IsRejected}, slog.Default().With("account_id", a.ID))
}
