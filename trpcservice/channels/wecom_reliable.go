package channels

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// WeComSender delivers one committed reply through the 企业微信 app-message
// API in reliable mode. Unlike the legacy adapter it has no streaming
// buffer: each reply_outbox row is one complete part with a Done flag, and
// the delivery role owns the retry policy, so this sender sends exactly what
// it is handed and classifies what came back.
type WeComSender struct {
	delegate *WeCom
}

// NewWeComSender builds the sender over a control-plane binding lookup.
// It reuses the WeCom adapter's token cache and sendText plumbing, so the
// legacy and reliable flows cannot drift on how a credential is fetched or a
// message is posted.
func NewWeComSender(lookup func(tenantID string) (*tenant.WeComBinding, bool)) *WeComSender {
	return &WeComSender{delegate: &WeCom{
		lookup:  lookup,
		apiBase: wecomDefaultAPIBase,
		client:  &http.Client{Timeout: 10 * time.Second},
		tokens:  make(map[string]*wecomToken),
	}}
}

// WithAPIBase overrides the WeCom API host (fake upstreams, private
// deployments), mirroring KfPuller.WithAPIBase.
func (s *WeComSender) WithAPIBase(base string) *WeComSender {
	s.delegate.apiBase = base
	return s
}

// Send implements outbox.Sender. Classification matches KfPuller.Send: an
// answered error is Rejected (safe to retry or dead-letter), an unanswered
// one is Unknown (never auto-retried, because the message may have arrived).
func (s *WeComSender) Send(ctx context.Context, reply *outbox.ClaimedReply) (outbox.Outcome, error) {
	b, ok := s.delegate.lookup(reply.TenantID)
	if !ok {
		return outbox.Rejected, errors.New("wecom: tenant has no binding")
	}
	token, err := s.delegate.accessToken(ctx, b)
	if err != nil {
		return classifyWeComError(err)
	}
	for _, part := range splitUTF8(reply.Text, wecomMaxTextBytes) {
		if err := s.delegate.sendText(ctx, b, token, reply.Target, part); err != nil {
			return classifyWeComError(err)
		}
	}
	return outbox.Sent, nil
}

// classifyWeComError maps a send failure onto the outbox vocabulary: an
// answer from the platform is Rejected; anything else (a dial timeout, a
// dropped connection) means we do not know whether it arrived.
func classifyWeComError(err error) (outbox.Outcome, error) {
	var apiErr *wecomAPIError
	if errors.As(err, &apiErr) {
		return outbox.Rejected, err
	}
	return outbox.Unknown, err
}
