package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type p104ProbeResolver struct {
	context tenant.TenantContext
	err     error
	seen    tenant.ResolveRequest
}

func (r *p104ProbeResolver) Resolve(_ context.Context, request tenant.ResolveRequest) (tenant.TenantContext, error) {
	r.seen = request
	return r.context, r.err
}

type p104ProbeIdentity struct{ err error }

func (r p104ProbeIdentity) ResolveIdentity(_ context.Context, _ tenant.TenantContext) (tenant.IdentityResolution, error) {
	return tenant.IdentityResolution{}, r.err
}

type p104ProbeClaims struct{ calls atomic.Int32 }

func (c *p104ProbeClaims) Claim(_ context.Context, _ tenant.TenantContext, key storage.DedupKey, _ time.Duration, owner string) (storage.Claim, error) {
	c.calls.Add(1)
	return storage.Claim{Key: key, Status: storage.ClaimAcquired, OwnerID: owner, Backend: storage.BackendPostgres, FenceToken: 1}, nil
}
func (*p104ProbeClaims) Complete(context.Context, tenant.TenantContext, storage.DedupKey, string, string, storage.OperationGuard) error {
	return nil
}
func (*p104ProbeClaims) Fail(context.Context, tenant.TenantContext, storage.DedupKey, string, storage.OperationGuard, bool) error {
	return nil
}

func TestP104IngressRejectsBeforeClaim(t *testing.T) {
	valid := tenant.TenantContext{TenantID: "tenant-p104-ingress", AgentAppID: "agent-p104-ingress", BindingID: "binding-p104-ingress", Channel: tenant.ChannelTelegram, ExternalUser: "user-ingress", ExternalChat: "chat-ingress", RequestID: "request-ingress", MessageID: "message-ingress", TraceID: "trace-ingress", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"}}
	cases := []struct {
		name        string
		incoming    channels.Incoming
		resolveErr  error
		identityErr error
		status      int
	}{
		{name: "caller tenant override", incoming: channels.Incoming{ID: "message-tenant", TenantID: "caller-tenant", UserID: "user-ingress", ChatID: "chat-ingress", Text: "hello"}, status: http.StatusBadRequest},
		{name: "caller channel override", incoming: channels.Incoming{ID: "message-channel", Channel: "lark", UserID: "user-ingress", ChatID: "chat-ingress", Text: "hello"}, status: http.StatusBadRequest},
		{name: "unknown binding", resolveErr: tenant.ErrBindingNotFound, status: http.StatusUnauthorized},
		{name: "disabled binding", resolveErr: tenant.ErrBindingNotFound, status: http.StatusUnauthorized},
		{name: "expired binding", resolveErr: tenant.ErrBindingNotFound, status: http.StatusUnauthorized},
		{name: "secret resolution failure", resolveErr: tenant.ErrBindingNotFound, status: http.StatusUnauthorized},
		{name: "cross channel identity", identityErr: tenant.ErrIdentityMismatch, status: http.StatusUnauthorized},
		{name: "cross binding identity", identityErr: tenant.ErrIdentityMismatch, status: http.StatusUnauthorized},
		{name: "chat thread conflict", identityErr: tenant.ErrIdentityConflict, status: http.StatusUnauthorized},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			claims := &p104ProbeClaims{}
			resolver := &p104ProbeResolver{context: valid, err: test.resolveErr}
			incoming := test.incoming
			if incoming.ID == "" {
				incoming = channels.Incoming{ID: "message-default", UserID: "user-ingress", ChatID: "chat-ingress", Text: "hello"}
			}
			gateway, err := New(queue.NewFakeQueue(queue.FakeQueueConfig{}))
			if err != nil {
				t.Fatal(err)
			}
			ingress, err := NewIngress(IngressConfig{Resolver: resolver, Identity: p104ProbeIdentity{err: test.identityErr}, Claims: claims, Gateway: gateway, ResolveAgent: func(context.Context, tenant.TenantContext) (agent.AgentSpec, error) {
				return agent.AgentSpec{TenantID: valid.TenantID, AgentAppID: valid.AgentAppID, Version: valid.ConfigVersion, Name: "assistant", ModelProvider: "fake"}, nil
			}, Adapters: map[string]WebhookAdapter{"telegram": ingressTestAdapter{incoming: incoming}}, OwnerID: "owner-p104"})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/webhook/telegram/app", strings.NewReader("{}"))
			result := ingress.Handle(context.Background(), "telegram", "app", request, []byte("{}"))
			if result.Status != test.status {
				t.Fatalf("status=%d want=%d", result.Status, test.status)
			}
			if claims.calls.Load() != 0 {
				t.Fatalf("Claim called %d times before rejection", claims.calls.Load())
			}
			if resolver.seen.TenantID != "" || resolver.seen.BindingID != "" {
				t.Fatalf("caller override reached resolver: %+v", resolver.seen)
			}
		})
	}
}

type p104ProbeIdentityValue struct{}

func (p104ProbeIdentityValue) ResolveIdentity(_ context.Context, tc tenant.TenantContext) (tenant.IdentityResolution, error) {
	return tenant.IdentityResolution{Identity: tenant.Identity{TenantID: tc.TenantID, ID: "identity-p104", Channel: tc.Channel, BindingID: tc.BindingID, ExternalUserID: tc.ExternalUser, InternalUserID: "internal-p104", Status: tenant.IdentityStatusActive, Scope: "group:" + tc.ExternalChat, ExternalChat: tc.ExternalChat, Version: 1}, Scope: "group:" + tc.ExternalChat}, nil
}

type p104ProbeAudit struct{ calls atomic.Int32 }

func (a *p104ProbeAudit) AppendBindingEvent(context.Context, tenant.TenantContext, storage.BindingAuditEvent) error {
	a.calls.Add(1)
	return errors.New("audit unavailable")
}

func TestP104IngressAuditFailureDoesNotBypassOrBlockRequest(t *testing.T) {
	valid := tenant.TenantContext{TenantID: "tenant-p104-audit", AgentAppID: "agent-p104-audit", BindingID: "binding-p104-audit", Channel: tenant.ChannelTelegram, ExternalUser: "user-audit", ExternalChat: "chat-audit", RequestID: "request-audit", MessageID: "message-audit", TraceID: "trace-audit", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"}}
	claims := &p104ProbeClaims{}
	audit := &p104ProbeAudit{}
	gateway, err := New(queue.NewFakeQueue(queue.FakeQueueConfig{}))
	if err != nil {
		t.Fatal(err)
	}
	resolver := &p104ProbeResolver{context: valid}
	ingress, err := NewIngress(IngressConfig{Resolver: resolver, Identity: p104ProbeIdentityValue{}, Audit: audit, Claims: claims, Gateway: gateway, ResolveAgent: func(context.Context, tenant.TenantContext) (agent.AgentSpec, error) {
		return agent.AgentSpec{TenantID: valid.TenantID, AgentAppID: valid.AgentAppID, Version: valid.ConfigVersion, Name: "assistant", ModelProvider: "fake"}, nil
	}, Adapters: map[string]WebhookAdapter{"telegram": ingressTestAdapter{incoming: channels.Incoming{ID: "message-audit", UserID: valid.ExternalUser, ChatID: valid.ExternalChat, Text: "hello"}}}, OwnerID: "owner-audit"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/webhook/telegram/app", strings.NewReader("{}"))
	result := ingress.Handle(context.Background(), "telegram", "app", request, []byte("{}"))
	if result.Status != http.StatusAccepted || claims.calls.Load() != 1 || audit.calls.Load() != 1 {
		t.Fatalf("audit failure result=%d claims=%d audit=%d", result.Status, claims.calls.Load(), audit.calls.Load())
	}
}
