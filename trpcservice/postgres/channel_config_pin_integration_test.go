//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestChannelAttachmentUsesPinnedCanaryAfterRolloutChanges(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	stable, err := p.store.ResolveAppConfig(p.ctx, p.scope.TenantID, p.scope.AppID, "v1")
	if err != nil {
		t.Fatalf("resolve stable config: %v", err)
	}
	canary := stable
	canary.Version = "v2"
	canary.Model.Model = "pinned-canary-model"
	if err := p.store.InsertAppConfigVersion(p.ctx, canary); err != nil {
		t.Fatalf("insert canary config: %v", err)
	}
	if _, err := p.store.EnableAppCanary(p.ctx, p.scope.TenantID, p.scope.AppID, canary.Version, 100); err != nil {
		t.Fatalf("enable app canary: %v", err)
	}

	request := newIM05Request(t, p.route, p.binding, "message-pinned-canary", "request-pinned-canary", channels.MessageTypeText, "pin this")
	gotPinnedVersion := ""
	result, err := gateway.New(p.store).HandleChannel(
		p.ctx,
		gateway.Request{
			RequestID:      request.RequestID,
			IdempotencyKey: request.IdempotencyKey,
			Tenant:         testAdmissionIdentityResolver{identity: request.Identity},
			Message:        request.Message,
			ChannelInput:   request.ChannelInput,
		},
		func(ctx context.Context, input channels.ChannelInput, version string) (channels.ChannelInput, func(context.Context) error, error) {
			gotPinnedVersion = version
			if version != canary.Version {
				t.Fatalf("pinned version = %q, want %q", version, canary.Version)
			}
			if _, err := p.store.PauseAppCanary(ctx, p.scope.TenantID, p.scope.AppID); err != nil {
				return channels.ChannelInput{}, nil, err
			}
			return input, func(context.Context) error { return nil }, nil
		},
	)
	if err != nil {
		t.Fatalf("admit pinned channel request: %v", err)
	}
	if gotPinnedVersion != canary.Version || result.ConfigVersion != canary.Version {
		t.Fatalf("pinned channel result = callback:%q result:%q, want %q", gotPinnedVersion, result.ConfigVersion, canary.Version)
	}

	var storedVersion string
	if err := p.pool.QueryRow(p.ctx, `
SELECT config_version FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		p.scope.TenantID, p.scope.AppID, result.RequestID).Scan(&storedVersion); err != nil {
		t.Fatalf("read pinned execution version: %v", err)
	}
	if storedVersion != canary.Version {
		t.Fatalf("stored execution version = %q, want %q", storedVersion, canary.Version)
	}
}

type testAdmissionIdentityResolver struct {
	identity gateway.AdmissionIdentity
}

func (r testAdmissionIdentityResolver) ResolveTenant(context.Context) (tenant.RuntimeContext, gateway.TenantSource, error) {
	return r.identity.Tenant, r.identity.Source, nil
}

func (r testAdmissionIdentityResolver) ResolveAdmissionIdentity(context.Context) (gateway.AdmissionIdentity, error) {
	return r.identity, nil
}
