package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func p104StorageContext(tenantID, channel string) tenant.TenantContext {
	thread := "77"
	chatType := "supergroup"
	if channel == tenant.ChannelLark {
		thread = ""
		chatType = ""
	}
	return tenant.TenantContext{TenantID: tenantID, AgentAppID: "agent-" + tenantID, BindingID: "binding-" + channel, Channel: channel, ExternalUser: "external-user", ExternalChat: "-100", ExternalChatType: chatType, ExternalThreadID: thread, SessionID: "session-" + tenantID, RequestID: "request-" + tenantID, MessageID: "message-" + tenantID, TraceID: "trace-" + tenantID, ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"}}
}

func TestFakeMetadataRepositoryTenantIsolationAndCAS(t *testing.T) {
	repo := NewFakeMetadataRepository()
	ctx := context.Background()
	one := p104StorageContext("tenant-one", tenant.ChannelTelegram)
	two := p104StorageContext("tenant-two", tenant.ChannelTelegram)
	binding := tenant.ChannelBinding{TenantID: one.TenantID, ID: one.BindingID, Channel: one.Channel, ExternalAppID: "bot-one", SecretRef: "env://P104_FAKE_SECRET", Enabled: true}.Canonical()
	if err := repo.PutBinding(binding); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutBinding(binding); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate binding error=%v", err)
	}
	other := binding
	other.TenantID = two.TenantID
	other.ID = two.BindingID
	if err := repo.PutBinding(other); !errors.Is(err, tenant.ErrBindingConflict) {
		t.Fatalf("cross-tenant provider conflict=%v", err)
	}
	if _, err := repo.GetBinding(ctx, two, binding.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant read error=%v", err)
	}
	updated := binding
	updated.Version = 2
	updated.Status = tenant.BindingStatusDisabled
	updated.Enabled = false
	if err := repo.UpdateBinding(ctx, one, updated, 1); err != nil {
		t.Fatal(err)
	}
	updated.Version = 2
	if err := repo.UpdateBinding(ctx, one, updated, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS error=%v", err)
	}
}

func TestFakeMetadataRepositoryIdentityAndAuditAreBounded(t *testing.T) {
	repo := NewFakeMetadataRepository()
	ctx := context.Background()
	tc := p104StorageContext("tenant-audit", tenant.ChannelLark)
	identity, err := repo.ResolveIdentity(ctx, tc)
	if err != nil || identity.TenantID != tc.TenantID || identity.ExternalUserID != tc.ExternalUser || identity.InternalUserID == tc.ExternalUser {
		t.Fatalf("identity=%+v err=%v", identity, err)
	}
	event := BindingAuditEvent{TenantID: tc.TenantID, AuditID: "audit-1", BindingID: tc.BindingID, Channel: tc.Channel, Operation: "identity_resolve", Success: true, IdentityFingerprint: identity.ID, SecretFingerprint: tenant.SecretRefFingerprint("env://P104_FAKE_SECRET"), Version: 1, CreatedAt: time.Now()}
	if err := repo.AppendBindingEvent(ctx, tc, event); err != nil {
		t.Fatal(err)
	}
	if got := repo.AuditEvents(); len(got) != 1 || got[0].SecretFingerprint == "env://P104_FAKE_SECRET" {
		t.Fatalf("unexpected audit events=%+v", got)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repo.ResolveIdentity(cancelled, tc); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled identity error=%v", err)
	}
}
