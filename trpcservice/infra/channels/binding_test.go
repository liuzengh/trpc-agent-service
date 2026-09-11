package channels

import (
	"context"
	"errors"
	"testing"
)

func TestMemBindingStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	s := NewMemBindingStore()

	// create
	if err := s.Create(ctx, ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "a1",
		Channel: ChannelWeCom, AccountID: "wx-1", CredentialRef: "sec://wecom-1",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// duplicate channel+account rejected
	if err := s.Create(ctx, ChannelBinding{
		BindingID: "b2", TenantID: "t1", AgentID: "a2",
		Channel: ChannelWeCom, AccountID: "wx-1",
	}); !errors.Is(err, ErrBindingDuplicate) {
		t.Errorf("dup create err = %v, want ErrBindingDuplicate", err)
	}
	// another channel/account allowed
	if err := s.Create(ctx, ChannelBinding{
		BindingID: "b3", TenantID: "t1", AgentID: "a3",
		Channel: ChannelFeishu, AccountID: "fs-1",
	}); err != nil {
		t.Fatalf("create feishu: %v", err)
	}

	// get
	got, err := s.Get(ctx, "b1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.TenantID != "t1" || got.CredentialRef != "sec://wecom-1" {
		t.Errorf("get = %+v", got)
	}
	if _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrBindingNotFound) {
		t.Errorf("get missing err = %v, want not found", err)
	}

	// list filters
	all, _ := s.List(ctx, "", "")
	if len(all) != 2 {
		t.Errorf("list all = %d, want 2", len(all))
	}
	byTenant, _ := s.List(ctx, "t1", "")
	if len(byTenant) != 2 {
		t.Errorf("list t1 = %d, want 2", len(byTenant))
	}
	byChannel, _ := s.List(ctx, "", ChannelFeishu)
	if len(byChannel) != 1 || byChannel[0].BindingID != "b3" {
		t.Errorf("list feishu = %+v", byChannel)
	}
	otherTenant, _ := s.List(ctx, "t-other", "")
	if len(otherTenant) != 0 {
		t.Errorf("list other tenant = %+v, want empty", otherTenant)
	}

	// delete removes; the account becomes rebindable
	if err := s.Delete(ctx, "b1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Get(ctx, "b1"); !errors.Is(err, ErrBindingNotFound) {
		t.Errorf("get after delete err = %v", err)
	}
	if err := s.Create(ctx, ChannelBinding{
		BindingID: "b4", TenantID: "t1", AgentID: "a4",
		Channel: ChannelWeCom, AccountID: "wx-1",
	}); err != nil {
		t.Errorf("re-binding same account after delete: %v", err)
	}
	if err := s.Delete(ctx, "b1"); !errors.Is(err, ErrBindingNotFound) {
		t.Errorf("double delete err = %v, want not found", err)
	}
}
