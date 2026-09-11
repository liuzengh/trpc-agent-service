//go:build integration

package bindingstore

import (
	"context"
	"errors"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
)

func TestMySQLBindingStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"),
		mysql.WithScripts(
			"../../../../deployments/mysql/init/006_knowledge.sql",
			"../../../../deployments/mysql/init/005_skills.sql",
			"../../../../deployments/mysql/init/002_model_endpoints.sql",
			"../../../../deployments/mysql/init/009_audit_usage_artifacts.sql",
			"../../../../deployments/mysql/init/008_channels_outbox.sql"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s := NewMySQLBindingStore(db)

	// create + duplicate guard + get
	if err := s.Create(ctx, channels.ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "a1",
		Channel: channels.ChannelWeCom, AccountID: "wx-1", CredentialRef: "sec://wx-1",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.Create(ctx, channels.ChannelBinding{
		BindingID: "b2", TenantID: "t1", AgentID: "a2",
		Channel: channels.ChannelWeCom, AccountID: "wx-1",
	}); !errors.Is(err, channels.ErrBindingDuplicate) {
		t.Errorf("dup err = %v, want channels.ErrBindingDuplicate", err)
	}
	if err := s.Create(ctx, channels.ChannelBinding{
		BindingID: "b3", TenantID: "t1", AgentID: "a3",
		Channel: channels.ChannelFeishu, AccountID: "fs-1",
	}); err != nil {
		t.Fatalf("create feishu: %v", err)
	}

	got, err := s.Get(ctx, "b1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.CredentialRef != "sec://wx-1" || got.Channel != channels.ChannelWeCom {
		t.Errorf("get = %+v", got)
	}

	// list with filters
	all, _ := s.List(ctx, "", "")
	if len(all) != 2 {
		t.Errorf("list all = %d, want 2", len(all))
	}
	wecom, _ := s.List(ctx, "", channels.ChannelWeCom)
	if len(wecom) != 1 || wecom[0].BindingID != "b1" {
		t.Errorf("wecom list = %+v", wecom)
	}
	other, _ := s.List(ctx, "t-other", "")
	if len(other) != 0 {
		t.Errorf("other tenant list = %+v", other)
	}

	// delete makes the account rebindable (physical delete)
	if err := s.Delete(ctx, "b1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Get(ctx, "b1"); !errors.Is(err, channels.ErrBindingNotFound) {
		t.Errorf("get after delete err = %v", err)
	}
	if err := s.Create(ctx, channels.ChannelBinding{
		BindingID: "b4", TenantID: "t1", AgentID: "a4",
		Channel: channels.ChannelWeCom, AccountID: "wx-1",
	}); err != nil {
		t.Errorf("rebind after delete: %v", err)
	}
	if err := s.Delete(ctx, "b-missing"); !errors.Is(err, channels.ErrBindingNotFound) {
		t.Errorf("delete missing err = %v", err)
	}
}
