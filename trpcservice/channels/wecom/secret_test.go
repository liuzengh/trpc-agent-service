package wecom

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestWeComTokenCacheIsTenantScopedAndInvalidatesCorrectEntry(t *testing.T) {
	adapter, _ := New(secret.EnvStore{}, nil)
	binding := controlplane.ChannelBinding{TenantID: "tenant-a", ID: "same-id", Version: 1}
	adapter.tokens["tenant-a:same-id:1"] = tokenEntry{value: "cached-a", expiresAt: time.Now().Add(time.Hour)}
	other := binding
	other.TenantID = "tenant-b"
	if _, err := adapter.accessToken(context.Background(), other, bindingConfig{AppSecretRef: "env://KEY"}); !errors.Is(err, secret.ErrForbidden) {
		t.Fatalf("cross tenant cache: %v", err)
	}
	adapter.tokens["tenant-b:same-id:1"] = tokenEntry{value: "cached-b", expiresAt: time.Now().Add(time.Hour)}
	adapter.invalidateToken(binding)
	if adapter.tokens["tenant-a:same-id:1"].value != "" || adapter.tokens["tenant-b:same-id:1"].value != "cached-b" {
		t.Fatal("incorrect token cache invalidation")
	}
}
