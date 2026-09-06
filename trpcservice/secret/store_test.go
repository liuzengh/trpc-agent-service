package secret

import (
	"context"
	"errors"
	"testing"
)

func TestEnvAndStaticStore(t *testing.T) {
	t.Setenv("SECRET_STORE_TEST", "value")
	store, err := NewEnvStore([]Grant{{TenantID: "tenant-a", Purpose: Model, Reference: "env://SECRET_STORE_TEST"}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := store.Resolve(context.Background(), "tenant-a", Model, "env://SECRET_STORE_TEST")
	if err != nil || value != "value" {
		t.Fatalf("env value=%q err=%v", value, err)
	}
	static := StaticStore{"secret://one": "static"}
	value, err = static.Resolve(context.Background(), "tenant-a", Model, "secret://one")
	if err != nil || value != "static" {
		t.Fatalf("static value=%q err=%v", value, err)
	}
	if _, err := static.Resolve(context.Background(), "tenant-a", Model, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing error=%v", err)
	}
}

func TestEnvStoreDeniesCrossTenantPurposeAndReference(t *testing.T) {
	t.Setenv("SECRET_STORE_TEST", "private-canary")
	grant := Grant{TenantID: "tenant-a", Purpose: Model, Reference: "env://SECRET_STORE_TEST"}
	store, _ := NewEnvStore([]Grant{grant})
	for _, input := range []Grant{
		{TenantID: "tenant-b", Purpose: Model, Reference: grant.Reference},
		{TenantID: grant.TenantID, Purpose: Memory, Reference: grant.Reference},
		{TenantID: grant.TenantID, Purpose: Model, Reference: "env://OPENAI_API_KEY"},
		{TenantID: grant.TenantID, Purpose: Model, Reference: "env:// SECRET_STORE_TEST"},
		{Purpose: Model, Reference: grant.Reference},
	} {
		value, err := store.Resolve(context.Background(), input.TenantID, input.Purpose, input.Reference)
		if value != "" || !errors.Is(err, ErrForbidden) {
			t.Fatalf("denial = %q, %v", value, err)
		}
	}
	if _, err := (EnvStore{}).Resolve(context.Background(), grant.TenantID, Model, grant.Reference); !errors.Is(err, ErrForbidden) {
		t.Fatalf("zero store: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Resolve(ctx, grant.TenantID, Model, grant.Reference); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// Grant validation does not read values; resolution distinguishes allowed-but-missing.
	t.Setenv("SECRET_STORE_TEST", "")
	if err := store.Authorize(context.Background(), grant.TenantID, Model, grant.Reference); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(context.Background(), grant.TenantID, Model, grant.Reference); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestEnvStoreRejectsAmbiguousGrants(t *testing.T) {
	valid := Grant{TenantID: "tenant-a", Purpose: Model, Reference: "env://MODEL_KEY"}
	for _, grants := range [][]Grant{
		{valid, valid},
		{{TenantID: "*", Purpose: Model, Reference: valid.Reference}},
		{{TenantID: valid.TenantID, Purpose: "*", Reference: valid.Reference}},
		{{TenantID: valid.TenantID, Purpose: Model, Reference: "env://MODEL_*"}},
	} {
		if _, err := NewEnvStore(grants); err == nil {
			t.Fatal("invalid grants accepted")
		}
	}
}
