package tenant

import (
	"context"
	"testing"
)

func TestContextPropagation(t *testing.T) {
	ctx := WithTenantID(context.Background(), "t1")
	got, ok := TenantIDFromContext(ctx)
	if !ok {
		t.Fatal("expected tenant id in context")
	}
	if got != "t1" {
		t.Errorf("tenant id = %q, want %q", got, "t1")
	}
}

func TestContextMissing(t *testing.T) {
	if _, ok := TenantIDFromContext(context.Background()); ok {
		t.Error("expected no tenant id in empty context")
	}
}
