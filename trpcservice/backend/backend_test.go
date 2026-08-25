package backend

import "testing"

func TestNamespaceAndObjectKeySanitization(t *testing.T) {
	if got := sanitize(" Tenant/A "); got != "tenant_a" {
		t.Fatalf("namespace=%q", got)
	}
	key, err := objectKey("tenant", "smoke/item.txt")
	if err != nil || key != "tenant/smoke/item.txt" {
		t.Fatalf("key=%q err=%v", key, err)
	}
	if _, err := objectKey("tenant", ""); err == nil {
		t.Fatal("empty key must fail")
	}
}
