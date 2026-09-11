package storage

import (
	"strings"
	"testing"
)

func TestSessionRouteAdvisoryLockKeyIsPostgresTextSafeAndRouteSpecific(t *testing.T) {
	t.Parallel()
	route := SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "wecom",
		BindingID: "wecom-main", ConversationID: "user-1",
	}
	key := sessionRouteAdvisoryLockKey(route)
	if strings.ContainsRune(key, '\x00') {
		t.Fatalf("lock key contains PostgreSQL-invalid NUL byte: %q", key)
	}
	if len(key) != 64 {
		t.Fatalf("lock key length = %d, want SHA-256 hex length 64", len(key))
	}

	other := route
	other.ConversationID = "user-2"
	if otherKey := sessionRouteAdvisoryLockKey(other); otherKey == key {
		t.Fatalf("different routes shared advisory lock key %q", key)
	}

	withNUL := route
	withNUL.ConversationID = "user\x00provider"
	if unsafeKey := sessionRouteAdvisoryLockKey(withNUL); strings.ContainsRune(unsafeKey, '\x00') {
		t.Fatalf("lock key from NUL-containing input is unsafe: %q", unsafeKey)
	}
}
