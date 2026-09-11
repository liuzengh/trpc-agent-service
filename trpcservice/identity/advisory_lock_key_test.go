package identity

import (
	"strings"
	"testing"
)

func TestAdvisoryLockKeyIsPostgresTextSafeAndUnambiguous(t *testing.T) {
	key := advisoryLockKey("corp-a", "user:1")
	if strings.ContainsRune(key, '\x00') {
		t.Fatalf("advisory lock key contains NUL: %q", key)
	}

	if advisoryLockKey("ab", "c") == advisoryLockKey("a", "bc") {
		t.Fatal("advisory lock tuple encoding is ambiguous")
	}
	if advisoryLockKey("a:", "b") == advisoryLockKey("a", ":b") {
		t.Fatal("advisory lock tuple encoding collides on delimiter-like input")
	}
}
