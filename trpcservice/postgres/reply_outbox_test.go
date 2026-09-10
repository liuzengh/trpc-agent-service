package postgres

import (
	"strings"
	"testing"
)

func TestSanitizeReplySnapshotCatchesCredentialAcrossStreamDeltas(t *testing.T) {
	snapshot := sanitizeReplySnapshot("prefix sk-" + "test_1234567890123456 suffix")
	if strings.Contains(snapshot, "test_1234567890123456") || !strings.Contains(snapshot, "[REDACTED]") {
		t.Fatalf("snapshot = %q", snapshot)
	}
}
