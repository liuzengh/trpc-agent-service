package migrations_test

import (
	"crypto/sha256"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
	"testing"
)

// These migrations have already been published independently. Full filenames,
// not numeric prefixes, identify immutable ledger entries; do not renumber them.
func TestPublishedTracingAndAuthorizationMigrationsCoexist(t *testing.T) {
	for name, want := range map[string]string{
		"0005_authorization_attempt_gate.sql":      "ac051fed35d640c998718a1f4a51139179c7779e0d49b1dc8a8275167a2aea8b",
		"0005_run_trace.sql":                       "7b1bc5d2ea889ef94412cfab7d989379f90266c8483d4b00bd6e887880cf727f",
		"0006_current_authorization_snapshots.sql": "187415b1f9ac1c260ef6ef13b63d0250748151073470bd8cc5ffbc91833fb647",
		"0006_reply_trace.sql":                     "0dfde95dd4e194e93c36bb4d49b058386bb935824b60d75b21f530a19c47ba23",
	} {
		body, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(body)); got != want {
			t.Fatalf("published migration %s changed: %s", name, got)
		}
	}
}
