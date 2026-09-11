package migrations_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

// Published sibling migrations use full filenames as ledger identities. Keep
// both immutable when merging receiver ownership with Worker Reply transport.
func TestReceiveAndReplyPublishedMigrationsCoexist(t *testing.T) {
	for name, want := range map[string]string{
		"0011_telegram_receive_modes.sql":   "ec151afacd7b1d0663dc95c09291266b22e78c531e6f158b99767261d151d9e5",
		"0011_reply_transport_receipts.sql": "2d389a4e441de1056ccfcae6ddf280f0940bbd0ed3906cd88df4e05ff99befd4",
	} {
		raw, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != want {
			t.Fatalf("published migration %s changed: %s", name, got)
		}
	}
}
