package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

const testEpoch = "00000000-0000-4000-8000-000000000001"

func TestSnapshotDigestNormalizationAndIsolation(t *testing.T) {
	a := fixtureAccount(t)
	entry, err := ProjectSnapshotAccount(a, fixtureMetas())
	if err != nil {
		t.Fatal(err)
	}
	entry.Credentials[0], entry.Credentials[1] = entry.Credentials[1], entry.Credentials[0]
	snapshot, err := NewSnapshot("gateway_pool", testEpoch, 1, []SnapshotAccount{entry})
	if err != nil {
		t.Fatal(err)
	}
	if entry.Credentials[0].Purpose != TelegramWebhookSecret {
		t.Fatal("constructor mutated input")
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeSnapshot(raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "value") || strings.Contains(string(raw), "ciphertext") || strings.Contains(string(raw), "测试机器人") {
		t.Fatal("snapshot contains non-wire fields")
	}
	before := snapshot.Digest
	snapshot.Revision++
	d, _ := snapshotDigest(snapshot)
	if before == d {
		t.Fatal("catalog revision not in digest")
	}
	_, err = DecodeSnapshot([]byte(strings.Replace(string(raw), `"connection_revision":1`, `"connection_revision":2`, 1)))
	assertCode(t, err, SourceIntegrity)
	snapshot.Accounts[0].Config.WebhookPath = "/other"
	_, err = NewSnapshot(snapshot.ScopeID, testEpoch, 2, snapshot.Accounts)
	assertCode(t, err, SourceIntegrity)
}
func TestSnapshotRejectsDuplicateIDsCredentialIDsAndMissingRequirements(t *testing.T) {
	a := fixtureAccount(t)
	entry, _ := ProjectSnapshotAccount(a, fixtureMetas())
	_, err := NewSnapshot("gateway_pool", testEpoch, 1, []SnapshotAccount{entry, entry})
	assertCode(t, err, SourceIntegrity)
	second := entry
	second.AccountID = "cha_b"
	second.Config.WebhookPath = "/v1/telegram/cha_b"
	second.ProviderAccountID = "456"
	second.TenantID = "tnt_b"
	_, err = NewSnapshot("gateway_pool", testEpoch, 1, []SnapshotAccount{entry, second})
	assertCode(t, err, SourceIntegrity)
	second.Credentials = []CredentialMeta{{TelegramBotToken, "ccr_b1", 1, true}, {TelegramWebhookSecret, "ccr_b2", 1, true}}
	if _, err = NewSnapshot("gateway_pool", testEpoch, 1, []SnapshotAccount{entry, second}); err != nil {
		t.Fatal(err)
	}
	second.Enabled = true
	second.Credentials[0].Configured = false
	_, err = NewSnapshot("gateway_pool", testEpoch, 1, []SnapshotAccount{second})
	assertCode(t, err, SourceIntegrity)
	empty, err := NewSnapshot("gateway_pool", testEpoch, 1, nil)
	if err != nil || empty.Accounts == nil {
		t.Fatal("empty complete snapshot must use []", err)
	}
}
