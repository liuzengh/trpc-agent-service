package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

var testTime = time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)

func fixtureAccount(t *testing.T) Account {
	t.Helper()
	a, err := NewAccount("tnt_a", "cha_a", "gateway_pool", "usr_owner", Telegram, "000123", "  测试机器人  ", "", testTime)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func fixtureMetas() []CredentialMeta {
	return []CredentialMeta{{TelegramBotToken, "ccr_token", 1, true}, {TelegramWebhookSecret, "ccr_webhook", 1, true}}
}
func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}
func str(v string) *string { return &v }
func TestAccountIdentityAndVersionMatrix(t *testing.T) {
	a := fixtureAccount(t)
	if a.Enabled || a.Revision != 1 || a.ConnectionRevision != 1 || a.MinRouteGeneration != 0 || a.ProviderAccountID != "123" || a.Name != "测试机器人" || a.Config.WebhookPath != "/v1/telegram/cha_a" {
		t.Fatalf("bad initial state: %+v", a)
	}
	next, changed, err := a.ChangeMetadata(1, str("new"), nil, testTime.Add(time.Second))
	if err != nil || !changed || next.Revision != 2 || next.ConnectionRevision != 1 || a.Revision != 1 {
		t.Fatalf("metadata: %+v %v %v", next, changed, err)
	}
	_, _, err = next.ChangeMetadata(1, str("new"), nil, testTime)
	assertCode(t, err, RevisionConflict)
	same, changed, err := next.ChangeMetadata(2, str(" new "), nil, testTime)
	if err != nil || changed || same != next {
		t.Fatal("current semantic noop must preserve versions")
	}
	enabled, changed, err := next.SetEnabled(2, true, fixtureMetas(), testTime)
	if err != nil || !changed || !enabled.Enabled || enabled.Revision != 3 || enabled.ConnectionRevision != 2 {
		t.Fatal("enable versions", err)
	}
	cleared := fixtureMetas()
	cleared[0].Configured = false
	_, _, err = next.SetEnabled(2, true, cleared, testTime)
	assertCode(t, err, CredentialRequired)
	enabled.Revision = MaxVersion
	_, _, err = enabled.SetEnabled(MaxVersion, false, fixtureMetas(), testTime)
	assertCode(t, err, VersionExhausted)
}
func TestPhysicalIDAndMetadataBoundaries(t *testing.T) {
	for _, v := range []string{"", "0", "000", "-1", "12 3", "１２３", "123\n", strings.Repeat("1", 1025)} {
		if _, err := NormalizeProviderAccountID(Telegram, v); err == nil {
			t.Errorf("accepted invalid physical ID case len=%d", len(v))
		}
	}
	for _, v := range []string{"bot-id", "机器人"} {
		if got, err := NormalizeProviderAccountID(WeCom, v); err != nil || got != v {
			t.Errorf("wecom identifier: %v", err)
		}
	}
	if _, err := NewAccount("t", "a", "s", "u", Telegram, "1", strings.Repeat("中", 129), "", testTime); err == nil {
		t.Fatal("name limit")
	}
	a := fixtureAccount(t)
	a.Config.BotID = "123"
	assertCode(t, a.Validate(), SourceIntegrity)
}
func TestWebhookSecretAndEditBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{{"empty", "", false}, {"one", "a", true}, {"256", strings.Repeat("a", 256), true}, {"257", strings.Repeat("a", 257), false}, {"symbols", "a_B-1", true}, {"unicode", "秘密", false}, {"space", "a b", false}, {"newline", "abc\n", false}, {"slash", "a/b", false}} {
		t.Run(tc.name, func(t *testing.T) {
			err := (CredentialEdit{"replace", str(tc.value)}).Validate(Telegram, TelegramWebhookSecret)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v got=%v", tc.valid, err)
			}
		})
	}
	for _, e := range []CredentialEdit{{"keep", str("masked")}, {"clear", str("")}, {"replace", nil}, {"other", nil}} {
		assertCode(t, e.Validate(Telegram, TelegramBotToken), InputInvalid)
	}
	if err := (CredentialEdit{"keep", nil}).Validate(Telegram, TelegramBotToken); err != nil {
		t.Fatal(err)
	}
	assertCode(t, (CredentialEdit{"replace", str(strings.Repeat("中", 6000))}).Validate(Telegram, TelegramBotToken), InputInvalid)
}
func TestCredentialVersioningAndNoSecretProjection(t *testing.T) {
	a := fixtureAccount(t)
	r := CredentialRecord{TenantID: a.TenantID, AccountID: a.ID, Provider: a.Provider, Meta: fixtureMetas()[0], KeyID: "k1", Ciphertext: []byte("ciphertext-fixture")}
	next, updated, changed, err := PlanCredentialUpdate(a, r, 1, 1, CredentialEdit{"replace", str("fixture-value")}, testTime)
	if err != nil || !changed || next.Revision != 2 || next.ConnectionRevision != 2 || updated.Meta.Version != 2 || len(updated.Ciphertext) != 0 {
		t.Fatal("replace must allocate new AAD/version", err)
	}
	if r.Meta.Version != 1 || a.Revision != 1 {
		t.Fatal("mutated original")
	}
	_, _, _, err = PlanCredentialUpdate(next, updated, 2, 1, CredentialEdit{"keep", nil}, testTime)
	assertCode(t, err, CredentialVersionConflict)
	next.Enabled = true
	_, _, _, err = PlanCredentialUpdate(next, updated, 2, 2, CredentialEdit{"clear", nil}, testTime)
	assertCode(t, err, AccountMustBeDisabled)
	next.Enabled = false
	cleared, record, changed, err := PlanCredentialUpdate(next, updated, 2, 2, CredentialEdit{"clear", nil}, testTime)
	if err != nil || !changed || record.Meta.Configured || record.Meta.Version != 3 || cleared.ConnectionRevision != 3 {
		t.Fatal("clear", err)
	}
	_, _, changed, err = PlanCredentialUpdate(cleared, record, 3, 3, CredentialEdit{"clear", nil}, testTime)
	if err != nil || changed {
		t.Fatal("already clear noop")
	}
	raw, _ := json.Marshal(r)
	if string(raw) != "{}" {
		t.Fatal("storage record has a wire projection")
	}
	public, _ := json.Marshal(PublicCredentialStatus(fixtureMetas()))
	if strings.Contains(string(public), "ccr_") || strings.Contains(string(public), "credential_id") {
		t.Fatal("public details expose internal IDs")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", r, CredentialEdit{"replace", str("fixture-value")}), "fixture-value") {
		t.Fatal("diagnostic formatting exposes secret")
	}
}
func TestRegistrationPurposeSetAndPreciseMatch(t *testing.T) {
	a := fixtureAccount(t)
	a.Enabled = true
	metas := fixtureMetas()
	records := []CredentialRecord{}
	uses := []CredentialUse{}
	for _, m := range metas {
		records = append(records, CredentialRecord{TenantID: a.TenantID, AccountID: a.ID, Provider: a.Provider, Meta: m})
		uses = append(uses, CredentialUse{m.Purpose, m.ID, m.Version})
	}
	c := Consumer{Kind: "telegram_registration", InstanceID: "gw_1"}
	if err := MatchCredentialUses(a, c, uses, records); err != nil {
		t.Fatal(err)
	}
	assertCode(t, ValidateUses(Telegram, c, uses[:1]), InputInvalid)
	assertCode(t, ValidateUses(Telegram, c, []CredentialUse{uses[0], uses[0]}), InputInvalid)
	epoch := int64(1)
	c.OwnerEpoch = &epoch
	assertCode(t, ValidateUses(Telegram, c, uses), InputInvalid)
	c.OwnerEpoch = nil
	uses[0].Version = 2
	assertCode(t, MatchCredentialUses(a, c, uses, records), CredentialVersionConflict)
	uses[0].Version = 1
	records[0].TenantID = "tnt_other"
	assertCode(t, MatchCredentialUses(a, c, uses, records), SourceIntegrity)
}
