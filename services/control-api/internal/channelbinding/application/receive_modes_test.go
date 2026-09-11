package application

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func TestCreateLongPollingWithOptionalWebhookSecret(t *testing.T) {
	s, store, _, _ := setup(t)
	in := input()
	delete(in.Credentials, domain.TelegramWebhookSecret)
	r, err := s.CreateAccount(context.Background(), owner, "polling", in)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(r.Account.Config)
	var config map[string]any
	_ = json.Unmarshal(raw, &config)
	if config["receive_mode"] != "long_polling" {
		t.Fatalf("mode=%v", config["receive_mode"])
	}
	a := store.data.accounts[r.Account.ID]
	if len(a.Credentials) != 2 {
		t.Fatal("optional credential metadata missing")
	}
	for _, c := range a.Credentials {
		if c.Meta.Purpose == domain.TelegramWebhookSecret && (c.Meta.Configured || c.Meta.Version != 1 || c.Meta.ID == "" || len(c.Ciphertext) != 0) {
			t.Fatal("optional secret is not a stable unconfigured record")
		}
	}
	enabled, err := s.SetAccountEnabled(context.Background(), owner, r.Account.ID, "enable", AccountEnabledInput{ExpectedAccountRevision: 1, Enabled: true})
	if err != nil || !enabled.Account.Enabled {
		t.Fatalf("enable: %v", err)
	}
}

func TestReceiveModeCASAndDisabledTransition(t *testing.T) {
	s, store, _, _ := setup(t)
	in := input()
	delete(in.Credentials, domain.TelegramWebhookSecret)
	r, err := s.CreateAccount(context.Background(), owner, "create-lp", in)
	if err != nil {
		t.Fatal(err)
	}
	id := r.Account.ID
	set := UpdateAccountInput{ExpectedAccountRevision: 1, Config: &AccountConfigInput{ReceiveMode: domain.Webhook}}
	next, err := s.UpdateAccount(context.Background(), owner, id, "mode", set)
	if err != nil || next.Account.Revision != 2 || next.Account.ConnectionRevision != 2 || next.Account.Config.ReceiveMode != domain.Webhook {
		t.Fatalf("change: %+v %v", next, err)
	}
	if _, err = s.SetAccountEnabled(context.Background(), owner, id, "enable-missing", AccountEnabledInput{ExpectedAccountRevision: 2, Enabled: true}); err == nil {
		t.Fatal("webhook enabled without secret")
	}
	set.ExpectedAccountRevision = 2
	same, err := s.UpdateAccount(context.Background(), owner, id, "same", set)
	if err != nil || same.Account.Revision != 2 {
		t.Fatal("same mode changes version", err)
	}
	set.Config.ReceiveMode = domain.LongPolling
	current, err := s.UpdateAccount(context.Background(), owner, id, "back", set)
	if err != nil || current.Account.ConnectionRevision != 3 {
		t.Fatal("ABA", err)
	}
	enabled, err := s.SetAccountEnabled(context.Background(), owner, id, "enabled", AccountEnabledInput{ExpectedAccountRevision: 3, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	set.ExpectedAccountRevision = enabled.Account.Revision
	set.Config.ReceiveMode = domain.Webhook
	if _, err = s.UpdateAccount(context.Background(), owner, id, "must-disable", set); err == nil || err.Error() != domain.AccountMustBeDisabled {
		t.Fatal("enabled mode edit", err)
	}
	if len(store.data.events) != 0 {
		t.Fatal("mode created a route event without a binding")
	}
}

func TestLegacyCreateRecoveryPreservesUncommittedIntentAndReceipt(t *testing.T) {
	s, store, _, _ := setup(t)
	in := input()
	in.LegacyWebhook = true
	created, err := s.CreateAccount(context.Background(), owner, "legacy", in)
	if err != nil || created.Account.Config.ReceiveMode != domain.Webhook {
		t.Fatal("uncommitted legacy intent", err)
	}
	// A receipt written by the deployed webhook-only implementation had no mode.
	for k, r := range store.data.receipts {
		var v map[string]any
		_ = json.Unmarshal(r.Result, &v)
		delete(v["account"].(map[string]any)["config"].(map[string]any), "receive_mode")
		r.Result, _ = json.Marshal(v)
		store.data.receipts[k] = r
	}
	replay, err := s.CreateAccount(context.Background(), owner, "legacy", in)
	if err != nil || replay.Account.Config.ReceiveMode != "" || len(store.data.accounts) != 1 {
		t.Fatal("legacy replay changed historical wire", err)
	}
	in.LegacyWebhook = false
	if _, err = s.CreateAccount(context.Background(), owner, "legacy", in); err != ErrIdempotencyConflict {
		t.Fatal("new default matched legacy command", err)
	}
	in.LegacyWebhook = true
	in.Config = &AccountConfigInput{ReceiveMode: domain.Webhook}
	if _, err = s.CreateAccount(context.Background(), owner, "invalid-legacy", in); err == nil {
		t.Fatal("legacy marker accepted nonlegacy body")
	}
}
