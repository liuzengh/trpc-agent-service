package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func valid() Acceptance {
	return Acceptance{Input: Inbound{Key: EventKey{Provider: "telegram", AccountID: "account", EventID: "1"}, Kind: "text", ConversationID: "100", SenderID: "100", Text: "hello", ReplyContext: json.RawMessage(`{"chat_id":"100"}`), SourceDigest: strings.Repeat("a", 64), ReceivedAt: time.Now().UTC()}, Receipt: Receipt{Decision: "admit-run", AdmissionID: "admission", RunID: "run"}, Route: &RouteSnapshot{Provider: "telegram", AccountID: "account", TenantID: "tenant", BindingID: "binding", Generation: 1, DeploymentRevisionID: "revision", ManifestRef: "manifest/revision", ManifestDigest: "sha256:" + strings.Repeat("b", 64)}}
}
func TestAcceptanceValidation(t *testing.T) {
	if err := valid().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Acceptance){
		"provider": func(c *Acceptance) { c.Input.Key.Provider = "untrusted" }, "account": func(c *Acceptance) { c.Input.Key.AccountID = " account" }, "event": func(c *Acceptance) { c.Input.Key.EventID = "\n" },
		"digest": func(c *Acceptance) { c.Input.SourceDigest = "bad" }, "receive time": func(c *Acceptance) { c.Input.ReceivedAt = time.Time{} }, "text": func(c *Acceptance) { c.Input.Text = " " }, "sender": func(c *Acceptance) { c.Input.SenderID = "" }, "reply json": func(c *Acceptance) { c.Input.ReplyContext = json.RawMessage(`{`) },
		"large text": func(c *Acceptance) { c.Input.Text = strings.Repeat("a", 65537) }, "wrong kind": func(c *Acceptance) { c.Input.Kind = "service" }, "route missing": func(c *Acceptance) { c.Route = nil }, "route wrong account": func(c *Acceptance) { c.Route.AccountID = "other" }, "route digest": func(c *Acceptance) { c.Route.ManifestDigest = "none" }, "route generation": func(c *Acceptance) { c.Route.Generation = 0 }, "missing run": func(c *Acceptance) { c.Receipt.RunID = "" }, "decision mismatch": func(c *Acceptance) { c.Receipt.Decision = "ignore" }, "reason on run": func(c *Acceptance) { c.Receipt.Reason = "unexpected" },
	} {
		t.Run(name, func(t *testing.T) {
			c := valid()
			mutate(&c)
			if !errors.Is(c.Validate(), ErrInvalidInput) {
				t.Fatal("invalid acceptance accepted")
			}
		})
	}
	for _, kind := range []string{"ignore", "interaction"} {
		c := valid()
		c.Input.Kind = kind
		c.Input.Text = ""
		c.Route = nil
		c.Receipt = Receipt{Decision: kind, Reason: "audited"}
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		c.Input.Text = "never prompt"
		if !errors.Is(c.Validate(), ErrInvalidInput) {
			t.Fatal("non-run text accepted")
		}
	}
}
