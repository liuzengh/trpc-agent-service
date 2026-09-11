package channelv1

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAllSchemasCompile(t *testing.T) {
	compiled.Do(compile)
	if compiled.err != nil {
		t.Fatal(compiled.err)
	}
	if len(compiled.schemas) != 22 {
		t.Fatalf("got %d schemas", len(compiled.schemas))
	}
}
func TestProtocolFixtures(t *testing.T) {
	entries, err := os.ReadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		t.Run(entry.Name(), func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("fixtures", entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			var f struct {
				Schema   string          `json:"schema"`
				Valid    bool            `json:"valid"`
				Document json.RawMessage `json:"document"`
			}
			if err = json.Unmarshal(raw, &f); err != nil {
				t.Fatal(err)
			}
			err = Validate(f.Schema, f.Document)
			if (err == nil) != f.Valid {
				t.Fatalf("valid=%v got=%v", f.Valid, err)
			}
		})
	}
}
func TestSecretErrorsAreSanitizedAndObjectsClosed(t *testing.T) {
	valid := `{"provider":"telegram","provider_account_id":"123","name":"test","credentials":{"telegram.bot_token":{"action":"replace","value":"TEST_SECRET_DO_NOT_ECHO"},"telegram.webhook_secret":{"action":"replace","value":"fixture_secret"}}}`
	if err := Validate("account-create.schema.json", []byte(valid)); err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"case":      strings.Replace(valid, `"name"`, `"Name"`, 1),
		"duplicate": strings.Replace(valid, `"name":"test"`, `"name":"test","name":"second"`, 1),
		"null":      strings.Replace(valid, `"name":"test"`, `"name":null`, 1),
		"unknown":   strings.Replace(valid, `"name":"test"`, `"name":"test","environment_id":"env_x"`, 1),
		"newline":   strings.Replace(valid, `"fixture_secret"`, `"fixture_secret\n"`, 1),
		"trailing":  valid + " {}",
	} {
		t.Run(name, func(t *testing.T) {
			err := Validate("account-create.schema.json", []byte(raw))
			if !errors.Is(err, ErrInvalidDocument) || strings.Contains(err.Error(), "TEST_SECRET") {
				t.Fatal("unsanitized or accepted error", err)
			}
		})
	}
}
func TestRegistrationRequiresExactPurposeSet(t *testing.T) {
	base := map[string]any{"schema_version": 1, "scope_id": "gateway_pool", "source_epoch": "00000000-0000-4000-8000-000000000001", "connection_revision": 1, "consumer": map[string]any{"kind": "telegram_registration", "instance_id": "gw_1"}, "uses": []any{map[string]any{"purpose": "telegram.bot_token", "credential_id": "ccr_a", "credential_version": 1}, map[string]any{"purpose": "telegram.webhook_secret", "credential_id": "ccr_b", "credential_version": 1}}}
	raw, _ := json.Marshal(base)
	if err := Validate("credentials-resolve-request.schema.json", raw); err != nil {
		t.Fatal(err)
	}
	uses := base["uses"].([]any)
	base["uses"] = uses[:1]
	raw, _ = json.Marshal(base)
	if err := Validate("credentials-resolve-request.schema.json", raw); err == nil {
		t.Fatal("single purpose registration accepted")
	}
	base["uses"] = []any{uses[0], uses[0]}
	raw, _ = json.Marshal(base)
	if err := Validate("credentials-resolve-request.schema.json", raw); err == nil {
		t.Fatal("duplicate purpose registration accepted")
	}
	base["uses"] = uses
	base["consumer"].(map[string]any)["owner_epoch"] = 1
	raw, _ = json.Marshal(base)
	if err := Validate("credentials-resolve-request.schema.json", raw); err == nil {
		t.Fatal("telegram borrowed wecom owner epoch")
	}
}
func TestWebhookSecretBoundariesInSchema(t *testing.T) {
	for _, tc := range []struct {
		v     string
		valid bool
	}{{"", false}, {"a", true}, {strings.Repeat("a", 256), true}, {strings.Repeat("a", 257), false}, {"_-", true}, {"中文", false}, {"a\n", false}, {"a b", false}, {"a/b", false}} {
		v := map[string]any{"provider": "telegram", "provider_account_id": "123", "name": "test", "credentials": map[string]any{"telegram.bot_token": map[string]any{"action": "replace", "value": "fixture"}, "telegram.webhook_secret": map[string]any{"action": "replace", "value": tc.v}}}
		raw, _ := json.Marshal(v)
		if err := Validate("account-create.schema.json", raw); (err == nil) != tc.valid {
			t.Fatalf("len=%d valid=%v err=%v", len(tc.v), tc.valid, err)
		}
	}
}

func TestCommonDefinitionsAreNotARequestSchema(t *testing.T) {
	if err := Validate("common.schema.json", []byte(`{"value":"fixture"}`)); !errors.Is(err, ErrUnknownSchema) {
		t.Fatal("definitions root must not validate requests", err)
	}
}
