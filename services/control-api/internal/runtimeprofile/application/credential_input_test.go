package application_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const profileWriteJSON = `{"expected_draft_revision":1,"credential_protocol_version":"v1","config":{"models":{"primary":{"kind":"openai_compatible","model":"chat-model","base_url":"https://model.example.test/v1","capabilities":["chat"]}},"tools":{"search":{"kind":"mcp_streamable_http","server_url":"https://tools.example.test/mcp","toolset_name":"web","tool_name":"search_web","auth":{"kind":"bearer"},"capability":"web.search"}},"knowledge":{"docs":{"kind":"qdrant_openai","host":"qdrant.example.test","port":6334,"tls":true,"collection":"docs","embedding":{"model":"embedding-model","base_url":"https://model.example.test/v1","dimensions":1536}}},"storage":{"session":{"kind":"postgres_state"}}},"credentials":{"models":{"primary":{"api_key":{"action":"replace","value":"private-model-value"}}},"tools":{"search":{"bearer_token":{"action":"replace","value":"private-tool-value"}}},"knowledge":{"docs":{"qdrant_api_key":{"action":"replace","value":"private-qdrant-value"},"embedding_api_key":{"action":"replace","value":"private-embedding-value"}}},"storage":{"session":{"dsn":{"action":"replace","value":"postgresql://user:private-db-value@db.example.test/db?sslmode=require"}}}}}`

func TestDecodeProfileWriteAcceptsTypedCredentialActions(t *testing.T) {
	got, err := application.DecodeProfileWrite([]byte(profileWriteJSON))
	if err != nil {
		t.Fatalf("typed write rejected: %v", err)
	}
	if got.ExpectedDraftRevision != 1 || got.CredentialProtocolVersion != "v1" || len(got.Config.Models) != 1 || len(got.Config.Tools) != 1 || len(got.Config.Knowledge) != 1 || len(got.Config.Storage) != 1 {
		t.Fatal("typed configuration was lost")
	}
	for category, resources := range got.Credentials {
		for name, purposes := range resources {
			for purpose, action := range purposes {
				if action.Action != "replace" || action.Value == nil || *action.Value == "" {
					t.Errorf("missing replace action for %s/%s/%s", category, name, purpose)
				}
			}
		}
	}
	for _, action := range []string{"keep", "clear"} {
		t.Run(action, func(t *testing.T) {
			root := writeRoot(t)
			a := modelAction(root)
			a["action"] = action
			delete(a, "value")
			data := writeBytes(t, root)
			if _, err := application.DecodeProfileWrite(data); err != nil {
				t.Fatalf("%s rejected: %v", action, err)
			}
		})
	}
}

func TestDecodeProfileWriteRejectsNonObjectAndDuplicateJSON(t *testing.T) {
	cases := map[string][]byte{"null": []byte(`null`), "array": []byte(`[]`), "string": []byte(`"text"`), "number": []byte(`1`), "empty": nil, "truncated": []byte(`{"expected_draft_revision":1`), "second value": append([]byte(profileWriteJSON), []byte(` {}`)...), "duplicate root": bytes.Replace([]byte(profileWriteJSON), []byte(`"expected_draft_revision":1`), []byte(`"expected_draft_revision":1,"expected_draft_revision":2`), 1), "duplicate secret": bytes.Replace([]byte(profileWriteJSON), []byte(`"value":"private-model-value"`), []byte(`"value":"private-model-value","value":"private-duplicate-value"`), 1), "invalid utf8": append([]byte(profileWriteJSON), 0xff)}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) { assertWriteRejected(t, data) })
	}
}

func TestDecodeProfileWriteRejectsUnknownFieldsAndCanonicalCredentialInjection(t *testing.T) {
	mutations := map[string]func(map[string]any){
		"case-folded action field":   func(r map[string]any) { modelAction(r)["Value"] = "private-casefold-value" },
		"case-folded protocol field": func(r map[string]any) { r["Credential_Protocol_Version"] = "v1" },
		"legacy spec":                func(r map[string]any) { r["spec"] = map[string]any{} },
		"legacy expected revision":   func(r map[string]any) { r["expected_revision"] = 1 },
		"unknown root":               func(r map[string]any) { r["unexpected"] = true },
		"internal model ID": func(r map[string]any) {
			modelConfig(r)["api_key_credential_id"] = "crd_0123456789abcdef0123456789abcdef"
		},
		"legacy model ref":    func(r map[string]any) { modelConfig(r)["api_key_ref"] = "model-key" },
		"model inline secret": func(r map[string]any) { modelConfig(r)["api_key"] = "private-inline-value" },
		"tool internal ID": func(r map[string]any) {
			writeConfig(r)["tools"].(map[string]any)["search"].(map[string]any)["auth"].(map[string]any)["credential_id"] = "crd_0123456789abcdef0123456789abcdef"
		},
		"tool legacy ref": func(r map[string]any) {
			writeConfig(r)["tools"].(map[string]any)["search"].(map[string]any)["auth"].(map[string]any)["secret_ref"] = "tool-key"
		},
		"knowledge internal ID": func(r map[string]any) {
			writeConfig(r)["knowledge"].(map[string]any)["docs"].(map[string]any)["qdrant_api_key_credential_id"] = "crd_0123456789abcdef0123456789abcdef"
		},
		"embedding legacy ref": func(r map[string]any) {
			writeConfig(r)["knowledge"].(map[string]any)["docs"].(map[string]any)["embedding"].(map[string]any)["api_key_ref"] = "embedding-key"
		},
		"storage internal ID": func(r map[string]any) {
			writeConfig(r)["storage"].(map[string]any)["session"].(map[string]any)["dsn_credential_id"] = "crd_0123456789abcdef0123456789abcdef"
		},
		"storage legacy ref": func(r map[string]any) {
			writeConfig(r)["storage"].(map[string]any)["session"].(map[string]any)["dsn_ref"] = "dsn-key"
		},
		"unknown action field": func(r map[string]any) { modelAction(r)["secret_value"] = "private-unknown-value" },
		"old action value":     func(r map[string]any) { a := modelAction(r); delete(a, "value"); a["secret"] = "private-old-value" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) { r := writeRoot(t); mutate(r); assertWriteRejected(t, writeBytes(t, r)) })
	}
}

func TestDecodeProfileWriteRejectsMalformedActionsAndCollections(t *testing.T) {
	mutations := map[string]func(map[string]any){
		"missing protocol": func(r map[string]any) { delete(r, "credential_protocol_version") },
		"wrong protocol":   func(r map[string]any) { r["credential_protocol_version"] = "v2" },
		"zero revision":    func(r map[string]any) { r["expected_draft_revision"] = 0 },
		"unknown action":   func(r map[string]any) { modelAction(r)["action"] = "rotate" },
		"unknown category": func(r map[string]any) { r["credentials"].(map[string]any)["unknown"] = map[string]any{} },
		"unknown purpose": func(r map[string]any) {
			r["credentials"].(map[string]any)["models"].(map[string]any)["primary"].(map[string]any)["password"] = map[string]any{"action": "keep"}
		},
		"unknown resource": func(r map[string]any) {
			r["credentials"].(map[string]any)["models"].(map[string]any)["missing"] = map[string]any{"api_key": map[string]any{"action": "keep"}}
		},
		"missing action":               func(r map[string]any) { delete(modelAction(r), "action") },
		"missing value":                func(r map[string]any) { delete(modelAction(r), "value") },
		"null value":                   func(r map[string]any) { modelAction(r)["value"] = nil },
		"wrong value type":             func(r map[string]any) { modelAction(r)["value"] = 123 },
		"keep with value":              func(r map[string]any) { modelAction(r)["action"] = "keep" },
		"clear with value":             func(r map[string]any) { modelAction(r)["action"] = "clear" },
		"negative credential revision": func(r map[string]any) { modelAction(r)["expected_credential_revision"] = -1 },
		"config null":                  func(r map[string]any) { r["config"] = nil },
		"model resource null":          func(r map[string]any) { writeConfig(r)["models"].(map[string]any)["primary"] = nil },
		"null action": func(r map[string]any) {
			r["credentials"].(map[string]any)["models"].(map[string]any)["primary"].(map[string]any)["api_key"] = nil
		},
		"null action resources": func(r map[string]any) { r["credentials"].(map[string]any)["models"] = nil },
		"null action purposes":  func(r map[string]any) { r["credentials"].(map[string]any)["models"].(map[string]any)["primary"] = nil },
	}
	for _, category := range []string{"models", "tools", "knowledge", "storage"} {
		mutations["missing "+category] = func(r map[string]any) { delete(writeConfig(r), category) }
		mutations["null "+category] = func(r map[string]any) { writeConfig(r)[category] = nil }
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) { r := writeRoot(t); mutate(r); assertWriteRejected(t, writeBytes(t, r)) })
	}
	for _, value := range []string{"", " ", "****", "••••", "●●●", "[redacted]", "unchanged", "<unchanged>", "private\nvalue", "private\x00value"} {
		t.Run("invalid credential value", func(t *testing.T) {
			r := writeRoot(t)
			modelAction(r)["value"] = value
			assertWriteRejected(t, writeBytes(t, r))
		})
	}
}

func TestParseStorageCredentialSeparatesDestinationFromPassword(t *testing.T) {
	for _, scheme := range []string{"postgres", "postgresql"} {
		for _, mode := range []string{"disable", "require", "verify-full"} {
			t.Run(scheme+" "+mode, func(t *testing.T) {
				destination, password, err := application.ParseStorageCredential(scheme + "://agent:p%40ss%3Aword@DB.Example.Test:6432/agent_db?sslmode=" + mode)
				if err != nil {
					t.Fatalf("valid URI rejected: %v", err)
				}
				want := domain.StorageDestination{Host: "db.example.test", Port: 6432, Database: "agent_db", Username: "agent", SSLMode: mode}
				if destination != want || string(password) != "p@ss:word" {
					t.Fatal("URI credential was not split into the expected password and destination")
				}
				encoded, _ := json.Marshal(destination)
				if bytes.Contains(encoded, password) {
					t.Fatal("destination exposes password")
				}
			})
		}
	}
	dest, password, err := application.ParseStorageCredential("postgres://agent:secret@[2001:db8::1]/db?sslmode=require")
	if err != nil || dest.Host != "2001:db8::1" || dest.Port != 5432 || string(password) != "secret" {
		t.Fatal("single IPv6 destination or default port rejected")
	}
}

func TestParseStorageCredentialRejectsUnsupportedDSNsWithoutEcho(t *testing.T) {
	const secret = "private-parser-canary"
	cases := []string{
		"host=db user=agent password=" + secret + " sslmode=require",
		"mysql://agent:" + secret + "@db/database?sslmode=require",
		"postgres://agent:" + secret + "@db/database",
		"postgres://agent:" + secret + "@db/database?sslmode=prefer",
		"postgres://agent:" + secret + "@db/database?sslmode=require&options=-csearch_path=public",
		"postgres://agent:" + secret + "@db/database?sslmode=require&sslmode=disable",
		"postgres://agent:" + secret + "@db,other/database?sslmode=require",
		"postgres://agent:" + secret + "@db:5432,other:5432/database?sslmode=require",
		"postgres://agent:" + secret + "@/database?sslmode=require",
		"postgres://agent:" + secret + "@db/?sslmode=require",
		"postgres://agent:" + secret + "@db/a/b?sslmode=require",
		"postgres://agent:" + secret + "@db/a%2Fb?sslmode=require",
		"postgres://agent:" + secret + "@db:0/database?sslmode=require",
		"postgres://agent:" + secret + "@db:65536/database?sslmode=require",
		"postgres://agent:" + secret + "@db:abc/database?sslmode=require",
		"postgres://agent:" + secret + "@[not-ip]/database?sslmode=require",
		"postgres://agent:" + secret + "@db/database?sslmode=require#fragment",
		"postgres://agent:" + secret + "@db/database?sslmode=require#",
		"postgres://agent@db/database?sslmode=require",
		"postgres://agent:@db/database?sslmode=require",
		"postgres://:" + secret + "@db/database?sslmode=require",
		"postgres://agent:****@db/database?sslmode=require",
		"postgres://agent:%00@db/database?sslmode=require",
		"postgres://agent:%zz@db/database?sslmode=require",
		"postgres://agent:" + secret + "@db/database?sslmode=require&application_name=canary",
	}
	for index, value := range cases {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			destination, password, err := application.ParseStorageCredential(value)
			if !errors.Is(err, domain.ErrCredentialInput) {
				t.Errorf("unsupported DSN was not rejected (case %d)", index)
			}
			if err != nil && strings.Contains(err.Error(), secret) {
				t.Fatal("DSN parser error echoed secret")
			}
			if destination != (domain.StorageDestination{}) || len(password) != 0 {
				t.Errorf("failed DSN parse retained destination or secret (case %d)", index)
			}
		})
	}
}

func assertWriteRejected(t *testing.T, data []byte) {
	t.Helper()
	got, err := application.DecodeProfileWrite(data)
	if !errors.Is(err, domain.ErrCredentialInput) {
		t.Error("invalid write was not rejected")
	}
	if err != nil && (strings.Contains(err.Error(), "private-") || len(data) > 0 && strings.Contains(err.Error(), string(data))) {
		t.Fatal("input error echoes secret-bearing request")
	}
	if err != nil && !reflect.DeepEqual(got, application.ProfileWrite{}) {
		t.Error("failed decode retained a partial secret-bearing DTO")
	}
}
func writeRoot(t *testing.T) map[string]any {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal([]byte(profileWriteJSON), &root); err != nil {
		t.Fatal(err)
	}
	return root
}
func writeConfig(root map[string]any) map[string]any { return root["config"].(map[string]any) }
func modelConfig(root map[string]any) map[string]any {
	return writeConfig(root)["models"].(map[string]any)["primary"].(map[string]any)
}
func modelAction(root map[string]any) map[string]any {
	return root["credentials"].(map[string]any)["models"].(map[string]any)["primary"].(map[string]any)["api_key"].(map[string]any)
}
func writeBytes(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
