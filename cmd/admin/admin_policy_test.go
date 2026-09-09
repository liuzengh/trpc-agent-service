package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

// Keep the real TenantService validation, credential encryption and repository
// write path behind the HTTP handler. Only persistence and its initial read
// snapshot are isolated from external infrastructure.
type credentialUpdateRepository struct {
	tenant.Repository
	stored *tenant.Tenant
	writes int
}

func (r *credentialUpdateRepository) Close() error { return nil }

func (r *credentialUpdateRepository) Update(_ context.Context, value *tenant.Tenant) error {
	r.stored = value
	r.writes++
	return nil
}

type credentialUpdateService struct {
	*tenant.TenantService
	current  *tenant.Tenant
	received *tenant.Tenant
}

func (s *credentialUpdateService) GetTenant(context.Context, string) (*tenant.Tenant, error) {
	return s.current, nil
}

func (s *credentialUpdateService) UpdateTenant(ctx context.Context, value *tenant.Tenant) error {
	s.received = value
	return s.TenantService.UpdateTenant(ctx, value)
}

func credentialTenantFixture() *tenant.Tenant {
	return &tenant.Tenant{
		ID: "tenant-credential-test", Name: "Credential test", Status: tenant.TenantStatusActive, ConfigVersion: 1,
		Agents:     []tenant.AgentConfig{{Name: "support", Type: "llm", DefaultModel: "gpt-4o-mini", MaxLLMCalls: 1}},
		Models:     []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt-4o-mini", APIKey: "test-only-model-credential", MaxTokens: 1024}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Channels: []tenant.ChannelBinding{{
			Type: "wework", AccountID: "test-account", WebhookKey: "test-route", AgentApp: "support", AppID: "1000002",
			Token: "test-only-callback-token", Secret: "test-only-wecom-corp-secret", EncodingAESKey: strings.Repeat("A", 43),
			Config:       map[string]string{"corp_id": "ww0123456789abcdef"},
			AccessPolicy: tenant.ChannelAccessPolicy{AllowDirectMessages: true, AllowedUsers: []string{"test-user"}},
		}},
		Storage: tenant.StorageConfig{SessionBackend: "postgres", SessionProfile: "test-postgres", MemoryBackend: "redis", MemoryProfile: "test-redis"},
	}
}

var credentialFields = []struct {
	name string
	get  func(*tenant.Tenant) (*string, *string)
}{
	{"model", func(value *tenant.Tenant) (*string, *string) {
		return &value.Models[0].APIKey, &value.Models[0].APIKeyRef
	}},
	{"channel-token", func(value *tenant.Tenant) (*string, *string) {
		return &value.Channels[0].Token, &value.Channels[0].TokenRef
	}},
	{"channel-secret", func(value *tenant.Tenant) (*string, *string) {
		return &value.Channels[0].Secret, &value.Channels[0].SecretRef
	}},
	{"channel-aes", func(value *tenant.Tenant) (*string, *string) {
		return &value.Channels[0].EncodingAESKey, &value.Channels[0].EncodingAESKeyRef
	}},
}

func updateCredentials(t *testing.T, current, update *tenant.Tenant, wantStatus int) *tenant.Tenant {
	t.Helper()
	original, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/api/v1/tenants/"+current.ID, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(adminauth.ContextWithPrincipal(request.Context(), adminauth.Principal{ID: "credential-test-admin", Role: adminauth.RolePlatformAdmin}))
	repository := &credentialUpdateRepository{}
	production := tenant.NewService(repository, strings.Repeat("k", 32))
	t.Cleanup(func() { _ = production.Close() })
	service := &credentialUpdateService{TenantService: production, current: current}
	response := httptest.NewRecorder()
	updateTenant(response, request, service, current.ID)
	if response.Code != wantStatus {
		t.Fatalf("update status=%d, want %d; error=%s", response.Code, wantStatus, response.Body.String())
	}
	after, err := json.Marshal(current)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, after) {
		t.Fatal("credential merge mutated the existing tenant snapshot")
	}
	if wantStatus != http.StatusOK && repository.writes != 0 {
		t.Fatal("invalid credential configuration reached persistence")
	}
	if wantStatus == http.StatusOK {
		if repository.writes != 1 || repository.stored == nil {
			t.Fatal("successful HTTP update did not persist through TenantService")
		}
		if service.received == nil || tenantContainsRedacted(service.received) {
			t.Fatal("successful update did not reach validation with materialized credential sources")
		}
		for _, field := range credentialFields {
			inline, ref := field.get(service.received)
			storedInline, storedRef := field.get(repository.stored)
			if *ref != *storedRef || (*inline == "" && *storedInline != "") {
				t.Fatalf("persistence changed %s credential source", field.name)
			}
			if *inline != "" && *storedInline == *inline {
				t.Fatalf("persistence did not encrypt %s inline credential", field.name)
			}
			if *inline != "" && strings.Contains(response.Body.String(), *inline) {
				t.Fatalf("response exposed %s inline credential", field.name)
			}
		}
		if inline := service.received.Channels[0].Config["encoding_aes_key"]; inline != "" {
			if repository.stored.Channels[0].Config["encoding_aes_key"] == inline || strings.Contains(response.Body.String(), inline) {
				t.Fatal("legacy AES credential was stored or returned without protection")
			}
		}
	}
	return service.received
}

func TestTenantCredentialSourceTransitions(t *testing.T) {
	const reference = "env://TRPC_SECRET_TEST_CREDENTIAL"
	for _, field := range credentialFields {
		for _, representation := range []string{"", redactedValue} {
			name := "omitted"
			if representation != "" {
				name = "masked"
			}
			t.Run(field.name+"/inline-to-ref/"+name, func(t *testing.T) {
				current := credentialTenantFixture()
				update := redactTenant(current)
				value, ref := field.get(update)
				*value, *ref = representation, reference
				received := updateCredentials(t, current, update, http.StatusOK)
				value, ref = field.get(received)
				if *value != "" || *ref != reference {
					t.Fatal("explicit reference was mixed with the previous inline source")
				}
			})
			t.Run(field.name+"/keep-inline/"+name, func(t *testing.T) {
				current := credentialTenantFixture()
				previous, _ := field.get(current)
				update := redactTenant(current)
				value, ref := field.get(update)
				*value, *ref = representation, ""
				received := updateCredentials(t, current, update, http.StatusOK)
				value, ref = field.get(received)
				if *value != *previous || *ref != "" {
					t.Fatal("masked inline credential was not preserved")
				}
			})
			t.Run(field.name+"/keep-ref/"+name, func(t *testing.T) {
				current := credentialTenantFixture()
				value, ref := field.get(current)
				*value, *ref = "", reference
				update := redactTenant(current)
				value, ref = field.get(update)
				*value, *ref = representation, ""
				received := updateCredentials(t, current, update, http.StatusOK)
				value, ref = field.get(received)
				if *value != "" || *ref != reference {
					t.Fatal("unchanged reference-backed credential was not preserved")
				}
			})
		}
		t.Run(field.name+"/ref-to-inline", func(t *testing.T) {
			current := credentialTenantFixture()
			value, ref := field.get(current)
			inline := *value
			*value, *ref = "", reference
			update := redactTenant(current)
			value, ref = field.get(update)
			*value, *ref = inline, ""
			received := updateCredentials(t, current, update, http.StatusOK)
			value, ref = field.get(received)
			if *value != inline || *ref != "" {
				t.Fatal("explicit inline rotation restored the previous reference")
			}
		})
		t.Run(field.name+"/explicit-conflict", func(t *testing.T) {
			current := credentialTenantFixture()
			update := redactTenant(current)
			previous, _ := field.get(current)
			value, ref := field.get(update)
			*value, *ref = *previous, reference
			received := updateCredentials(t, current, update, http.StatusBadRequest)
			if received == nil {
				t.Fatal("explicit source conflict did not reach the tenant validator")
			}
			value, ref = field.get(received)
			if *value != *previous || *ref != reference {
				t.Fatal("merge silently discarded an explicit source conflict")
			}
		})
	}
}

func TestTenantAESAliasSourceTransitions(t *testing.T) {
	const reference = "env://TRPC_SECRET_TEST_AES"
	t.Run("masked-roundtrip", func(t *testing.T) {
		current := credentialTenantFixture()
		inline := current.Channels[0].EncodingAESKey
		current.Channels[0].Config["encoding_aes_key"] = inline
		current.Channels[0].EncodingAESKey = ""
		update := redactTenant(current)
		update.Name = "Updated tenant name"
		received := updateCredentials(t, current, update, http.StatusOK)
		if received.Channels[0].EncodingAESKey != "" || received.Channels[0].EncodingAESKeyRef != "" || received.Channels[0].Config["encoding_aes_key"] != inline {
			t.Fatal("masked alias roundtrip changed the credential source")
		}
	})
	t.Run("canonical-to-legacy", func(t *testing.T) {
		current := credentialTenantFixture()
		update := redactTenant(current)
		inline := base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
		update.Channels[0].Config["encoding_aes_key"] = inline
		received := updateCredentials(t, current, update, http.StatusOK)
		if received.Channels[0].EncodingAESKey != "" || received.Channels[0].EncodingAESKeyRef != "" || received.Channels[0].Config["encoding_aes_key"] != inline {
			t.Fatal("explicit alias rotation resurrected the canonical value")
		}
	})
	for _, configForm := range []string{"omitted", "masked", "empty"} {
		t.Run("legacy-to-ref/"+configForm, func(t *testing.T) {
			current := credentialTenantFixture()
			current.Channels[0].Config["encoding_aes_key"] = current.Channels[0].EncodingAESKey
			current.Channels[0].EncodingAESKey = ""
			update := redactTenant(current)
			update.Channels[0].EncodingAESKeyRef = reference
			switch configForm {
			case "omitted":
				update.Channels[0].Config = nil
			case "empty":
				update.Channels[0].Config["encoding_aes_key"] = ""
			}
			received := updateCredentials(t, current, update, http.StatusOK)
			if received.Channels[0].EncodingAESKeyRef != reference || received.Channels[0].Config["encoding_aes_key"] != "" {
				t.Fatal("reference switch resurrected the legacy AES value")
			}
		})
	}
	t.Run("ref-to-legacy", func(t *testing.T) {
		current := credentialTenantFixture()
		inline := current.Channels[0].EncodingAESKey
		current.Channels[0].EncodingAESKey, current.Channels[0].EncodingAESKeyRef = "", reference
		update := redactTenant(current)
		update.Channels[0].EncodingAESKeyRef = ""
		update.Channels[0].Config["encoding_aes_key"] = inline
		received := updateCredentials(t, current, update, http.StatusOK)
		if received.Channels[0].EncodingAESKeyRef != "" || received.Channels[0].Config["encoding_aes_key"] != inline {
			t.Fatal("legacy inline rotation restored the old reference")
		}
	})
	t.Run("legacy-to-canonical", func(t *testing.T) {
		current := credentialTenantFixture()
		current.Channels[0].Config["encoding_aes_key"] = current.Channels[0].EncodingAESKey
		current.Channels[0].EncodingAESKey = ""
		update := redactTenant(current)
		inline := base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
		update.Channels[0].EncodingAESKey = inline
		received := updateCredentials(t, current, update, http.StatusOK)
		if received.Channels[0].EncodingAESKey != inline || received.Channels[0].Config["encoding_aes_key"] != "" {
			t.Fatal("canonical rotation retained the old legacy alias")
		}
	})
	t.Run("explicit-alias-and-ref-conflict", func(t *testing.T) {
		current := credentialTenantFixture()
		update := redactTenant(current)
		update.Channels[0].EncodingAESKeyRef = reference
		update.Channels[0].Config["encoding_aes_key"] = current.Channels[0].EncodingAESKey
		received := updateCredentials(t, current, update, http.StatusBadRequest)
		if received == nil || received.Channels[0].Config["encoding_aes_key"] == "" || received.Channels[0].EncodingAESKeyRef != reference {
			t.Fatal("explicit alias conflict was silently discarded")
		}
	})
	t.Run("unsupported-config-alias-stays-rejected", func(t *testing.T) {
		current := credentialTenantFixture()
		update := redactTenant(current)
		update.Channels[0].TokenRef = "env://TRPC_SECRET_TEST_TOKEN"
		update.Channels[0].Config["token"] = "test-only-unsupported-token"
		updateCredentials(t, current, update, http.StatusBadRequest)
	})
}
