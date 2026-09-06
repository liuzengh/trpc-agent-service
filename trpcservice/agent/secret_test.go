package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestRevisionCompilerCannotReadUnassignedEnvironment(t *testing.T) {
	t.Setenv("OTHER_TENANT_KEY", "secret-canary")
	for _, keyField := range []string{`"api_key_env":"OTHER_TENANT_KEY"`, `"api_key_ref":"env://OTHER_TENANT_KEY"`} {
		data := controlplane.DefaultBootstrapData()
		data.Revisions[0].ModelConfig = json.RawMessage(`{"source":"revision","provider":"openai","name":"example","base_url":"https://untrusted.example/v1",` + keyField + `}`)
		repository := controlplane.NewMemoryRepository(data)
		defer repository.Close()
		for _, grant := range []secret.Grant{
			{TenantID: "another-tenant", Purpose: secret.Model, Reference: "env://OTHER_TENANT_KEY"},
			{TenantID: "tutorial-tenant", Purpose: secret.Memory, Reference: "env://OTHER_TENANT_KEY"},
		} {
			store, _ := secret.NewEnvStore([]secret.Grant{grant})
			compiler, _ := NewRevisionCompiler(repository, NewTutorialModel(), false, WithSecretStore(store))
			if _, err := compiler.Compile(context.Background(), runtimecontext.TutorialScope()); !errors.Is(err, secret.ErrForbidden) {
				t.Fatalf("compile error: %v", err)
			}
		}
		compiler, _ := NewRevisionCompiler(repository, NewTutorialModel(), false)
		if _, err := compiler.Compile(context.Background(), runtimecontext.TutorialScope()); !errors.Is(err, secret.ErrForbidden) {
			t.Fatalf("default compiler: %v", err)
		}
	}
	if events, err := (DisabledModel{}).GenerateContent(context.Background(), nil); err == nil || events != nil {
		t.Fatal("disabled model generated a reply")
	}
}
