package application_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestSaveCredentialDraftRejectsCredentialBearingEndpoints(t *testing.T) {
	for _, category := range []string{"model", "tool", "embedding", "knowledge-host", "storage-host"} {
		for _, endpoint := range []string{
			"https://user:private-endpoint-canary@example.test/v1",
			"https://example.test/v1?api_key=private-endpoint-canary",
			"https://example.test/v1#private-endpoint-canary",
		} {
			t.Run(category+"/"+strings.Split(endpoint, ":")[0], func(t *testing.T) {
				h := newCredentialHarness(t)
				command := modelCredentialCommand(1, "unsafe-endpoint", "write-only-model-value")
				switch category {
				case "model":
					model := command.Write.Config.Models["primary"]
					model.BaseURL = endpoint
					command.Write.Config.Models["primary"] = model
				case "tool":
					command.Write.Config.Tools["search"] = application.ToolConfig{Kind: domain.ToolKindMCPStreamableHTTP, ServerURL: endpoint, Auth: application.ToolAuthConfig{Kind: domain.AuthKindNone}}
				case "embedding":
					command.Write.Config.Knowledge["docs"] = application.KnowledgeConfig{Kind: domain.KnowledgeKindQdrantOpenAI, Host: "qdrant.example.test", Embedding: application.EmbeddingConfig{BaseURL: endpoint}}
				case "knowledge-host":
					command.Write.Config.Knowledge["docs"] = application.KnowledgeConfig{Kind: domain.KnowledgeKindQdrantOpenAI, Host: endpoint}
				case "storage-host":
					command.Write.Config.Storage["session"] = application.StorageConfig{Kind: domain.StorageKindPostgresState, Destination: &domain.StorageDestination{Host: endpoint}}
				}
				before := h.snapshot(t)
				_, err := h.service.SaveCredentialDraft(context.Background(), command)
				if !errors.Is(err, domain.ErrCredentialInput) {
					t.Fatalf("credential-bearing endpoint reached persistence: %v", err)
				}
				if h.snapshot(t) != before {
					t.Fatal("rejected endpoint changed draft, credential, or receipt")
				}
			})
		}
	}
}
