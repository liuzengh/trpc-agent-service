package llm

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/anthropic"
	"trpc.group/trpc-go/trpc-agent-go/model/gemini"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
)

type fakeModel struct{}

func (f *fakeModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	return nil, nil
}

func (f *fakeModel) Info() model.Info { return model.Info{Name: "fake"} }

func TestRegistryCachesModel(t *testing.T) {
	calls := 0
	factory := func(ctx context.Context, ep Endpoint) (model.Model, error) {
		calls++
		return &fakeModel{}, nil
	}
	r := NewRegistry(factory)
	r.Upsert(context.Background(), Endpoint{ID: "e1", ModelName: "m1", BaseURL: "http://x", APIKey: "k"})

	m1, err := r.Resolve(context.Background(), "e1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	m2, err := r.Resolve(context.Background(), "e1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if m1 != m2 {
		t.Error("Resolve should return the same cached instance")
	}
	if calls != 1 {
		t.Errorf("factory calls = %d, want 1", calls)
	}
}

func TestRegistryUnknownEndpoint(t *testing.T) {
	r := NewRegistry(nil)
	if _, err := r.Resolve(context.Background(), "nope"); err == nil {
		t.Error("unknown endpoint should return an error")
	}
}

func TestUpsertInvalidatesCache(t *testing.T) {
	calls := 0
	factory := func(ctx context.Context, ep Endpoint) (model.Model, error) {
		calls++
		return &fakeModel{}, nil
	}
	r := NewRegistry(factory)
	r.Upsert(context.Background(), Endpoint{ID: "e1", ModelName: "m1"})
	_, _ = r.Resolve(context.Background(), "e1")
	r.Upsert(context.Background(), Endpoint{ID: "e1", ModelName: "m2"}) // invalidate
	_, _ = r.Resolve(context.Background(), "e1")
	if calls != 2 {
		t.Errorf("factory calls = %d, want 2 after Upsert", calls)
	}
}

func TestNormalizeProvider(t *testing.T) {
	cases := map[string]string{
		"":                  ProviderOpenAICompat,
		"openai":            ProviderOpenAICompat,
		"openai-compatible": ProviderOpenAICompat,
		"openai_compatible": ProviderOpenAICompat,
		"anthropic":         ProviderAnthropic,
		"claude":            ProviderAnthropic,
		"gemini":            ProviderGemini,
		"google":            ProviderGemini,
		"vertex":            ProviderGemini,
		"my-custom":         "my-custom",
	}
	for in, want := range cases {
		if got := NormalizeProvider(in); got != want {
			t.Errorf("NormalizeProvider(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDefaultFactoryDispatches(t *testing.T) {
	ctx := context.Background()

	t.Run("openai", func(t *testing.T) {
		m, err := DefaultFactory(ctx, Endpoint{Provider: "openai", ModelName: "gpt-4o", BaseURL: "https://x", APIKey: "k"})
		if err != nil {
			t.Fatalf("openai: %v", err)
		}
		if _, ok := m.(*openai.Model); !ok {
			t.Fatalf("openai: got %T, want *openai.Model", m)
		}
	})

	t.Run("openai-compatible-empty-provider", func(t *testing.T) {
		m, err := DefaultFactory(ctx, Endpoint{ModelName: "glm-4.7", BaseURL: "https://x", APIKey: "k"})
		if err != nil {
			t.Fatalf("default: %v", err)
		}
		if _, ok := m.(*openai.Model); !ok {
			t.Fatalf("default: got %T, want *openai.Model", m)
		}
	})

	t.Run("anthropic", func(t *testing.T) {
		m, err := DefaultFactory(ctx, Endpoint{Provider: "anthropic", ModelName: "claude-3-5-sonnet", BaseURL: "https://x", APIKey: "k"})
		if err != nil {
			t.Fatalf("anthropic: %v", err)
		}
		if _, ok := m.(*anthropic.Model); !ok {
			t.Fatalf("anthropic: got %T, want *anthropic.Model", m)
		}
	})

	t.Run("gemini", func(t *testing.T) {
		m, err := DefaultFactory(ctx, Endpoint{Provider: "gemini", ModelName: "gemini-2.0-flash", APIKey: "k"})
		if err != nil {
			t.Fatalf("gemini: %v", err)
		}
		if _, ok := m.(*gemini.Model); !ok {
			t.Fatalf("gemini: got %T, want *gemini.Model", m)
		}
	})

	t.Run("unknown", func(t *testing.T) {
		if _, err := DefaultFactory(ctx, Endpoint{Provider: "unknown"}); err == nil {
			t.Fatal("expected error for unknown provider")
		}
	})
}

func TestRegistryDispatchesByProvider(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry(nil) // DefaultFactory

	r.Upsert(context.Background(), Endpoint{ID: "e1", Provider: "anthropic", ModelName: "claude", BaseURL: "https://x", APIKey: "k"})
	m, err := r.Resolve(ctx, "e1")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, ok := m.(*anthropic.Model); !ok {
		t.Fatalf("got %T, want *anthropic.Model", m)
	}

	// cache hit must return the same instance
	m2, err := r.Resolve(ctx, "e1")
	if err != nil {
		t.Fatalf("resolve(cached): %v", err)
	}
	if m2 != m {
		t.Fatal("expected cached instance to be reused")
	}
}
