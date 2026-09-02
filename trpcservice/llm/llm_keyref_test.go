package llm

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

type keyrefModel struct{ name string }

func (m *keyrefModel) Info() model.Info { return model.Info{Name: m.name} }
func (m *keyrefModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	return nil, errors.New("not used")
}

type fakeKeySource struct{ m map[string]string }

func (f *fakeKeySource) Get(_ context.Context, key string) (string, error) {
	if v, ok := f.m[key]; ok {
		return v, nil
	}
	return "", errors.New("key not found")
}

func TestResolveResolvesAPIKeyRef(t *testing.T) {
	ctx := context.Background()
	var captured string
	reg := NewRegistry(func(_ context.Context, ep Endpoint) (model.Model, error) {
		captured = ep.APIKey
		return &keyrefModel{name: "m"}, nil
	})
	reg.SetKeySource(&fakeKeySource{m: map[string]string{"ref1": "sk-from-secret"}})

	if err := reg.Upsert(ctx, Endpoint{ID: "e1", Name: "x", ModelName: "m", APIKeyRef: "ref1"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if _, err := reg.Resolve(ctx, "e1"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if captured != "sk-from-secret" {
		t.Errorf("model built with APIKey %q, want sk-from-secret", captured)
	}
}

func TestResolveAPIKeyRefWithoutSourceErrors(t *testing.T) {
	ctx := context.Background()
	reg := NewRegistry(func(_ context.Context, ep Endpoint) (model.Model, error) {
		return &keyrefModel{name: "m"}, nil
	})
	// no SetKeySource
	if err := reg.Upsert(ctx, Endpoint{ID: "e1", Name: "x", ModelName: "m", APIKeyRef: "ref1"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if _, err := reg.Resolve(ctx, "e1"); err == nil {
		t.Error("Resolve with api_key_ref and no key source must error")
	}
}

func TestResolveLegacyAPIKeyStillWorks(t *testing.T) {
	ctx := context.Background()
	var captured string
	reg := NewRegistry(func(_ context.Context, ep Endpoint) (model.Model, error) {
		captured = ep.APIKey
		return &keyrefModel{name: "m"}, nil
	})
	if err := reg.Upsert(ctx, Endpoint{ID: "e1", Name: "x", ModelName: "m", APIKey: "plain-key"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if _, err := reg.Resolve(ctx, "e1"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if captured != "plain-key" {
		t.Errorf("legacy APIKey = %q, want plain-key", captured)
	}
}
