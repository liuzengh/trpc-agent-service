package assembly

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type inputGuardProbeModel struct{ calls int }

func (m *inputGuardProbeModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	m.calls++
	ch := make(chan *model.Response)
	close(ch)
	return ch, nil
}

func (*inputGuardProbeModel) Info() model.Info { return model.Info{Name: "probe"} }

func TestInputValidatingModelRejectsHistoricalUnsupportedImageBeforeProvider(t *testing.T) {
	catalog, err := config.NewModelCatalog([]config.ModelProviderConfig{{
		ID: "primary", Models: []config.ModelPricingConfig{{
			Name: "text-only", Capabilities: &config.ModelCapabilities{Input: &config.ModelInputCapabilities{}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	probe := &inputGuardProbeModel{}
	guarded := newInputValidatingModel(probe, catalog, config.ModelConfig{ProviderID: "primary", Name: "text-only"})
	message := model.NewUserMessage("new text turn")
	message.ContentParts = append(message.ContentParts, model.ContentPart{Type: model.ContentTypeImage})
	if _, err := guarded.GenerateContent(context.Background(), &model.Request{Messages: []model.Message{message}}); err == nil {
		t.Fatal("GenerateContent() error = nil, want unsupported historical image rejected")
	}
	if probe.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", probe.calls)
	}
}

func TestInputValidatingModelAllowsDeclaredImageInput(t *testing.T) {
	catalog, err := config.NewModelCatalog([]config.ModelProviderConfig{{
		ID: "primary", Models: []config.ModelPricingConfig{{
			Name: "vision", Capabilities: &config.ModelCapabilities{Input: &config.ModelInputCapabilities{Image: true}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	probe := &inputGuardProbeModel{}
	guarded := newInputValidatingModel(probe, catalog, config.ModelConfig{ProviderID: "primary", Name: "vision"})
	message := model.NewUserMessage("look")
	message.ContentParts = append(message.ContentParts, model.ContentPart{Type: model.ContentTypeImage})
	if _, err := guarded.GenerateContent(context.Background(), &model.Request{Messages: []model.Message{message}}); err != nil {
		t.Fatalf("GenerateContent() error = %v", err)
	}
	if probe.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", probe.calls)
	}
}
