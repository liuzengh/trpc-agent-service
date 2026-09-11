package assembly

import (
	"context"
	"reflect"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestInputValidationCallbackRejectsHistoricalUnsupportedImage(t *testing.T) {
	catalog, err := config.NewModelCatalog([]config.ModelProviderConfig{{
		ID: "primary", Models: []config.ModelPricingConfig{{
			Name: "text-only", Capabilities: &config.ModelCapabilities{Input: &config.ModelInputCapabilities{}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	callbacks := newInputValidationCallbacks(catalog, config.ModelConfig{ProviderID: "primary", Name: "text-only"})
	message := model.NewUserMessage("new text turn")
	message.ContentParts = append(message.ContentParts, model.ContentPart{Type: model.ContentTypeImage})
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{Request: &model.Request{Messages: []model.Message{message}}}); err == nil {
		t.Fatal("RunBeforeModel() error = nil, want unsupported historical image rejected")
	}
}

func TestInputValidationCallbackAllowsDeclaredImageInput(t *testing.T) {
	catalog, err := config.NewModelCatalog([]config.ModelProviderConfig{{
		ID: "primary", Models: []config.ModelPricingConfig{{
			Name: "vision", Capabilities: &config.ModelCapabilities{Input: &config.ModelInputCapabilities{Image: true}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	callbacks := newInputValidationCallbacks(catalog, config.ModelConfig{ProviderID: "primary", Name: "vision"})
	message := model.NewUserMessage("look")
	message.ContentParts = append(message.ContentParts, model.ContentPart{Type: model.ContentTypeImage})
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{Request: &model.Request{Messages: []model.Message{message}}}); err != nil {
		t.Fatalf("RunBeforeModel() error = %v", err)
	}
}

func TestRequiredRequestInputsClassifiesAndDeduplicatesMultimodalParts(t *testing.T) {
	if got := requiredRequestInputs(nil); got != nil {
		t.Fatalf("requiredRequestInputs(nil) = %v", got)
	}
	message := model.NewUserMessage("inspect attachments")
	message.ContentParts = []model.ContentPart{
		{Type: model.ContentTypeImage},
		{Type: model.ContentTypeImage},
		{Type: model.ContentTypeAudio},
		{Type: model.ContentTypeFile},
		{Type: model.ContentTypeVideo},
		{Type: model.ContentTypeText},
	}
	want := []config.ModelInputKind{config.ModelInputImage, config.ModelInputAudio, config.ModelInputFile}
	if got := requiredRequestInputs(&model.Request{Messages: []model.Message{message}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("requiredRequestInputs() = %v, want %v", got, want)
	}
}

func TestInputValidationCallbackRejectsMissingCatalog(t *testing.T) {
	callbacks := newInputValidationCallbacks(nil, config.ModelConfig{})
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{Request: &model.Request{}}); err == nil {
		t.Fatal("RunBeforeModel() accepted missing catalog")
	}
}
