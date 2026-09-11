package privacy

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"strings"
	"testing"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type captureModel struct {
	request *model.Request
	calls   int
}

func (m *captureModel) Info() model.Info { return model.Info{Name: "capture"} }
func (m *captureModel) GenerateContent(_ context.Context, r *model.Request) (<-chan *model.Response, error) {
	m.calls++
	m.request = r
	out := make(chan *model.Response)
	close(out)
	return out, nil
}
func TestPolicyModesAndTenantSecrets(t *testing.T) {
	t.Setenv("PRIVACY_SECRET_A", "tenant-a-key-value")
	tenant := config.TenantConfig{Model: config.ModelConfig{APIKeyEnv: "PRIVACY_SECRET_A"}}
	raw := "mail alice@example.com phone 13800138000 tenant-a-key-value"
	clean, changed, err := Apply("redact", raw, tenant)
	if err != nil || !changed || strings.Contains(clean, "alice") || strings.Contains(clean, "13800138000") || strings.Contains(clean, "tenant-a-key-value") {
		t.Fatalf("redact: %q %v", clean, err)
	}
	if _, _, err := Apply("block", raw, tenant); !errors.Is(err, ErrBlocked) {
		t.Fatalf("block: %v", err)
	}
	if clean, changed, err := Apply("", raw, tenant); err != nil || changed || clean != raw {
		t.Fatal("legacy mode changed")
	}
	if clean, changed, err := Apply("redact", "tenant-a-key-value", config.TenantConfig{}); err != nil || changed || clean != "tenant-a-key-value" {
		t.Fatal("another tenant inherited secret values")
	}
}
func TestModelPrivacyCoversHistoryToolArgumentsAndMultipartWithoutMutation(t *testing.T) {
	tenant := config.TenantConfig{Privacy: config.PrivacyPolicy{Input: "redact"}}
	next := &captureModel{}
	wrapped := WrapModel(next, tenant)
	part := "contact alice@example.com"
	req := &model.Request{Messages: []model.Message{
		{Role: model.RoleSystem, Content: "contact alice@example.com"},
		{Role: model.RoleTool, Content: "phone 13800138000"},
		{Role: model.RoleAssistant, ReasoningContent: "alice@example.com", ToolCalls: []model.ToolCall{{Function: model.FunctionDefinitionParam{Arguments: []byte(`{"phone":13800138000,"large":9007199254740993,"contact":"alice@example.com"}`)}}}},
		{Role: model.RoleUser, ContentParts: []model.ContentPart{{Type: model.ContentTypeText, Text: &part}}},
	}}
	original, _ := json.Marshal(req)
	if _, err := wrapped.GenerateContent(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(next.request)
	if strings.Contains(string(encoded), "alice@example.com") || strings.Contains(string(encoded), "13800138000") {
		t.Fatal("sensitive history reached model")
	}
	if !strings.Contains(string(encoded), "9007199254740993") {
		t.Fatal("JSON integer precision changed")
	}
	after, _ := json.Marshal(req)
	if string(original) != string(after) {
		t.Fatal("mutated shared history")
	}
	tenant.Privacy.Input = "block"
	next = &captureModel{}
	if _, err := WrapModel(next, tenant).GenerateContent(context.Background(), req); !errors.Is(err, ErrBlocked) || next.calls != 0 {
		t.Fatalf("blocked history dispatched: %v", err)
	}
}
func TestModelPrivacyRejectsUninspectedMediaAndInvalidArguments(t *testing.T) {
	for _, message := range []model.Message{
		{ContentParts: []model.ContentPart{{Type: model.ContentTypeImage}}},
		{ToolCalls: []model.ToolCall{{Function: model.FunctionDefinitionParam{Arguments: []byte(`{"x":1} {"y":2}`)}}}},
	} {
		next := &captureModel{}
		_, err := WrapModel(next, config.TenantConfig{Privacy: config.PrivacyPolicy{Input: "redact"}}).GenerateContent(context.Background(), &model.Request{Messages: []model.Message{message}})
		if !errors.Is(err, ErrBlocked) || next.calls != 0 {
			t.Fatalf("uninspected content dispatched: %v", err)
		}
	}
}
