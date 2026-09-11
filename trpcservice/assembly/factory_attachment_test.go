package assembly

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type attachmentCapabilityModelProvider struct {
	capabilities config.ModelInputCapabilities
}

func (p attachmentCapabilityModelProvider) Model(context.Context, config.TenantConfig) (model.Model, error) {
	return nil, nil
}

func (p attachmentCapabilityModelProvider) GuaranteedInputCapabilities(config.ModelConfig) (config.ModelInputCapabilities, bool) {
	return p.capabilities, true
}

func TestFactoryRuntimeInstructionStatesActualAttachmentCapabilities(t *testing.T) {
	factory := NewFactoryWithModelProvider(
		attachmentCapabilityModelProvider{capabilities: config.ModelInputCapabilities{Image: true}},
		nil, nil, nil, nil, nil, nil,
		WithDocumentInputSupport(true),
	)
	instruction := factory.runtimeInstruction(config.TenantConfig{
		Instruction: "你是售后助手。",
		Model:       config.ModelConfig{ProviderID: "primary", Name: "vision"},
	})
	for _, want := range []string{"你是售后助手。", "PDF、Word", "Excel（xlsx）", "图片附件", "压缩包"} {
		if !strings.Contains(instruction, want) {
			t.Fatalf("runtime instruction missing %q: %q", want, instruction)
		}
	}
}

func TestFactoryRuntimeInstructionDelegatesConfirmedSideEffectsToToolChain(t *testing.T) {
	factory := NewFactoryWithModelProvider(
		attachmentCapabilityModelProvider{}, nil, nil, nil, nil, nil, nil,
	)
	instruction := factory.runtimeInstruction(config.TenantConfig{
		Instruction: "你是售后助手。",
		Tools:       config.ToolPolicy{RequireConfirmation: []string{"request_refund"}},
	})
	for _, want := range []string{"有副作用工具", "应调用工具", "由平台工具链负责", "不要因为需要确认而拒绝调用"} {
		if !strings.Contains(instruction, want) {
			t.Fatalf("runtime instruction missing %q: %q", want, instruction)
		}
	}
}
