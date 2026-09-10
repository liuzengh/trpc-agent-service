package postgres

import (
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

func TestAgentPostgresRevisionCodec(t *testing.T) {
	revision := appmodel.Revision{Generation: appmodel.GenerationConfig{}, Runtime: appmodel.DefaultRuntimePolicy(), Tools: []appmodel.ToolAuthorization{{ToolID: "tool", Required: true}}, Chain: &appmodel.ChainConfiguration{Steps: []appmodel.ChainStep{{Name: "first", Instruction: "one"}, {Name: "second", Instruction: "two"}}}}
	generation, runtime, tools, err := encodeAgentRevisionParts(revision)
	if err != nil {
		t.Fatal(err)
	}
	var decoded appmodel.Revision
	if err := decodeAgentRevisionParts(generation, runtime, &decoded); err != nil || decoded.Runtime.MaxLLMCalls != revision.Runtime.MaxLLMCalls {
		t.Fatalf("revision decode = %+v, err=%v", decoded, err)
	}
	if len(decoded.Chain.Steps) != 2 || decoded.Chain.Steps[1].Name != "second" {
		t.Fatalf("chain decode = %+v", decoded.Chain)
	}
	var decodedTools []appmodel.ToolAuthorization
	if err := decodeJSON(tools, &decodedTools); err != nil || len(decodedTools) != 1 || decodedTools[0].ToolID != "tool" {
		t.Fatalf("tools decode = %+v, err=%v", decodedTools, err)
	}
	if err := decodeAgentRevisionParts([]byte("not-json"), []byte("{}"), &appmodel.Revision{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("malformed generation error = %v", err)
	}
	if err := decodeAgentRevisionParts([]byte("{}"), []byte("not-json"), &appmodel.Revision{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("malformed runtime error = %v", err)
	}
}
