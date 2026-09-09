package agent

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestDeterministicToolModelHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	modelStub := &deterministicToolModel{name: "test-model", toolName: "deploy"}
	if _, err := modelStub.GenerateContent(ctx, &model.Request{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateContent error = %v, want context.Canceled", err)
	}
}
