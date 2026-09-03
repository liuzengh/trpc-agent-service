package governance

import (
	"context"
	"encoding/json"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestGuardrailBlocksInputAndRedactsOutput(t *testing.T) {
	callbacks, err := BuildModelCallbacks(json.RawMessage(
		"{\"max_input_chars\":100,\"blocked_input_patterns\":[\"(?i)drop database\"],\"redact_output_patterns\":[\"sk-[A-Za-z0-9]+\"]}",
	))
	if err != nil {
		t.Fatalf("build callbacks: %v", err)
	}
	_, err = callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{
		Request: &model.Request{
			Messages: []model.Message{model.NewUserMessage("please DROP DATABASE now")},
		},
	})
	if err == nil {
		t.Fatal("expected blocked input")
	}
	response := &model.Response{Choices: []model.Choice{{
		Message: model.NewAssistantMessage("secret sk-abcdef"),
	}}}
	redacted, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Request: &model.Request{}, Response: response,
	})
	if err != nil || redacted == nil || redacted.CustomResponse == nil ||
		redacted.CustomResponse.Choices[0].Message.Content != "secret [REDACTED]" {
		t.Fatalf("redacted=%+v err=%v", redacted, err)
	}
}
