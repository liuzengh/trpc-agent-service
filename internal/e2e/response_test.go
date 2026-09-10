package e2e

import (
	"bytes"
	"strings"
	"testing"
)

func TestDecodeChatCompletionRequiresProjection(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		allowToolCall bool
		wantKind      ProjectionKind
		wantErr       bool
	}{
		{
			name:     "final assistant",
			body:     `{"object":"chat.completion","choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]}`,
			wantKind: ProjectionFinal,
		},
		{
			name:          "approval tool call",
			body:          `{"object":"chat.completion","choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1"}]},"finish_reason":"tool_calls"}]}`,
			allowToolCall: true,
			wantKind:      ProjectionToolCall,
		},
		{
			name:    "tool call not accepted as final",
			body:    `{"object":"chat.completion","choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1"}]},"finish_reason":"tool_calls"}]}`,
			wantErr: true,
		},
		{
			name:    "null content",
			body:    `{"object":"chat.completion","choices":[{"message":{"role":"assistant","content":null},"finish_reason":"stop"}]}`,
			wantErr: true,
		},
		{
			name:    "empty choices",
			body:    `{"object":"chat.completion","choices":[]}`,
			wantErr: true,
		},
		{
			name:    "malformed JSON",
			body:    `{`,
			wantErr: true,
		},
		{
			name:    "multiple JSON values",
			body:    `{"object":"chat.completion","choices":[]} {}`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeChatCompletion([]byte(tt.body), tt.allowToolCall)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("DecodeChatCompletion() error = nil, got %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeChatCompletion() error = %v", err)
			}
			if got.Kind != tt.wantKind {
				t.Fatalf("projection kind = %d, want %d", got.Kind, tt.wantKind)
			}
		})
	}
}

func TestReadBodyIsBounded(t *testing.T) {
	_, err := ReadBody(bytes.NewReader(bytes.Repeat([]byte("x"), MaxResponseBytes+1)))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReadBody() error = %v, want bounded-body error", err)
	}
}

func TestDecodeOpenAIErrorRequiresMessage(t *testing.T) {
	if err := DecodeOpenAIError([]byte(`{"error":{"message":"execution failed"}}`)); err != nil {
		t.Fatalf("valid error response rejected: %v", err)
	}
	if err := DecodeOpenAIError([]byte(`{"error":{"message":""}}`)); err == nil {
		t.Fatal("empty error response accepted")
	}
}
