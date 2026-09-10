// Package e2e contains strict protocol assertions shared by acceptance commands.
package e2e

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const MaxResponseBytes = 1 << 20

type ProjectionKind uint8

const (
	ProjectionFinal ProjectionKind = iota + 1
	ProjectionToolCall
)

type Projection struct {
	Kind    ProjectionKind
	Content string
}

// ReadBody bounds acceptance-command response handling so a broken endpoint
// cannot make an E2E process buffer an unbounded response.
func ReadBody(reader io.Reader) ([]byte, error) {
	if reader == nil {
		return nil, errors.New("response body is required")
	}
	body, err := io.ReadAll(io.LimitReader(reader, MaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxResponseBytes {
		return nil, fmt.Errorf("response body exceeds %d bytes", MaxResponseBytes)
	}
	return body, nil
}

// DecodeChatCompletion accepts a real assistant projection, or an explicit
// tool-call projection when the caller is testing an approval boundary.
func DecodeChatCompletion(body []byte, allowToolCalls bool) (Projection, error) {
	if len(body) == 0 {
		return Projection{}, errors.New("response body is empty")
	}
	if len(body) > MaxResponseBytes {
		return Projection{}, fmt.Errorf("response body exceeds %d bytes", MaxResponseBytes)
	}
	var response struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Role      string            `json:"role"`
				Content   json.RawMessage   `json:"content"`
				ToolCalls []json.RawMessage `json:"tool_calls"`
			} `json:"message"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&response); err != nil {
		return Projection{}, fmt.Errorf("decode chat completion: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Projection{}, errors.New("response contains multiple JSON values")
		}
		return Projection{}, fmt.Errorf("decode trailing response data: %w", err)
	}
	if response.Object != "chat.completion" {
		return Projection{}, fmt.Errorf("unexpected response object %q", response.Object)
	}
	if len(response.Choices) == 0 {
		return Projection{}, errors.New("response choices are empty")
	}
	choice := response.Choices[0]
	if choice.Message.Role != "assistant" {
		return Projection{}, fmt.Errorf("unexpected response role %q", choice.Message.Role)
	}
	if len(choice.Message.Content) != 0 && !bytes.Equal(bytes.TrimSpace(choice.Message.Content), []byte("null")) {
		var content string
		if err := json.Unmarshal(choice.Message.Content, &content); err != nil {
			return Projection{}, fmt.Errorf("decode assistant content: %w", err)
		}
		if strings.TrimSpace(content) != "" {
			return Projection{Kind: ProjectionFinal, Content: content}, nil
		}
	}
	if allowToolCalls && len(choice.Message.ToolCalls) > 0 &&
		choice.FinishReason != nil && *choice.FinishReason == "tool_calls" {
		return Projection{Kind: ProjectionToolCall}, nil
	}
	return Projection{}, errors.New("response has no non-empty assistant projection")
}

func DecodeOpenAIError(body []byte) error {
	if len(body) == 0 {
		return errors.New("error response body is empty")
	}
	if len(body) > MaxResponseBytes {
		return fmt.Errorf("error response body exceeds %d bytes", MaxResponseBytes)
	}
	var response struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&response); err != nil {
		return fmt.Errorf("decode error response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("error response contains multiple JSON values")
		}
		return fmt.Errorf("decode trailing error response data: %w", err)
	}
	if strings.TrimSpace(response.Error.Message) == "" {
		return errors.New("error response message is empty")
	}
	return nil
}
