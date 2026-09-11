package governance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

type GuardrailConfig struct {
	MaxInputChars        int      `json:"max_input_chars"`
	BlockedInputPatterns []string `json:"blocked_input_patterns"`
	RedactOutputPatterns []string `json:"redact_output_patterns"`
}

func BuildModelCallbacks(raw json.RawMessage) (*model.Callbacks, error) {
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config GuardrailConfig
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("decode guardrail config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("guardrail config must contain one JSON value")
	}
	if config.MaxInputChars < 0 {
		return nil, errors.New("max_input_chars must not be negative")
	}
	blocked, err := compilePatterns(config.BlockedInputPatterns)
	if err != nil {
		return nil, fmt.Errorf("blocked input pattern: %w", err)
	}
	redactors, err := compilePatterns(config.RedactOutputPatterns)
	if err != nil {
		return nil, fmt.Errorf("output redaction pattern: %w", err)
	}
	if config.MaxInputChars == 0 && len(blocked) == 0 && len(redactors) == 0 {
		return nil, nil
	}
	callbacks := model.NewCallbacks()
	callbacks.RegisterBeforeModel(func(
		_ context.Context,
		args *model.BeforeModelArgs,
	) (*model.BeforeModelResult, error) {
		if args == nil || args.Request == nil {
			return &model.BeforeModelResult{}, nil
		}
		for _, message := range args.Request.Messages {
			if message.Role != model.RoleUser {
				continue
			}
			if config.MaxInputChars > 0 && len([]rune(message.Content)) > config.MaxInputChars {
				return nil, errors.New("guardrail rejected input length")
			}
			for _, pattern := range blocked {
				if pattern.MatchString(message.Content) {
					return nil, errors.New("guardrail rejected input content")
				}
			}
		}
		return &model.BeforeModelResult{}, nil
	})
	callbacks.RegisterAfterModel(func(
		_ context.Context,
		args *model.AfterModelArgs,
	) (*model.AfterModelResult, error) {
		if args == nil || args.Response == nil || len(redactors) == 0 {
			return &model.AfterModelResult{}, nil
		}
		response := args.Response.Clone()
		for index := range response.Choices {
			response.Choices[index].Message.Content = redactContent(
				response.Choices[index].Message.Content, redactors,
			)
			response.Choices[index].Delta.Content = redactContent(
				response.Choices[index].Delta.Content, redactors,
			)
		}
		return &model.AfterModelResult{CustomResponse: response}, nil
	})
	return callbacks, nil
}

func compilePatterns(values []string) ([]*regexp.Regexp, error) {
	result := make([]*regexp.Regexp, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		pattern, err := regexp.Compile(value)
		if err != nil {
			return nil, err
		}
		result = append(result, pattern)
	}
	return result, nil
}

func redactContent(value string, patterns []*regexp.Regexp) string {
	for _, pattern := range patterns {
		value = pattern.ReplaceAllString(value, "[REDACTED]")
	}
	return value
}
