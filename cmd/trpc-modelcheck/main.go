package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func main() {
	log.SetOutput(platformlog.NewRedactingWriter(os.Stderr))
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Printf("model check failed: %v", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("trpc-modelcheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	envFile := flags.String("env-file", ".env", "dotenv configuration file")
	timeout := flags.Duration("timeout", 30*time.Second, "model request timeout")
	prompt := flags.String("prompt", "请只回复 OK", "minimal connectivity-check prompt")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if strings.TrimSpace(*prompt) == "" {
		return fmt.Errorf("prompt must not be empty")
	}

	loaded, err := config.LoadDotEnv(*envFile)
	if err != nil {
		return err
	}
	if !loaded && strings.TrimSpace(*envFile) != "" {
		return fmt.Errorf("dotenv file %q does not exist", *envFile)
	}
	modelConfig, err := config.LoadModelConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load model config: %w", err)
	}
	if modelConfig.Provider != config.ModelProviderOpenAI {
		return fmt.Errorf(
			"TRPC_AGENT_MODEL_PROVIDER must be %q for a real-model check",
			config.ModelProviderOpenAI,
		)
	}
	selectedModel, err := agentservice.BuildModel(modelConfig)
	if err != nil {
		return fmt.Errorf("build model: %w", err)
	}
	baseURL := modelConfig.BaseURL
	if baseURL == "" {
		baseURL = "SDK default"
	}
	fmt.Fprintf(
		stdout,
		"checking model provider=%s name=%s base_url=%s\n",
		modelConfig.Provider,
		modelConfig.Name,
		baseURL,
	)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	started := time.Now()
	responses, err := selectedModel.GenerateContent(ctx, model.NewRequest(
		[]model.Message{model.NewUserMessage(strings.TrimSpace(*prompt))},
	))
	if err != nil {
		return fmt.Errorf("request model: %w", err)
	}
	if responses == nil {
		return fmt.Errorf("model returned a nil response channel")
	}

	var reply strings.Builder
	responseModel := selectedModel.Info().Name
	promptTokens := 0
	completionTokens := 0
	for response := range responses {
		if response == nil {
			continue
		}
		if response.Error != nil {
			code := ""
			if response.Error.Code != nil {
				code = *response.Error.Code
			}
			return fmt.Errorf(
				"model API error: type=%s code=%s message=%s",
				response.Error.Type,
				code,
				response.Error.Message,
			)
		}
		if response.Model != "" {
			responseModel = response.Model
		}
		if response.Usage != nil {
			promptTokens = response.Usage.PromptTokens
			completionTokens = response.Usage.CompletionTokens
		}
		for _, choice := range response.Choices {
			if choice.Delta.Content != "" {
				reply.WriteString(choice.Delta.Content)
				continue
			}
			if choice.Message.Content != "" {
				reply.WriteString(choice.Message.Content)
			}
		}
	}
	if err := context.Cause(ctx); err != nil {
		return fmt.Errorf("model request did not complete: %w", err)
	}
	if strings.TrimSpace(reply.String()) == "" {
		return fmt.Errorf("model returned no text content")
	}
	fmt.Fprintf(
		stdout,
		"model check passed: model=%s latency=%s prompt_tokens=%d completion_tokens=%d\nreply: %s\n",
		responseModel,
		time.Since(started).Round(time.Millisecond),
		promptTokens,
		completionTokens,
		strings.TrimSpace(reply.String()),
	)
	return nil
}
