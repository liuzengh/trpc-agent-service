package agent

import (
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/model"
	openai "trpc.group/trpc-go/trpc-agent-go/model/openai"
)

// BuildModel creates the model selected by application configuration.
func BuildModel(cfg config.ModelConfig) (model.Model, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case config.ModelProviderMock:
		return NewTutorialModel(), nil
	case config.ModelProviderOpenAI:
		if strings.TrimSpace(cfg.Name) == "" {
			return nil, fmt.Errorf("openai model name is required")
		}
		if strings.TrimSpace(cfg.APIKey) == "" {
			return nil, fmt.Errorf("openai API key is required")
		}
		options := []openai.Option{
			openai.WithAPIKey(cfg.APIKey),
		}
		if cfg.BaseURL != "" {
			options = append(options, openai.WithBaseURL(cfg.BaseURL))
		}
		return openai.New(cfg.Name, options...), nil
	default:
		return nil, fmt.Errorf("unsupported model provider %q", cfg.Provider)
	}
}
