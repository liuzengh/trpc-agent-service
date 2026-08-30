package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Printf("usage: %s\n", os.Args[0])
		return
	}
	ctx, signals, cleanupSignals := installProcessSignals()
	exitCode, err := runService(ctx, signals, os.Stdout, os.Stderr)
	cleanupSignals()
	logLifecycleResult(os.Stderr, err)
	os.Exit(exitCode)
}

func newResponder(mode string) (platform.Responder, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "echo":
		return platform.EchoResponder{}, nil
	case "runner":
		endpoint := strings.TrimSpace(os.Getenv("MODEL_BASE_URL"))
		modelName := strings.TrimSpace(os.Getenv("MODEL_NAME"))
		secret := strings.TrimSpace(os.Getenv("MODEL_API_KEY"))
		bootstrapTenant := env("DEFAULT_TENANT", "demo")
		bootstrapAgent := env("DEFAULT_AGENT_APP_ID", "demo-agent")
		bootstrapRef := env("MODEL_CONFIG_REF", "env")
		version, err := strconv.ParseInt(env("MODEL_CONFIG_VERSION", "1"), 10, 64)
		if err != nil || version < 1 {
			return nil, errors.New("MODEL_CONFIG_VERSION must be a positive integer")
		}
		if endpoint == "" || modelName == "" || secret == "" {
			return nil, errors.New("runner requires MODEL_BASE_URL, MODEL_NAME, and MODEL_API_KEY")
		}
		providerFactory := agent.OpenAIProviderFactory{
			Configs: agent.ModelConfigResolverFunc(func(ctx context.Context, tc tenant.TenantContext, spec agent.AgentSpec) (agent.ModelConfig, error) {
				if err := ctx.Err(); err != nil {
					return agent.ModelConfig{}, err
				}
				if tc.TenantID != bootstrapTenant || tc.AgentAppID != bootstrapAgent || tc.ConfigVersion != version || spec.ModelConfigRef != bootstrapRef {
					return agent.ModelConfig{}, errors.New("environment model config is bound to a different tenant, agent, version, or config ref")
				}
				return agent.ModelConfig{TenantID: bootstrapTenant, AgentAppID: bootstrapAgent, ConfigVersion: version, ConfigRef: bootstrapRef, Provider: spec.ModelProvider, Endpoint: endpoint, Model: modelName, SecretRef: "env:MODEL_API_KEY"}, nil
			}),
			Secrets: agent.SecretResolverFunc(func(ctx context.Context, tc tenant.TenantContext, ref string) (string, error) {
				if err := ctx.Err(); err != nil {
					return "", err
				}
				if tc.TenantID != bootstrapTenant || tc.AgentAppID != bootstrapAgent || ref != "env:MODEL_API_KEY" {
					return "", errors.New("environment model secret is bound to a different tenant, agent, or reference")
				}
				return secret, nil
			}),
		}
		factory, err := agent.NewFactory(agent.RuntimeDependencies{ProviderFactory: providerFactory})
		if err != nil {
			return nil, err
		}
		return platform.RuntimeResponder{Factory: factory}, nil
	default:
		return nil, fmt.Errorf("unsupported MODEL_PROVIDER %q; expected runner or echo", mode)
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
