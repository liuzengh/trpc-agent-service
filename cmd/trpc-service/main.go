package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Printf("usage: %s\n", os.Args[0])
		return
	}
	store := platform.NewMemoryStore()
	_ = store.SaveTenant(context.Background(), platform.Tenant{ID: env("DEFAULT_TENANT", "demo"), Name: "Demo tenant", Agent: platform.AgentConfig{Name: env("DEFAULT_AGENT_APP_ID", "demo-agent"), Model: env("MODEL", "openai"), ModelConfigRef: env("MODEL_CONFIG_REF", "env"), ToolPolicyRef: env("TOOL_POLICY_REF", "default")}, Backend: platform.BackendConfig{Session: "memory", Memory: "memory", Vector: "none"}})
	var migrator pgstore.Migrator
	var readiness web.ReadinessGate
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		var err error
		migrator, err = pgstore.NewMigrator(context.Background(), pgstore.PostgresConfig{URL: databaseURL, SearchPath: os.Getenv("DATABASE_SCHEMA")}, os.DirFS(filepath.Clean(env("MIGRATIONS_DIR", "migrations"))))
		if err != nil {
			fmt.Fprintf(os.Stderr, "startup blocked: migration initialization failed (%v)\n", err)
			os.Exit(1)
		}
		gate := pgstore.NewMigrationReadiness(migrator)
		if err := gate.Initialize(context.Background()); err != nil {
			migrator.Close()
			fmt.Fprintf(os.Stderr, "startup blocked: migration initialization failed (%v)\n", err)
			os.Exit(1)
		}
		readiness = gate
	}
	responder, err := newResponder(env("MODEL_PROVIDER", "runner"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "startup blocked: responder initialization failed (%v)\n", err)
		if migrator != nil {
			migrator.Close()
		}
		os.Exit(1)
	}
	server := web.NewServer(store, platform.Runner{Store: store, Responder: responder})
	server.Readiness = readiness
	srv := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()
	fmt.Printf("trpc-agent-service %s listening on %s\n", trpcservice.Version, srv.Addr)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	if migrator != nil {
		migrator.Close()
	}
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
