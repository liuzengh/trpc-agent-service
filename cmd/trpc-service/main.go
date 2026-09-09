package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	embedderopenai "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"

	"github.com/Violet2314/trpc-agent-service/trpcservice"
	"github.com/Violet2314/trpc-agent-service/trpcservice/admin"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels/ilink"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels/webui"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/Violet2314/trpc-agent-service/trpcservice/channels/wecombot"
	"github.com/Violet2314/trpc-agent-service/trpcservice/config"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/Violet2314/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/Violet2314/trpc-agent-service/trpcservice/metrics"
	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/Violet2314/trpc-agent-service/trpcservice/tool"
	"github.com/Violet2314/trpc-agent-service/trpcservice/web"
	"github.com/Violet2314/trpc-agent-service/trpcservice/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		log.Printf("service stopped with error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("trpc-service", flag.ContinueOnError)
	configPath := flags.String("config", os.Getenv("TRPC_SERVICE_CONFIG"), "optional YAML configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load node configuration: %w", err)
	}
	if cfg.OTELEndpoint != "" {
		options := []agenttrace.Option{
			agenttrace.WithServiceName("trpc-agent-service"),
			agenttrace.WithServiceVersion(trpcservice.Version),
		}
		if strings.Contains(cfg.OTELEndpoint, "://") {
			options = append(
				options,
				agenttrace.WithProtocol("http"),
				agenttrace.WithEndpointURL(cfg.OTELEndpoint),
			)
		} else {
			options = append(options, agenttrace.WithEndpoint(cfg.OTELEndpoint))
		}
		cleanTrace, err := agenttrace.Start(context.Background(), options...)
		if err != nil {
			return fmt.Errorf("start OpenTelemetry: %w", err)
		}
		defer func() {
			if err := cleanTrace(); err != nil {
				log.Printf("stop OpenTelemetry: %v", err)
			}
		}()
	}
	telemetryMetrics := platformmetrics.New()

	var (
		adminHandler    http.Handler
		channelRegistry *channels.Registry
		appRouter       *gateway.Router
		ilinkChannel    *ilink.Adapter
		wecomBotChannel *wecombot.Adapter
		auditWriter     platformlog.AuditWriter = platformlog.NopAuditWriter{}
	)
	if cfg.MySQLDSN != "" {
		configStore, err := tenant.OpenMySQLStore(ctx, cfg.MySQLDSN)
		if err != nil {
			return err
		}
		defer configStore.Close()
		if err := storage.ApplyMigrations(ctx, configStore.DB()); err != nil {
			return fmt.Errorf("apply database migrations: %w", err)
		}
		mysqlAuditWriter, err := platformlog.NewMySQLAuditWriter(
			configStore.DB(),
			platformlog.NewRedactor(),
			platformlog.AuditWriterConfig{},
		)
		if err != nil {
			return fmt.Errorf("create audit writer: %w", err)
		}
		defer mysqlAuditWriter.Close()
		auditWriter = mysqlAuditWriter
		cache, err := tenant.NewConfigCache(configStore, cfg.ConfigCacheTTL)
		if err != nil {
			return fmt.Errorf("create config cache: %w", err)
		}
		redisOptions, err := parseRedisOptions(cfg.RedisAddr)
		if err != nil {
			return err
		}
		redisClient := redis.NewClient(redisOptions)
		defer redisClient.Close()
		if err := redisClient.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("ping Redis: %w", err)
		}
		deduper, err := gateway.NewRedisDeduper(
			redisClient, cfg.DedupInflightTTL, cfg.DedupDoneTTL,
		)
		if err != nil {
			return fmt.Errorf("create message deduper: %w", err)
		}
		sessionLock, err := gateway.NewRedisSessionLock(
			redisClient,
			gateway.RedisSessionLockConfig{TTL: cfg.LockTTL},
		)
		if err != nil {
			return fmt.Errorf("create session lock: %w", err)
		}
		eventStore, err := storage.NewMySQLEventStore(configStore.DB())
		if err != nil {
			return fmt.Errorf("create event store: %w", err)
		}
		pgvectorEmbedder, err := buildEmbedder(cfg)
		if err != nil {
			return err
		}
		backends := storage.NewBackendFactory(storage.FactoryConfig{
			RedisURL:    cfg.RedisAddr,
			MySQLDSN:    cfg.MySQLDSN,
			PGVectorDSN: cfg.PGVectorDSN,
			Mem0BaseURL: cfg.Mem0BaseURL,
		}, pgvectorEmbedder)
		defer backends.Close()
		migrationStore, err := storage.NewMySQLMigrationStore(configStore.DB())
		if err != nil {
			return fmt.Errorf("create migration store: %w", err)
		}
		// Replicas that never ran a migration resolve its route from the
		// durable migration table, so all replicas converge.
		backends.SetRouteSource(storage.NewMigrationRouteSource(migrationStore))
		sessionCatalog, err := storage.NewMySQLSessionCatalog(configStore.DB())
		if err != nil {
			return fmt.Errorf("create session catalog: %w", err)
		}
		sessionMigrator, err := storage.NewSessionMigrator(
			migrationStore,
			configStore,
			sessionCatalog,
			migrationLockAdapter{lock: sessionLock},
			backends,
		)
		if err != nil {
			return fmt.Errorf("create Session migrator: %w", err)
		}
		if err := sessionMigrator.Resume(ctx); err != nil {
			return fmt.Errorf("resume Session migrations: %w", err)
		}
		password, err := tenant.ResolveSecret(cfg.AdminPasswordRef)
		if err != nil {
			return fmt.Errorf("resolve admin password: %w", err)
		}
		adminHandler, err = admin.NewHandler(
			configStore,
			cache,
			cfg.AdminUsername,
			password,
			admin.WithMigrator(sessionMigrator),
		)
		if err != nil {
			return fmt.Errorf("create Admin API: %w", err)
		}
		quotaCounter, err := worker.NewRedisQuotaCounter(redisClient)
		if err != nil {
			return fmt.Errorf("create quota counter: %w", err)
		}
		toolRegistry := platformtool.NewRegistry()
		pendingStore, err := worker.NewRedisPendingStore(redisClient, 10*time.Minute)
		if err != nil {
			return fmt.Errorf("create pending confirmation store: %w", err)
		}
		confirmationGate, err := worker.NewRedisConfirmationGate(pendingStore, toolRegistry)
		if err != nil {
			return fmt.Errorf("create dangerous tool confirmation gate: %w", err)
		}
		executor, err := worker.NewExecutor(
			backends,
			worker.OpenAIModelFactory{},
			toolRegistry,
			worker.NewPolicyGovernor(quotaCounter),
			worker.NewPolicyRedactor(),
			worker.WithConfirmationGate(confirmationGate),
			worker.WithMetrics(telemetryMetrics),
		)
		if err != nil {
			return fmt.Errorf("create Worker executor: %w", err)
		}
		channelRegistry = channels.NewRegistry()
		channelRegistry.SetMetrics(telemetryMetrics)
		webHub, err := webui.NewRedisHub(redisClient)
		if err != nil {
			return fmt.Errorf("create WebUI Redis hub: %w", err)
		}
		webAdapter, err := webui.New(
			webHub, web.Handler(), webui.StoreCatalog{Store: configStore},
		)
		if err != nil {
			return fmt.Errorf("create WebUI adapter: %w", err)
		}
		if err := channelRegistry.Register(webAdapter); err != nil {
			return fmt.Errorf("register WebUI adapter: %w", err)
		}
		feishuAdapter, err := feishu.New(cache, nil, "")
		if err != nil {
			return fmt.Errorf("create Feishu adapter: %w", err)
		}
		if err := channelRegistry.Register(feishuAdapter); err != nil {
			return fmt.Errorf("register Feishu adapter: %w", err)
		}
		// Self-built WeCom app callbacks remain registered for protocol
		// completeness. The required WeCom IM is wecombot below; Feishu is
		// the other required real IM.
		wecomAdapter, err := wecom.New(cache, nil, "")
		if err != nil {
			return fmt.Errorf("create WeCom adapter: %w", err)
		}
		if err := channelRegistry.Register(wecomAdapter); err != nil {
			return fmt.Errorf("register WeCom adapter: %w", err)
		}
		if len(cfg.ILinkRouteKeys) > 0 {
			ilinkChannel, err = ilink.New(
				cache, redisClient, nil, "", cfg.ILinkRouteKeys,
			)
			if err != nil {
				return fmt.Errorf("create iLink adapter: %w", err)
			}
			if err := channelRegistry.Register(ilinkChannel); err != nil {
				return fmt.Errorf("register iLink adapter: %w", err)
			}
		}
		if len(cfg.WecomBotRouteKeys) > 0 {
			wecomBotChannel, err = wecombot.New(
				cache, cfg.WecomBotRouteKeys, cfg.WecomBotWSURL,
			)
			if err != nil {
				return fmt.Errorf("create WeCom bot adapter: %w", err)
			}
			if err := channelRegistry.Register(wecomBotChannel); err != nil {
				return fmt.Errorf("register WeCom bot adapter: %w", err)
			}
		}
		appRouter, err = gateway.NewRouter(
			cache,
			deduper,
			sessionLock,
			gateway.NewDebouncer(cfg.Debounce),
			eventStore,
			executor,
			channelRegistry,
			auditWriter,
			gateway.WithMetrics(telemetryMetrics),
		)
		if err != nil {
			return fmt.Errorf("create Agent Gateway: %w", err)
		}
	}

	mux := trpcservice.NewHTTPMux(trpcservice.HTTPOptions{
		AdminHandler:   adminHandler,
		MetricsHandler: telemetryMetrics.Handler(),
	})
	channelCtx, cancelChannels := context.WithCancel(ctx)
	defer func() {
		cancelChannels()
		if ilinkChannel != nil {
			ilinkChannel.Wait()
		}
		if wecomBotChannel != nil {
			wecomBotChannel.Wait()
		}
	}()
	if channelRegistry != nil {
		if err := channelRegistry.Run(channelCtx, mux, appRouter.Handle); err != nil {
			return fmt.Errorf("start channel adapters: %w", err)
		}
	}
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("trpc-agent-service %s listening on %s", trpcservice.Version, cfg.ListenAddr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful HTTP shutdown: %w", err)
	}
	cancelChannels()
	if ilinkChannel != nil {
		ilinkChannel.Wait()
	}
	if wecomBotChannel != nil {
		wecomBotChannel.Wait()
	}
	if appRouter != nil {
		appRouter.Drain()
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP during shutdown: %w", err)
	}
	return nil
}

func parseRedisOptions(address string) (*redis.Options, error) {
	if strings.Contains(address, "://") {
		options, err := redis.ParseURL(address)
		if err != nil {
			return nil, fmt.Errorf("parse Redis URL: %w", err)
		}
		return options, nil
	}
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("Redis address is required")
	}
	return &redis.Options{Addr: address}, nil
}

func buildEmbedder(cfg config.Config) (embedder.Embedder, error) {
	if cfg.PGVectorDSN == "" {
		return nil, nil
	}
	apiKey, err := tenant.ResolveSecret(cfg.EmbeddingKeyRef)
	if err != nil {
		return nil, fmt.Errorf("resolve embedding API key: %w", err)
	}
	options := []embedderopenai.Option{
		embedderopenai.WithModel(cfg.EmbeddingModel),
		embedderopenai.WithAPIKey(apiKey),
		embedderopenai.WithDimensions(cfg.EmbeddingDim),
		embedderopenai.WithMaxRetries(1),
	}
	if cfg.EmbeddingBaseURL != "" {
		options = append(options, embedderopenai.WithBaseURL(cfg.EmbeddingBaseURL))
	}
	return embedderopenai.New(options...), nil
}

type migrationLockAdapter struct {
	lock gateway.SessionLock
}

func (a migrationLockAdapter) WithSessionLock(
	ctx context.Context,
	sessionID string,
	operation func() error,
) error {
	lease, err := a.lock.Acquire(ctx, sessionID)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = lease.Release(releaseCtx)
	}()
	return operation()
}
