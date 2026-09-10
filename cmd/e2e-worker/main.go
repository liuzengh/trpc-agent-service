// Command e2e-worker runs the production Worker/Consumer path with a
// deterministic model at the model boundary. It is test infrastructure, not
// a production runtime mode.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	platformruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	sessionredis "github.com/liuzengh/trpc-agent-service/trpcservice/session/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const (
	postgresDSNEnv       = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	redisURLEnv          = "TRPC_AGENT_SERVICE_REDIS_URL"
	redisStreamEnv       = "TRPC_AGENT_SERVICE_REDIS_STREAM"
	redisGroupEnv        = "TRPC_AGENT_SERVICE_REDIS_GROUP"
	workerIDEnv          = "TRPC_AGENT_SERVICE_WORKER_ID"
	httpAddrEnv          = "TRPC_AGENT_SERVICE_HTTP_ADDR"
	modelTimeoutEnv      = "TRPC_AGENT_SERVICE_MODEL_TIMEOUT"
	workerConcurrencyEnv = "TRPC_AGENT_SERVICE_WORKER_CONCURRENCY"
	redisClaimIdleEnv    = "TRPC_AGENT_SERVICE_REDIS_CLAIM_MIN_IDLE"
	modelModeEnv         = "TRPC_E2E_MODEL_MODE"
	defaultStream        = "trpc-agent-service:dispatch"
	defaultGroup         = "workers"
	defaultAddr          = ":8080"
)

func main() {
	if err := run(); err != nil {
		log.Printf("e2e-worker: %v", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := os.Getenv(postgresDSNEnv)
	redisURL := os.Getenv(redisURLEnv)
	owner := os.Getenv(workerIDEnv)
	if dsn == "" || redisURL == "" || owner == "" {
		return errors.New("TRPC_AGENT_SERVICE_POSTGRES_DSN, TRPC_AGENT_SERVICE_REDIS_URL, and TRPC_AGENT_SERVICE_WORKER_ID are required")
	}
	streamName := os.Getenv(redisStreamEnv)
	if streamName == "" {
		streamName = defaultStream
	}
	group := os.Getenv(redisGroupEnv)
	if group == "" {
		group = defaultGroup
	}
	modelTimeout, err := durationEnv(modelTimeoutEnv, time.Minute)
	if err != nil {
		return err
	}
	concurrency, err := positiveIntEnv(workerConcurrencyEnv, 2)
	if err != nil {
		return err
	}
	claimMinIdle, err := durationEnv(redisClaimIdleEnv, 30*time.Second)
	if err != nil {
		return err
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	store, err := postgres.New(pool)
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}
	redisClient, err := platformredis.NewClient(ctx, redisURL)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer redisClient.Close()
	stream, err := platformredis.NewStream(redisClient, streamName, group, claimMinIdle)
	if err != nil {
		return err
	}
	if err := stream.Init(ctx); err != nil {
		return fmt.Errorf("initialize redis stream: %w", err)
	}

	secretProvider := emptySecretProvider{}
	postgresSessions, err := sessionpostgres.NewSessionResolver(secretProvider, dsn)
	if err != nil {
		return err
	}
	redisSessions, err := sessionredis.NewSessionResolver(secretProvider, redisURL)
	if err != nil {
		_ = postgresSessions.Close()
		return err
	}
	sessions, err := platformsession.NewRouter(postgresSessions, redisSessions)
	if err != nil {
		_ = postgresSessions.Close()
		_ = redisSessions.Close()
		return err
	}
	defer sessions.Close()
	runtimeBuilder, err := platformruntime.NewRuntime(
		deterministicModelResolver{mode: os.Getenv(modelModeEnv)},
		sessions,
		nil,
		nil,
		nil,
		platformruntime.NewToolCatalog(),
	)
	if err != nil {
		return err
	}
	runtimeBuilder.SetExecutionLeaseValidator(store)
	runtimeBuilder.SetObservability(store, store.Metrics())
	locker, err := platformredis.NewSessionLocker(redisClient, 30*time.Second)
	if err != nil {
		return err
	}
	journal, err := postgres.NewExecutionEventJournal(store)
	if err != nil {
		return err
	}
	executor := worker.New(store, runtimeBuilder.BuildRunner, locker, journal, store.IsExecutionCanceled)
	executor.ModelTimeout = modelTimeout
	executor.LeaseValidator = store
	executor.Approvals = store
	executor.Audit = store
	executor.Metrics = store.Metrics()
	consumer, err := worker.NewConsumerWithOptions(executor, stream, store, owner, worker.ConsumerOptions{
		LeaseDuration: 5 * time.Second,
		PollInterval:  50 * time.Millisecond,
		Concurrency:   concurrency,
	})
	if err != nil {
		return err
	}

	address := os.Getenv(httpAddrEnv)
	if address == "" {
		address = defaultAddr
	}
	ready := &atomic.Bool{}
	server := &http.Server{Addr: address, Handler: workerHTTPHandler(ready)}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Serve(listener) }()
	consumerErr := make(chan error, 1)
	go func() { consumerErr <- consumer.Run(ctx) }()
	ready.Store(true)
	log.Printf("e2e-worker ready owner=%s addr=%s", owner, listener.Addr())
	select {
	case err := <-consumerErr:
		ready.Store(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("consumer: %w", err)
		}
	case <-ctx.Done():
		ready.Store(false)
		consumer.StopClaiming()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown http: %w", err)
		}
		if err := <-consumerErr; err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("consumer shutdown: %w", err)
		}
	case err := <-serverErr:
		ready.Store(false)
		cancel()
		consumer.StopClaiming()
		if consumerErr := <-consumerErr; consumerErr != nil && !errors.Is(consumerErr, context.Canceled) {
			return fmt.Errorf("consumer after http shutdown: %w", consumerErr)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
	}
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
	default:
	}
	return nil
}

func workerHTTPHandler(ready *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func positiveIntEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

type emptySecretProvider struct{}

func (emptySecretProvider) ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error) {
	return "", errors.New("e2e worker does not resolve external secrets")
}

var _ platformsecret.SecretProvider = emptySecretProvider{}

type deterministicModelResolver struct{ mode string }

func (r deterministicModelResolver) ResolveModel(ctx context.Context, exec worker.Execution) (platformruntime.ModelRuntime, error) {
	if err := ctx.Err(); err != nil {
		return platformruntime.ModelRuntime{}, err
	}
	return platformruntime.ModelRuntime{
		Model: deterministicModel{
			name:  exec.Config.Model.Model,
			mode:  r.mode,
			calls: &atomic.Int32{},
		},
	}, nil
}

type deterministicModel struct {
	name  string
	mode  string
	calls *atomic.Int32
}

func (m deterministicModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	responses := make(chan *model.Response, 1)
	go func() {
		defer close(responses)
		if m.mode == "block" {
			<-ctx.Done()
			return
		}
		if m.mode == "approval-block-final" && requestHasActualToolResult(request) {
			<-ctx.Done()
			return
		}
		if m.mode == "slow" || m.mode == "fault-slow" {
			delay := 750 * time.Millisecond
			if m.mode == "fault-slow" {
				delay = 10 * time.Second
			}
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
		select {
		case <-ctx.Done():
			return
		case responses <- deterministicResponse(m, request):
		}
	}()
	return responses, nil
}

func deterministicResponse(m deterministicModel, request *model.Request) *model.Response {
	if strings.HasPrefix(m.mode, "approval") && !requestHasDeniedPermission(request) && !requestHasApprovedPermission(request) && !requestHasActualToolResult(request) {
		if m.calls == nil || m.calls.Add(1) == 1 {
			return &model.Response{
				ID:      "e2e-model-tool-call",
				Object:  model.ObjectTypeChatCompletion,
				Model:   m.name,
				Created: 1,
				Done:    true,
				Choices: []model.Choice{
					{
						Index: 0,
						Message: model.Message{
							Role: model.RoleAssistant,
							ToolCalls: []model.ToolCall{
								{
									Type: "function",
									ID:   "e2e-approval-call",
									Function: model.FunctionDefinitionParam{
										Name:      "todo_write",
										Arguments: []byte(`{"todos":[{"content":"approval e2e","activeForm":"Running approval e2e","status":"in_progress"}]}`),
									},
								},
							},
						},
					},
				},
				Usage: &model.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
			}
		}
	}
	return &model.Response{
		ID:      "e2e-model-response",
		Object:  model.ObjectTypeChatCompletion,
		Model:   m.name,
		Created: 1,
		Done:    true,
		Choices: []model.Choice{{Index: 0, Message: model.NewAssistantMessage("deterministic response")}},
		Usage:   &model.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
	}
}

func requestHasActualToolResult(request *model.Request) bool {
	if request == nil {
		return false
	}
	for _, message := range request.Messages {
		if message.Role != model.RoleTool {
			continue
		}
		var permission struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal([]byte(message.Content), &permission); err == nil && permission.Status != "" {
			continue
		}
		return true
	}
	return false
}

func requestHasDeniedPermission(request *model.Request) bool {
	if request == nil {
		return false
	}
	for _, message := range request.Messages {
		if message.Role != model.RoleTool {
			continue
		}
		var permission struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal([]byte(message.Content), &permission); err == nil && permission.Status == "denied" {
			return true
		}
	}
	return false
}

func requestHasApprovedPermission(request *model.Request) bool {
	if request == nil {
		return false
	}
	for _, message := range request.Messages {
		if message.Role != model.RoleTool {
			continue
		}
		var permission struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal([]byte(message.Content), &permission); err == nil && permission.Status == "approved" {
			return true
		}
	}
	return false
}

func (m deterministicModel) Info() model.Info { return model.Info{Name: m.name, ContextWindow: 8192} }

var _ platformruntime.ModelResolver = deterministicModelResolver{}
var _ model.Model = deterministicModel{}
