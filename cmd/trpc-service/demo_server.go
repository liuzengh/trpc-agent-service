package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider/modelclient"
	providerpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type demoServerConfig struct {
	PostgresDSN, ListenAddress string
}

func loadDemoServerConfig(getenv func(string) string) (demoServerConfig, error) {
	if getenv == nil {
		return demoServerConfig{}, errors.New("environment reader is required")
	}
	value := demoServerConfig{PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), ListenAddress: strings.TrimSpace(getenv("TRPC_LISTEN_ADDRESS"))}
	if value.ListenAddress == "" {
		value.ListenAddress = ":8080"
	}
	if value.PostgresDSN == "" || value.ListenAddress == "" {
		return demoServerConfig{}, errors.New("TRPC_POSTGRES_DSN is required for demo-server")
	}
	return value, nil
}

// runDemoServer deliberately exposes only the deterministic fake-model
// acceptance surface. It is not a production gateway or a substitute for the
// durable Worker/IM execution path.
func runDemoServer(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || getenv == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadDemoServerConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	db, err := sql.Open("pgx", configValue.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	if err := db.PingContext(parent); err != nil {
		return errors.New("postgres unavailable")
	}
	runner := migrations.NewRunner(db)
	if err := runner.Ready(parent); err != nil {
		return fmt.Errorf("schema is not ready: %w", err)
	}
	catalog, err := provider.NewCatalog(provider.FakeModelSchema(), provider.FakeEmbeddingSchema(), provider.PostgresBackendSchema(), provider.PostgresBackendSchemaV2())
	if err != nil {
		return errors.New("provider catalog initialization failed")
	}
	models := modelclient.Resolver{Profiles: providerpostgres.New(db, catalog)}
	handler := newDemoHTTPHandler(models, func(ctx context.Context) error {
		if err := db.PingContext(ctx); err != nil {
			return err
		}
		return runner.Ready(ctx)
	})
	server := &http.Server{Addr: configValue.ListenAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errorsCh := make(chan error, 1)
	go func() {
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- serveErr
		}
	}()
	logger.Printf("demo server ready: http://localhost%s/v1/chat provider=fake", configValue.ListenAddress)
	select {
	case <-parent.Done():
	case serveErr := <-errorsCh:
		return fmt.Errorf("demo HTTP server stopped: %w", serveErr)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

type demoModelResolver interface {
	ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error)
}

type demoChatRequest struct {
	Message string `json:"message"`
	Stream  bool   `json:"stream"`
}

func newDemoHTTPHandler(models demoModelResolver, ready func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if ready == nil || ready(request.Context()) != nil {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/chat", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if models == nil {
			http.Error(writer, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		var input demoChatRequest
		body := http.MaxBytesReader(writer, request.Body, 64<<10)
		defer body.Close()
		decoder := json.NewDecoder(body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || strings.TrimSpace(input.Message) == "" {
			http.Error(writer, "invalid chat request", http.StatusBadRequest)
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			http.Error(writer, "invalid chat request", http.StatusBadRequest)
			return
		}
		resolved, err := models.ResolveModel(request.Context(), demoTenantID, profile.VersionedRef{ID: demoModelProfileID, Version: demoProfileVersion})
		if err != nil {
			http.Error(writer, "demo model unavailable", http.StatusServiceUnavailable)
			return
		}
		responses, err := resolved.GenerateContent(request.Context(), &model.Request{Messages: []model.Message{model.NewUserMessage(input.Message)},
			GenerationConfig: model.GenerationConfig{Stream: input.Stream}})
		if err != nil {
			http.Error(writer, "demo model failed", http.StatusBadGateway)
			return
		}
		if input.Stream {
			writeDemoChatStream(writer, responses)
			return
		}
		var text strings.Builder
		var responseID, modelName string
		for response := range responses {
			if response == nil {
				continue
			}
			responseID, modelName = response.ID, response.Model
			for _, choice := range response.Choices {
				if choice.Delta.Content != "" {
					text.WriteString(choice.Delta.Content)
				} else if choice.Message.Content != "" {
					text.WriteString(choice.Message.Content)
				}
			}
		}
		if text.Len() == 0 || responseID == "" || modelName == "" {
			http.Error(writer, "demo model returned no content", http.StatusBadGateway)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(writer).Encode(map[string]string{"id": responseID, "model": modelName, "response": text.String()})
	})
	return mux
}

// writeDemoChatStream keeps the demo transport deliberately small while
// exercising the exact Model streaming contract used by the fake provider.
// Each model delta is an independent SSE event so curl and browser clients can
// verify incremental delivery without relying on an upstream model API.
func writeDemoChatStream(writer http.ResponseWriter, responses <-chan *model.Response) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	for response := range responses {
		if response == nil {
			continue
		}
		for _, choice := range response.Choices {
			delta := choice.Delta.Content
			if delta == "" {
				delta = choice.Message.Content
			}
			if delta == "" {
				continue
			}
			encoded, err := json.Marshal(struct {
				ID    string `json:"id"`
				Model string `json:"model"`
				Delta string `json:"delta"`
				Done  bool   `json:"done"`
			}{ID: response.ID, Model: response.Model, Delta: delta, Done: response.Done})
			if err != nil {
				http.Error(writer, "demo stream encoding failed", http.StatusInternalServerError)
				return
			}
			if _, err := fmt.Fprintf(writer, "event: delta\ndata: %s\n\n", encoded); err != nil {
				return
			}
			flusher.Flush()
		}
	}
	if _, err := io.WriteString(writer, "event: done\ndata: {}\n\n"); err == nil {
		flusher.Flush()
	}
}
