package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Printf("usage: %s\n", os.Args[0])
		return
	}
	store := platform.NewMemoryStore()
	_ = store.SaveTenant(context.Background(), platform.Tenant{ID: env("DEFAULT_TENANT", "demo"), Name: "Demo tenant", Agent: platform.AgentConfig{Name: "demo-agent", Model: env("MODEL", "echo")}, Backend: platform.BackendConfig{Session: "memory", Memory: "memory", Vector: "none"}})
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
	server := web.NewServer(store, platform.Runner{Store: store, Responder: platform.EchoResponder{}})
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

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
