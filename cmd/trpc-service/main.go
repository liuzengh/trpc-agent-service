package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/lifecycle"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

func main() {
	redactor := servicelog.NewRedactor(redactionSecrets(os.Getenv), nil)
	log.SetOutput(servicelog.NewRedactingWriter(os.Stderr, redactor))
	defaultAddr := os.Getenv("TRPC_SERVICE_ADDR")
	if defaultAddr == "" {
		defaultAddr = ":8080"
	}
	addr := flag.String("addr", defaultAddr, "HTTP listen address")
	flag.Parse()

	role := os.Getenv("TRPC_SERVICE_ROLE")
	workerToken := os.Getenv("TRPC_WORKER_TOKEN")
	if workerToken == "" {
		workerToken = "development-worker"
	}
	manifestKeyID, manifestKeys, err := configuredManifestKeys(os.Getenv("TRPC_AUTH_MODE"))
	if err != nil {
		log.Fatal(err)
	}
	governanceToken := os.Getenv("TRPC_GOVERNANCE_TOKEN")
	if governanceToken == "" {
		governanceToken = "development-governance-secret"
	}
	modelFactory, err := configuredAgentFactory(os.Getenv("TRPC_AUTH_MODE"))
	if err != nil {
		log.Fatal(err)
	}
	if role == "worker" {
		worker := platform.NewWorkerServer(platform.WorkerServerConfig{
			Token: workerToken, Factory: modelFactory, ManifestKeys: manifestKeys,
			ToolGovernance: platform.NewRemoteToolGovernance(os.Getenv("TRPC_GOVERNANCE_URL"), governanceToken),
		})
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		mux.Handle("/internal/worker/", worker)
		server := &http.Server{Addr: *addr, Handler: mux}
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			fmt.Printf("trpc-agent-service %s worker listening on %s\n", trpcservice.Version, *addr)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("worker HTTP server: %v", err)
			}
		}()
		<-stop
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("worker HTTP shutdown: %v", err)
		}
		if err := worker.Close(); err != nil {
			log.Printf("worker runtime shutdown: %v", err)
		}
		return
	}
	if role != "" && role != "gateway" {
		log.Fatalf("unsupported service role %q", role)
	}

	life := lifecycle.New()
	var store platform.ControlPlaneStore
	controlPlaneDSN := os.Getenv("TRPC_CONTROL_PLANE_POSTGRES_DSN")
	if controlPlaneDSN != "" {
		store, err = platform.NewPostgresControlPlane(controlPlaneDSN)
	} else {
		controlPlanePath := os.Getenv("TRPC_CONTROL_PLANE_SQLITE_PATH")
		if controlPlanePath == "" {
			controlPlanePath = "data/control-plane.db"
		}
		store, err = platform.NewSQLiteControlPlane(controlPlanePath)
	}
	if err != nil {
		log.Fatalf("control plane unavailable: %v", err)
	}
	identity := platform.DevelopmentIdentity{
		ID: "local-developer", Name: "Local Developer",
		Assignments: []platform.TenantAssignment{
			{TenantID: "tenant-dev", TenantName: "Development Tenant", Role: platform.RolePlatformAdmin},
			{TenantID: "tenant-view", TenantName: "Read-only Tenant", Role: platform.RoleViewer},
		},
	}
	admin := platform.NewAdminHandler(store, identity)
	admin.ConfigureInternalGovernance(governanceToken)
	governancePath := os.Getenv("TRPC_GOVERNANCE_PATH")
	if governancePath == "" {
		governancePath = "data/governance.json"
	}
	governance, err := platform.NewPersistentGovernanceCenter(governancePath)
	if err != nil {
		log.Fatalf("governance state: %v", err)
	}
	if dsn := os.Getenv("TRPC_AUDIT_POSTGRES_DSN"); dsn != "" {
		auditStore, err := platform.NewPostgresStore(dsn)
		if err != nil {
			log.Fatal("audit store unavailable")
		}
		defer auditStore.Close()
		governance.ConfigureAuditStore(auditStore)
	}
	admin.ConfigureGovernance(governance)
	switch authMode := os.Getenv("TRPC_AUTH_MODE"); authMode {
	case "", "development":
	case "production":
		directoryPath := os.Getenv("TRPC_IDENTITY_DIRECTORY")
		directory, err := platform.LoadIdentityDirectory(directoryPath)
		if err != nil {
			log.Fatalf("identity directory: %v", err)
		}
		issuer, audience, secret := os.Getenv("TRPC_AUTH_ISSUER"), os.Getenv("TRPC_AUTH_AUDIENCE"), os.Getenv("TRPC_AUTH_HMAC_SECRET")
		if issuer == "" || audience == "" || secret == "" {
			log.Fatal("production identity requires issuer, audience, and signing secret")
		}
		admin.ConfigureIdentityProvider(platform.NewJWTIdentityProvider(platform.JWTIdentityConfig{Issuer: issuer, Audience: audience, HMACSecret: []byte(secret)}, directory))
	default:
		log.Fatalf("unsupported authentication mode %q", authMode)
	}
	faultInjectionEnabled := true
	if os.Getenv("TRPC_FAULT_INJECTION") == "0" {
		faultInjectionEnabled = false
	}
	if os.Getenv("TRPC_AUTH_MODE") == "production" {
		faultInjectionEnabled = false
	}
	admin.ConfigureFaultInjection(faultInjectionEnabled)
	if err := admin.ConfigureBackendSelections(os.Getenv("TRPC_BACKEND_SELECTIONS")); err != nil && os.Getenv("TRPC_BACKEND_SELECTIONS") != "" {
		log.Fatalf("backend selections: %v", err)
	}
	var runner platform.RunnerAdapter
	workerURL := os.Getenv("TRPC_WORKER_URL")
	if workerURL != "" {
		runner = platform.NewSignedRemoteRunnerAdapter(workerURL, workerToken, manifestKeyID, manifestKeys[manifestKeyID])
	} else {
		runner = platform.NewFrameworkRunnerAdapter(store.DeploymentVersion, modelFactory)
	}
	admin.ConfigureRuntime(runner, life)
	if controlPlaneDSN != "" {
		gatewayID := os.Getenv("TRPC_GATEWAY_ID")
		if gatewayID == "" {
			gatewayID, _ = os.Hostname()
			gatewayID = fmt.Sprintf("%s-%d", gatewayID, os.Getpid())
		}
		leaseTTL, err := configuredDuration("TRPC_SESSION_LEASE_TTL", 30*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		leaseRenewInterval, err := configuredDuration("TRPC_SESSION_LEASE_RENEW_INTERVAL", 10*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		leases, err := platform.NewPostgresSessionLeaseManager(controlPlaneDSN, gatewayID, leaseTTL, leaseRenewInterval)
		if err != nil {
			log.Fatalf("Session Execution Lease store: %v", err)
		}
		admin.ConfigureSessionLeases(leases)
		runCoordinator, err := platform.NewPostgresRunCoordinator(controlPlaneDSN, gatewayID, leaseTTL, leaseRenewInterval)
		if err != nil {
			log.Fatalf("Run Coordinator store: %v", err)
		}
		admin.ConfigureRunCoordinator(runCoordinator)
	}
	admin.ConfigureBackendCatalog(os.Getenv("TRPC_REDIS_ADDR"), os.Getenv("TRPC_SQLITE_PATH"))
	if err := admin.ConfigureBackendProfiles(os.Getenv("TRPC_BACKEND_PROFILES")); err != nil {
		log.Fatal("invalid backend profiles")
	}
	admin.ConfigurePostgresBackend(os.Getenv("TRPC_POSTGRES_DSN"))
	admin.ConfigureMigration(os.Getenv("TRPC_MIGRATION_REDIS_ADDR"), os.Getenv("TRPC_MIGRATION_SQLITE_PATH"), os.Getenv("TRPC_MIGRATION_CHECKPOINT_PATH"))
	var routes *platform.BotTenantAllowlist
	if controlPlaneDSN != "" {
		routes = platform.NewBotTenantAllowlist()
	} else {
		routePath := os.Getenv("TRPC_BOT_ROUTES_PATH")
		if routePath == "" {
			routePath = "data/bot-routes.json"
		}
		routes, err = platform.NewPersistentBotTenantAllowlist(routePath)
		if err != nil {
			log.Fatalf("bot tenant allowlist: %v", err)
		}
	}
	providers := platform.NewProviderRuntime(platform.LoadBotConfig(nil), routes, admin.ProcessProviderMessage)
	admin.ConfigureProviderRuntime(providers)
	providers.Start(context.Background())
	server := &http.Server{Addr: *addr, Handler: web.NewStage1Handler(platform.EchoRunner{}, platform.TenantContext{TenantID: "baseline"}, life, admin)}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		fmt.Printf("trpc-agent-service %s listening on %s\n", trpcservice.Version, *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server: %v", err)
		}
	}()

	<-stop
	life.BeginShutdown()
	providers.Close()
	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(httpCtx); err != nil {
		log.Printf("HTTP admission shutdown: %v", err)
		_ = server.Close()
	}
	cancelHTTP()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	waitErr := life.Wait(waitCtx)
	cancelWait()
	if waitErr != nil {
		log.Printf("active work wait: %v", waitErr)
		life.Cancel()
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
		if err := life.Wait(drainCtx); err != nil {
			log.Printf("cancelled work drain: %v", err)
		}
		cancelDrain()
	} else {
		life.Cancel()
	}
	if err := admin.Close(); err != nil {
		log.Printf("data stores: %v", err)
	}
}

func redactionSecrets(getenv func(string) string) []string {
	names := []string{
		"TRPC_AUTH_HMAC_SECRET", "TRPC_TELEGRAM_BOT_TOKEN", "TRPC_WECOM_BOT_SECRET",
		"TRPC_WORKER_TOKEN", "TRPC_GOVERNANCE_TOKEN", "TRPC_EXECUTION_MANIFEST_SECRET", "TRPC_EXECUTION_MANIFEST_KEYS",
		"OPENAI_API_KEY", "OPENAI_BASE_URL",
		"TRPC_REDIS_ADDR", "TRPC_MIGRATION_REDIS_ADDR", "TRPC_BACKEND_SELECTIONS", "TRPC_BACKEND_PROFILES",
		"TRPC_CONTROL_PLANE_POSTGRES_DSN", "TRPC_AUDIT_POSTGRES_DSN", "TRPC_POSTGRES_DSN",
	}
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, getenv(name))
	}
	return values
}

func configuredDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func configuredManifestKeys(authMode string) (string, map[string][]byte, error) {
	active := strings.TrimSpace(os.Getenv("TRPC_EXECUTION_MANIFEST_ACTIVE_KEY_ID"))
	if active == "" {
		active = "current"
	}
	keys := make(map[string][]byte)
	if raw := strings.TrimSpace(os.Getenv("TRPC_EXECUTION_MANIFEST_KEYS")); raw != "" {
		var encoded map[string]string
		if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
			return "", nil, fmt.Errorf("execution manifest keyring is invalid")
		}
		for id, secret := range encoded {
			if strings.TrimSpace(id) == "" || len(secret) < 8 {
				return "", nil, fmt.Errorf("execution manifest keyring is invalid")
			}
			keys[id] = []byte(secret)
		}
	} else if secret := os.Getenv("TRPC_EXECUTION_MANIFEST_SECRET"); secret != "" {
		keys[active] = []byte(secret)
	} else if authMode != "production" {
		keys[active] = []byte("development-manifest-secret")
	}
	if len(keys[active]) < 8 {
		return "", nil, fmt.Errorf("active execution manifest key is not configured")
	}
	return active, keys, nil
}

func configuredAgentFactory(authMode string) (platform.AgentFactory, error) {
	baseURL, apiKey, modelName := os.Getenv("OPENAI_BASE_URL"), os.Getenv("OPENAI_API_KEY"), os.Getenv("OPENAI_MODEL")
	configured := baseURL != "" && apiKey != "" && modelName != ""
	if configured {
		protocol := platform.ModelProtocolChatCompletions
		if strings.HasPrefix(strings.ToLower(modelName), "gpt-5.6-") {
			protocol = platform.ModelProtocolResponses
		}
		return platform.OpenAICompatibleAgentFactory(platform.ModelProviderProfile{
			ID: "default-openai", BaseURL: baseURL, APIKey: apiKey, Model: modelName, Protocol: protocol,
		}), nil
	}
	if authMode == "production" {
		return nil, fmt.Errorf("production model provider configuration is incomplete")
	}
	return nil, nil
}
