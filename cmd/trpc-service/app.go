package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledgeingest"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/node"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/webui"
)

type environment func(string) string

type tenantOutboxDispatcher interface {
	DispatchTenant(context.Context, string, int) (int, error)
}

type application struct {
	role                     serviceRole
	listenAddress            string
	requestTimeout           time.Duration
	sessionIdleArchiveAge    time.Duration
	handler                  http.Handler
	worker                   *messaging.Worker
	knowledgeIngestWorker    *knowledgeingest.Worker
	channelConnectors        *channelConnectorManager
	replyOutbox              tenantOutboxDispatcher
	configInvalidationOutbox *tenant.ConfigInvalidationDispatcher
	configurations           tenant.Repository
	auditRetention           storage.AuditRetentionStore
	sessionArchiver          storage.IdleSessionArchiver
	nodeLifecycle            *node.Lifecycle
	closeFuncs               []func()
}

func runService() error {
	slog.SetDefault(slog.New(platformlog.NewRedactingHandler(slog.NewJSONHandler(os.Stderr, nil))))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := buildApplication(ctx, os.Getenv)
	if err != nil {
		return err
	}
	defer app.Close()
	return app.Run(ctx)
}

func buildApplication(ctx context.Context, getenv environment) (*application, error) {
	role, err := serviceRoleFromEnvironment(getenv)
	if err != nil {
		return nil, err
	}
	serviceConfig, err := loadServiceConfig(getenv)
	if err != nil {
		return nil, err
	}
	resources, err := composeInfrastructure(ctx, serviceConfig, getenv, role)
	if err != nil {
		return nil, err
	}
	syncModels := func(ctx context.Context) ([]web.ModelProviderInfo, error) {
		providers := resources.modelCatalog.Providers()
		infos := make([]web.ModelProviderInfo, 0, len(providers))
		for _, provider := range providers {
			configured, syncErr := resources.modelProvider.SyncProvider(ctx, http.DefaultClient, provider.ID)
			syncError := ""
			if syncErr != nil {
				slog.Warn("model provider sync failed", "provider", provider.ID, "error", syncErr)
				if configured {
					syncError = "无法获取模型列表"
				} else {
					syncError = "凭据不可用"
				}
			}
			modelSet := make(map[string]web.ModelInfo)
			for _, m := range provider.Models {
				if name := strings.TrimSpace(m.Name); name != "" {
					modelSet[name] = web.ModelInfo{
						Name: name, Capabilities: m.Capabilities, Source: "configured",
						Pricing: &web.ModelPricingInfo{
							PromptMicros:       m.PromptCostMicrosPerMillionTokens,
							CachedPromptMicros: m.CachedPromptCostMicrosPerMillionTokens,
							CompletionMicros:   m.CompletionCostMicrosPerMillionTokens,
						},
					}
				}
			}
			for _, name := range resources.modelCatalog.ListDiscoveredModels(provider.ID) {
				if _, exists := modelSet[name]; !exists {
					modelSet[name] = web.ModelInfo{Name: name, Source: "discovered"}
				}
			}

			modelNames := make([]string, 0, len(modelSet))
			for name := range modelSet {
				modelNames = append(modelNames, name)
			}
			sort.Strings(modelNames)
			models := make([]web.ModelInfo, 0, len(modelNames))
			for _, name := range modelNames {
				models = append(models, modelSet[name])
			}

			infos = append(infos, web.ModelProviderInfo{
				ID:             provider.ID,
				Type:           provider.NormalizedType(),
				BaseURL:        provider.BaseURL,
				CredentialRefs: modelCredentialRefs(provider),
				Configured:     configured,
				Models:         models,
				SyncError:      syncError,
			})
		}
		return infos, nil
	}
	removeModel := func(_ context.Context, providerID, modelName string) error {
		resources.modelCatalog.RemoveDiscoveredModel(providerID, modelName)
		return nil
	}

	initialModelProviders, _ := syncModels(ctx)

	var handler http.Handler
	if role.runsGateway() {
		handler, err = NewKafkaHTTPHandler(KafkaHTTPDependencies{
			Observer:                resources.observer,
			Configurations:          resources.repository,
			Producer:                resources.producer,
			ExecutionManifests:      resources.executionManifests,
			StateStore:              resources.stateStore,
			ExecutionDedup:          resources.executionDedup,
			RetryTracker:            resources.retryTracker,
			ToolExecutions:          resources.toolExecutions,
			WebIdempotency:          resources.webIdempotency,
			KnowledgeIngest:         resources.knowledgeIngestQueue,
			KnowledgeMigrationStore: resources.knowledgeMigrationStore,
			KnowledgeMigrator:       resources.knowledgeMigrator,
			BackendProfiles:         resources.backendProfiles,
			ConsoleSystem: web.SystemInfo{
				Version:               trpcservice.Version,
				ListenAddress:         serviceConfig.Service.ListenAddress,
				KafkaTopic:            getenv("KAFKA_TOPIC"),
				KafkaBrokers:          getenv("KAFKA_BROKERS"),
				RedisAddress:          getenv("REDIS_ADDR"),
				ModelProviders:        initialModelProviders,
				ChannelCredentialRefs: append([]string(nil), serviceConfig.Service.ChannelCredentialRefs...),
				ToolCredentialRefs:    append([]string(nil), serviceConfig.Service.ToolCredentialRefs...),
			},
			ChannelStatuses:       resources.channelConnectors,
			ConsoleProbes:         resources.probes,
			Nodes:                 resources.nodes,
			ConsoleFS:             webui.FS(),
			AuthHandler:           resources.authHandler,
			Sessions:              resources.sessions,
			Identities:            resources.identities,
			Audits:                resources.authAudits,
			WebReplySubscriber:    resources.webReplyHub,
			Knowledge:             resources.platformStores,
			KnowledgeSourcePolicy: resources.knowledgeSourcePolicy,
			AgentMemory:           resources.agentMemory,
			ArtifactServices:      resources.platformStores,
			AgentSessions:         resources.agentSessions,
			SessionMigrationStore: resources.sessionMigrationStore,
			SessionMigrator:       resources.sessionMigrator,
			ApplicationValidator:  resources.applicationValidator,
			ToolCatalog:           resources.toolCatalog,
			ModelSyncer:           syncModels,
			ModelRemover:          removeModel,
		})
		if err != nil {
			resources.Close()
			return nil, err
		}
	} else {
		handler = NewWorkerHTTPHandler(resources.observer, resources.probes)
	}
	handler = mountPrometheus(handler, resources.metricsHandler)
	return &application{
		role:                     role,
		listenAddress:            serviceConfig.Service.ListenAddress,
		replyOutbox:              resources.replyOutbox,
		configInvalidationOutbox: resources.configInvalidationOutbox,
		configurations:           resources.repository,
		auditRetention: func() storage.AuditRetentionStore {
			retention, _ := resources.stateStore.(storage.AuditRetentionStore)
			return retention
		}(),
		sessionArchiver: func() storage.IdleSessionArchiver {
			archiver, _ := resources.stateStore.(storage.IdleSessionArchiver)
			return archiver
		}(),
		sessionIdleArchiveAge: serviceConfig.Service.SessionIdleArchiveAge.Duration,
		requestTimeout:        serviceConfig.Service.RequestTimeout.Duration,
		handler:               handler,
		worker:                resources.worker,
		knowledgeIngestWorker: resources.knowledgeIngestWorker,
		channelConnectors:     resources.channelConnectors,
		nodeLifecycle:         resources.nodeLifecycle,
		closeFuncs:            resources.closeFuncs,
	}, nil
}

func modelCredentialRefs(provider config.ModelProviderConfig) []string {
	refs := make([]string, 0, 3)
	seen := make(map[string]struct{}, 3)
	for _, reference := range []string{provider.APIKeyRef, provider.SecretIDRef, provider.SecretKeyRef} {
		reference = strings.TrimSpace(reference)
		if reference == "" {
			continue
		}
		if _, exists := seen[reference]; exists {
			continue
		}
		seen[reference] = struct{}{}
		refs = append(refs, reference)
	}
	return refs
}

func loadServiceConfig(getenv environment) (config.Config, error) {
	configPath, err := required(getenv, "CONFIG_PATH")
	if err != nil {
		return config.Config{}, err
	}
	file, err := os.Open(configPath)
	if err != nil {
		return config.Config{}, fmt.Errorf("open CONFIG_PATH: %w", err)
	}
	serviceConfig, loadErr := config.Load(file)
	closeErr := file.Close()
	if loadErr != nil {
		return config.Config{}, loadErr
	}
	if closeErr != nil {
		return config.Config{}, fmt.Errorf("close CONFIG_PATH: %w", closeErr)
	}
	return serviceConfig, nil
}

func (a *application) Run(ctx context.Context) error {
	if a == nil || a.handler == nil || strings.TrimSpace(a.listenAddress) == "" {
		return fmt.Errorf("application is not fully configured")
	}
	role := a.role
	if role == "" {
		role = roleAll
	}
	if role.runsWorker() && a.worker == nil {
		return fmt.Errorf("application is not fully configured")
	}
	if a.nodeLifecycle != nil {
		if err := a.nodeLifecycle.Start(context.Background()); err != nil {
			return fmt.Errorf("start node lifecycle: %w", err)
		}
		defer func() { _ = a.nodeLifecycle.Close() }()
	}
	listener, err := net.Listen("tcp", a.listenAddress)
	if err != nil {
		return fmt.Errorf("listen HTTP: %w", err)
	}
	serverCtx, stopServer := context.WithCancel(context.Background())
	workerCtx, stopWorker := context.WithCancel(context.Background())
	backgroundCtx, stopBackground := context.WithCancel(context.Background())
	defer stopServer()
	defer stopWorker()
	defer stopBackground()
	server := &http.Server{Handler: a.handler, ReadHeaderTimeout: a.requestTimeout}
	workerErrors := make(chan error, 1)
	workerDone := make(chan struct{})
	var background sync.WaitGroup
	startBackground := func(run func(context.Context)) {
		background.Add(1)
		go func() {
			defer background.Done()
			run(backgroundCtx)
		}()
	}
	if role.runsWorker() {
		go func() {
			defer close(workerDone)
			retry := newWorkerRetryBackoff()
			for {
				err := a.worker.RunOnce(workerCtx)
				if err == nil {
					retry.Reset()
					continue
				}
				if errors.Is(err, messaging.ErrWorkerDraining) {
					return
				}
				if errors.Is(err, messaging.ErrRetryScheduled) {
					if !waitRetryDelay(workerCtx, retry) {
						return
					}
					continue
				}
				if workerCtx.Err() != nil {
					return
				}
				workerErrors <- err
				return
			}
		}()
		if a.knowledgeIngestWorker != nil {
			startBackground(a.knowledgeIngestWorker.Run)
		}
	} else {
		close(workerDone)
	}
	if role.runsChannel() {
		if a.channelConnectors != nil {
			startBackground(a.channelConnectors.Run)
		}
	}
	if role.runsGateway() || role.runsChannel() {
		if a.replyOutbox != nil {
			startBackground(a.dispatchReplyOutbox)
		}
	}
	if role.runsGateway() {
		if a.configInvalidationOutbox != nil && a.configurations != nil {
			startBackground(a.dispatchConfigInvalidationOutbox)
		}
		if a.auditRetention != nil && a.configurations != nil {
			startBackground(a.enforceAuditRetention)
		}
		if a.sessionArchiver != nil {
			startBackground(a.archiveIdleSessions)
		}
	}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- ServeHTTPServer(serverCtx, server, listener) }()
	var workerErrorInput <-chan error
	if role.runsWorker() {
		workerErrorInput = workerErrors
	}
	var result error
	serverStopped := false
	select {
	case err := <-serverErrors:
		result = err
		serverStopped = true
	case err := <-workerErrorInput:
		result = fmt.Errorf("Kafka worker stopped: %w", err)
	case <-ctx.Done():
	}

	// Mark the process non-ready before stopping ingress or Kafka admission.
	// The Worker then drains only deliveries that
	// already crossed its admission point; Kafka polling is cancelled only after
	// those deliveries have completed their commit/DLQ handoff.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if a.nodeLifecycle != nil {
		if err := a.nodeLifecycle.BeginDrain(shutdownCtx); err != nil && result == nil {
			result = fmt.Errorf("mark node draining: %w", err)
		}
	}
	stopServer()
	if role.runsWorker() {
		a.worker.BeginDrain()
		if err := a.worker.WaitDrain(shutdownCtx); err != nil && result == nil {
			result = fmt.Errorf("drain Kafka worker: %w", err)
		}
	}
	stopWorker()
	stopBackground()
	if !serverStopped {
		select {
		case serverErr := <-serverErrors:
			if serverErr != nil && result == nil {
				result = serverErr
			}
		case <-shutdownCtx.Done():
			return fmt.Errorf("%w: HTTP server did not stop", result)
		}
	}
	select {
	case <-workerDone:
	case <-shutdownCtx.Done():
		return fmt.Errorf("%w: Kafka worker did not stop", result)
	}
	backgroundDone := make(chan struct{})
	go func() {
		background.Wait()
		close(backgroundDone)
	}()
	select {
	case <-backgroundDone:
	case <-shutdownCtx.Done():
		return fmt.Errorf("%w: background tasks did not stop", result)
	}
	return result
}

func (a *application) enforceAuditRetention(ctx context.Context) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		if err := a.enforceAuditRetentionOnce(ctx, time.Now().UTC()); err != nil {
			logBackgroundError("audit retention", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *application) enforceAuditRetentionOnce(ctx context.Context, now time.Time) error {
	snapshots, err := a.configurations.ListApplications(ctx, "")
	if err != nil {
		return fmt.Errorf("list applications for audit retention: %w", err)
	}
	daysByTenant := make(map[string]int)
	for _, snapshot := range snapshots {
		days := snapshot.Config.Audit.RetentionDays
		if days <= 0 {
			continue
		}
		current, exists := daysByTenant[snapshot.Config.TenantID]
		if !exists || days < current {
			daysByTenant[snapshot.Config.TenantID] = days
		}
	}
	for tenantID, days := range daysByTenant {
		before := now.Add(-time.Duration(days) * 24 * time.Hour)
		if _, err := a.auditRetention.PurgeAuditBefore(ctx, tenantID, before); err != nil {
			return fmt.Errorf("purge audit for tenant %q: %w", tenantID, err)
		}
	}
	return nil
}

const defaultSessionIdleArchiveAge = 30 * 24 * time.Hour

func (a *application) archiveIdleSessions(ctx context.Context) {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()
	for {
		if _, err := a.archiveIdleSessionsOnce(ctx, time.Now().UTC()); err != nil {
			logBackgroundError("session archive", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *application) archiveIdleSessionsOnce(ctx context.Context, now time.Time) (int64, error) {
	if a == nil || a.sessionArchiver == nil {
		return 0, fmt.Errorf("session archiver is required")
	}
	archiveAge := a.sessionIdleArchiveAge
	if archiveAge == 0 {
		archiveAge = defaultSessionIdleArchiveAge
	}
	return a.sessionArchiver.ArchiveIdleSessions(ctx, now.Add(-archiveAge), 1000)
}

func (a *application) dispatchReplyOutbox(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := a.dispatchReplyOutboxOnce(ctx); err != nil {
			logBackgroundError("reply outbox", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *application) dispatchReplyOutboxOnce(ctx context.Context) error {
	if a == nil || a.replyOutbox == nil || a.configurations == nil {
		return fmt.Errorf("reply outbox dispatcher and configuration lister are required")
	}
	// External-IM replies must only be claimed by the active Channel owner.
	// Gateway-only processes dispatch Web replies and intentionally do not wait
	// on the Channel-owner lease.
	if a.role.runsChannel() && a.channelConnectors != nil && !a.channelConnectors.IsLeader() {
		return nil
	}
	snapshots, err := a.configurations.ListApplications(ctx, "")
	if err != nil {
		return fmt.Errorf("list applications for reply outbox: %w", err)
	}
	return dispatchTenantOutboxes(ctx, a.replyOutbox, tenantIDsForSnapshots(snapshots), "reply")
}

func tenantIDsForSnapshots(snapshots []tenant.Snapshot) []string {
	unique := make(map[string]struct{}, len(snapshots))
	for _, snapshot := range snapshots {
		if tenantID := strings.TrimSpace(snapshot.Config.TenantID); tenantID != "" {
			unique[tenantID] = struct{}{}
		}
	}
	tenantIDs := make([]string, 0, len(unique))
	for tenantID := range unique {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	return tenantIDs
}

func dispatchTenantOutboxes(ctx context.Context, dispatcher tenantOutboxDispatcher, tenantIDs []string, kind string) error {
	var failures []error
	for _, tenantID := range tenantIDs {
		if _, err := dispatcher.DispatchTenant(ctx, tenantID, 100); err != nil {
			failures = append(failures, fmt.Errorf("dispatch %s outbox for tenant %q: %w", kind, tenantID, err))
		}
	}
	return errors.Join(failures...)
}

func (a *application) dispatchConfigInvalidationOutbox(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := a.dispatchConfigInvalidationOutboxOnce(ctx); err != nil {
			logBackgroundError("configuration invalidation outbox", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *application) dispatchConfigInvalidationOutboxOnce(ctx context.Context) error {
	if a == nil || a.configInvalidationOutbox == nil || a.configurations == nil {
		return fmt.Errorf("configuration invalidation dispatcher and configuration lister are required")
	}
	snapshots, err := a.configurations.ListApplications(ctx, "")
	if err != nil {
		return fmt.Errorf("list applications for configuration invalidation: %w", err)
	}
	return dispatchTenantOutboxes(ctx, a.configInvalidationOutbox, tenantIDsForSnapshots(snapshots), "configuration invalidation")
}

func logBackgroundError(component string, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("background loop failed", "component", component, "error", err)
	}
}

func (a *application) Close() {
	if a == nil {
		return
	}
	for index := len(a.closeFuncs) - 1; index >= 0; index-- {
		a.closeFuncs[index]()
	}
}

const (
	workerRetryInitialInterval = 100 * time.Millisecond
	workerRetryMaxInterval     = 5 * time.Second
)

// newWorkerRetryBackoff returns the pacing policy for retryable Kafka
// redeliveries. It never gives up on its own: Kafka keeps the message
// uncommitted and the worker's bounded retry policy decides when DLQ takes
// over, so the backoff only spaces out redelivery attempts.
func newWorkerRetryBackoff() *backoff.ExponentialBackOff {
	retry := backoff.NewExponentialBackOff()
	retry.InitialInterval = workerRetryInitialInterval
	retry.MaxInterval = workerRetryMaxInterval
	retry.MaxElapsedTime = 0
	return retry
}

// waitRetryDelay blocks for the next backoff delay or until ctx is done. It
// reports false when the context ended so the worker loop can exit.
func waitRetryDelay(ctx context.Context, retry backoff.BackOff) bool {
	delay := retry.NextBackOff()
	if delay == backoff.Stop {
		delay = workerRetryMaxInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
