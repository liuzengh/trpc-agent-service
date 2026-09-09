// Package admin implements tenant configuration administration.
package admin

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/recovery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagemigration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"gopkg.in/yaml.v3"
)

// ErrInvalidConfig distinguishes caller-correctable publication errors from
// repository and activation failures that must be reported as server errors.
var ErrInvalidConfig = errors.New("admin: invalid configuration")

// Service validates and publishes immutable tenant configurations.
type Service struct {
	store      repository.Store
	now        func() time.Time
	audit      audit.Store
	redactor   *servicelog.Redactor
	profiles   func(*config.File) error
	migrations storagemigration.Store
	recovery   recovery.Store
}

// Option customizes a Service.
type Option func(*Service)

// WithAudit records every publish and rollback decision in the shared audit
// log. The stored payload never contains resolved secret material.
func WithAudit(store audit.Store) Option {
	return func(service *Service) { service.audit = store }
}

// WithRedactor redacts error text before it reaches responses or audit.
func WithRedactor(redactor *servicelog.Redactor) Option {
	return func(service *Service) {
		if redactor != nil {
			service.redactor = redactor
		}
	}
}

// WithProfileValidator adds a deployment-level validation gate, for example
// rejecting non-persistent storage profiles in production. It runs during
// Validate, Publish, and Rollback so invalid configs can never be published.
func WithProfileValidator(validator func(*config.File) error) Option {
	return func(service *Service) { service.profiles = validator }
}

// WithMigrationStore enables authenticated storage migration administration.
func WithMigrationStore(store storagemigration.Store) Option {
	return func(service *Service) { service.migrations = store }
}

// WithRecoveryStore enables durable Inbox/Outbox operator recovery. Production
// passes a PostgreSQL store; there is no in-memory runtime fallback.
func WithRecoveryStore(store recovery.Store) Option {
	return func(service *Service) { service.recovery = store }
}

// NewService creates an administration Service.
func NewService(store repository.Store, options ...Option) (*Service, error) {
	if store == nil {
		return nil, errors.New("admin: nil repository")
	}
	service := &Service{store: store, now: time.Now, redactor: servicelog.NewRedactor(nil, nil)}
	for _, option := range options {
		if option != nil {
			option(service)
		}
	}
	return service, nil
}

// PlanMigration validates the current immutable route and creates one durable
// backfill job. Source/target routes come from the published config, never from
// client-supplied backend payloads.
func (service *Service) PlanMigration(ctx context.Context, tenantID, appID string, domain storagemigration.Domain, expected tenant.ConfigVersion) (job storagemigration.Job, err error) {
	started := service.now()
	defer func() {
		service.recordDecision(ctx, tenantID, "storage.migration.plan", expected, expected, err, started)
	}()
	if service.migrations == nil {
		return storagemigration.Job{}, errors.New("admin: storage migrations are unavailable")
	}
	record, err := service.store.GetCurrentConfig(ctx, tenantID)
	if err != nil {
		return storagemigration.Job{}, err
	}
	if record.Version != expected {
		return storagemigration.Job{}, repository.ErrVersionConflict
	}
	file, err := config.Load(bytes.NewReader(record.Payload))
	if err != nil {
		return storagemigration.Job{}, errors.New("admin: stored config is invalid")
	}
	snapshot, err := file.Snapshot(tenantID, appID)
	if err != nil {
		return storagemigration.Job{}, fmt.Errorf("%w: app route is unavailable", ErrInvalidConfig)
	}
	route, err := migrationRoute(snapshot.App().Storage, domain)
	if err != nil {
		return storagemigration.Job{}, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if route.MigrationTarget == nil {
		return storagemigration.Job{}, fmt.Errorf("%w: domain has no migration_target", ErrInvalidConfig)
	}
	source := route.Clone()
	target := route.MigrationTarget.Clone()
	source.MigrationTarget = nil
	target.MigrationTarget = nil
	job, err = storagemigration.NewJob(tenantID, appID, record.Version, domain, source, target, ActorFrom(ctx))
	if err != nil {
		return storagemigration.Job{}, fmt.Errorf("%w: invalid migration route", ErrInvalidConfig)
	}
	return service.migrations.Create(ctx, job)
}

func (service *Service) ListMigrations(ctx context.Context, tenantID string) ([]storagemigration.Job, error) {
	if service.migrations == nil {
		return nil, errors.New("admin: storage migrations are unavailable")
	}
	return service.migrations.List(ctx, tenantID)
}
func (service *Service) GetMigration(ctx context.Context, tenantID, jobID string) (storagemigration.Job, error) {
	if service.migrations == nil {
		return storagemigration.Job{}, errors.New("admin: storage migrations are unavailable")
	}
	return service.migrations.Get(ctx, tenantID, jobID)
}
func (service *Service) CancelMigration(ctx context.Context, tenantID, jobID string) (err error) {
	started := service.now()
	defer func() { service.recordDecision(ctx, tenantID, "storage.migration.cancel", 0, 0, err, started) }()
	if service.migrations == nil {
		return errors.New("admin: storage migrations are unavailable")
	}
	return service.migrations.Cancel(ctx, tenantID, jobID)
}

func migrationRoute(profile tenant.StorageProfile, domain storagemigration.Domain) (tenant.BackendConfig, error) {
	switch domain {
	case storagemigration.DomainSession:
		return profile.Session.Clone(), nil
	case storagemigration.DomainMemory:
		return profile.Memory.Clone(), nil
	case storagemigration.DomainArtifact:
		return profile.Artifact.Clone(), nil
	case storagemigration.DomainKnowledge:
		return profile.Knowledge.Clone(), nil
	default:
		return tenant.BackendConfig{}, errors.New("unsupported migration domain")
	}
}

// Validate validates one tenant configuration and runs the production profile
// preflight when configured. Responses never contain resolved secret values.
func (service *Service) Validate(payload []byte) (*config.File, error) {
	file, err := config.Load(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if len(file.Tenants) != 1 {
		return nil, fmt.Errorf("%w: publication must contain exactly one tenant", ErrInvalidConfig)
	}
	if service.profiles != nil {
		if err := service.profiles(file); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
	}
	return file, nil
}

// Publish validates and atomically publishes the next configuration version.
func (service *Service) Publish(ctx context.Context, tenantID string, expected tenant.ConfigVersion, payload []byte) (record repository.ConfigRecord, err error) {
	started := service.now()
	defer func() {
		var newVersion tenant.ConfigVersion
		if err == nil {
			newVersion = record.Version
		}
		service.recordDecision(ctx, tenantID, "config.publish", expected, newVersion, err, started)
	}()
	file, err := service.Validate(payload)
	if err != nil {
		return repository.ConfigRecord{}, err
	}
	configured := file.Tenants[0]
	if configured.ID != tenantID {
		return repository.ConfigRecord{}, fmt.Errorf("%w: tenant scope %q does not match payload", ErrInvalidConfig, tenantID)
	}
	if configured.ConfigVersion != expected+1 {
		return repository.ConfigRecord{}, fmt.Errorf("%w: payload config_version must be %d", ErrInvalidConfig, expected+1)
	}
	return service.publish(ctx, configured.Name, configured.Enabled, tenantID, expected, payload, configured.Apps, nil)
}

// Versions lists a tenant's immutable configuration history.
func (service *Service) Versions(ctx context.Context, tenantID string) ([]repository.ConfigRecord, error) {
	return service.store.ListConfigVersions(ctx, tenantID)
}

// Current returns the tenant's published head version.
func (service *Service) Current(ctx context.Context, tenantID string) (repository.ConfigRecord, error) {
	return service.store.GetCurrentConfig(ctx, tenantID)
}

// RecoveryItems lists only operational metadata for tenant-scoped failed work.
func (service *Service) RecoveryItems(ctx context.Context, tenantID string, kind recovery.Kind, statuses []recovery.Status, limit int) ([]recovery.Item, error) {
	if service.recovery == nil {
		return nil, errors.New("admin: message recovery is unavailable")
	}
	return service.recovery.List(ctx, tenantID, kind, statuses, limit)
}

// RedriveRecovery moves an exact DLQ item back to retry using a status CAS.
func (service *Service) RedriveRecovery(ctx context.Context, tenantID string, kind recovery.Kind, id string, expected recovery.Status, reason string) (item recovery.Item, err error) {
	started := service.now()
	defer func() {
		service.recordRecoveryDecision(ctx, tenantID, "message."+string(kind)+".redrive", id, string(expected), string(item.Status), reason, err, started)
	}()
	if service.recovery == nil {
		return recovery.Item{}, errors.New("admin: message recovery is unavailable")
	}
	return service.recovery.Redrive(ctx, tenantID, kind, id, expected)
}

// ResolveUncertain records an explicit human decision for an ambiguous Outbox
// delivery. Retrying is deliberately distinct from ordinary DLQ redrive.
func (service *Service) ResolveUncertain(ctx context.Context, tenantID, id string, expected, decision recovery.Status, reason string) (item recovery.Item, err error) {
	started := service.now()
	defer func() {
		service.recordRecoveryDecision(ctx, tenantID, "message.outbox.resolve", id, string(expected), string(item.Status), reason, err, started)
	}()
	if service.recovery == nil {
		return recovery.Item{}, errors.New("admin: message recovery is unavailable")
	}
	return service.recovery.ResolveOutbox(ctx, tenantID, id, expected, decision)
}

// Rollback republishes an old payload as a new version; history is never rewritten.
func (service *Service) Rollback(ctx context.Context, tenantID string, expected, target tenant.ConfigVersion) (record repository.ConfigRecord, err error) {
	started := service.now()
	defer func() {
		var newVersion tenant.ConfigVersion
		if err == nil {
			newVersion = record.Version
		}
		service.recordDecision(ctx, tenantID, "config.rollback", expected, newVersion, err, started)
	}()
	record, err = service.store.GetConfigVersion(ctx, tenantID, target)
	if err != nil {
		return repository.ConfigRecord{}, err
	}
	file, err := service.Validate(record.Payload)
	if err != nil {
		return repository.ConfigRecord{}, fmt.Errorf("admin: stored config is invalid: %w", err)
	}
	file.Tenants[0].ConfigVersion = expected + 1
	payload, err := yaml.Marshal(file)
	if err != nil {
		return repository.ConfigRecord{}, fmt.Errorf("admin: encode rollback config: %w", err)
	}
	return service.publish(ctx, file.Tenants[0].Name, file.Tenants[0].Enabled, tenantID, expected, payload, file.Tenants[0].Apps, &target)
}

func (service *Service) publish(ctx context.Context, name string, enabled bool, tenantID string, expected tenant.ConfigVersion, payload []byte, apps []tenant.AgentApp, rollback *tenant.ConfigVersion) (repository.ConfigRecord, error) {
	if err := service.validateStorageTransition(ctx, tenantID, expected, apps); err != nil {
		return repository.ConfigRecord{}, err
	}
	digest := sha256.Sum256(payload)
	return service.store.PublishConfig(ctx, repository.ConfigRecord{
		TenantID: tenantID, TenantName: name, TenantEnabled: enabled, Payload: append([]byte(nil), payload...),
		SHA256: hex.EncodeToString(digest[:]), RolledBackFrom: rollback, Apps: apps,
		CreatedBy: ActorFrom(ctx), CreatedAt: service.now().UTC(),
	}, expected)
}

func (service *Service) validateStorageTransition(ctx context.Context, tenantID string, expected tenant.ConfigVersion, nextApps []tenant.AgentApp) error {
	if service.migrations == nil || expected == 0 {
		return nil
	}
	current, err := service.store.GetCurrentConfig(ctx, tenantID)
	if err != nil {
		return err
	}
	if current.Version != expected {
		return repository.ErrVersionConflict
	}
	currentFile, err := config.Load(bytes.NewReader(current.Payload))
	if err != nil {
		return errors.New("admin: stored config is invalid")
	}
	currentApps := make(map[string]tenant.AgentApp)
	for _, app := range currentFile.Tenants[0].Apps {
		currentApps[app.ID] = app
	}
	jobs, err := service.migrations.List(ctx, tenantID)
	if err != nil {
		return errors.New("admin: read storage migration state failed")
	}
	for _, next := range nextApps {
		old, ok := currentApps[next.ID]
		if !ok {
			continue
		}
		checks := []struct {
			domain   storagemigration.Domain
			old, new tenant.BackendConfig
		}{{storagemigration.DomainSession, old.Storage.Session, next.Storage.Session}, {storagemigration.DomainMemory, old.Storage.Memory, next.Storage.Memory}, {storagemigration.DomainArtifact, old.Storage.Artifact, next.Storage.Artifact}, {storagemigration.DomainKnowledge, old.Storage.Knowledge, next.Storage.Knowledge}}
		for _, check := range checks {
			if samePrimaryRoute(check.old, check.new) {
				continue
			}
			if check.old.MigrationTarget == nil || !samePrimaryRoute(*check.old.MigrationTarget, check.new) {
				return fmt.Errorf("%w: %s route change must cut over the declared migration_target", ErrInvalidConfig, check.domain)
			}
			complete := false
			for _, job := range jobs {
				if job.AppID == next.ID && job.ConfigVersion == expected && job.Domain == check.domain && job.Status == storagemigration.StatusCompleted && job.CopiedRows >= job.SourceRows && samePrimaryRoute(job.Source, check.old) && samePrimaryRoute(job.Target, check.new) {
					complete = true
					break
				}
			}
			if !complete {
				return fmt.Errorf("%w: %s migration is not verified for cutover", ErrInvalidConfig, check.domain)
			}
		}
	}
	return nil
}

func samePrimaryRoute(left, right tenant.BackendConfig) bool {
	return left.Type == right.Type && left.Endpoint == right.Endpoint && left.Namespace == right.Namespace && left.Credential == right.Credential
}

// recordDecision appends one redacted audit record per publish/rollback call.
func (service *Service) recordDecision(ctx context.Context, tenantID, action string, oldVersion, newVersion tenant.ConfigVersion, callErr error, started time.Time) {
	if service.audit == nil || tenantID == "" {
		return
	}
	decision, errorType := "allow", ""
	if callErr != nil {
		decision = "error"
		errorType = fmt.Sprintf("%T", callErr)
	}
	traceID := TraceFrom(ctx)
	operationID := newOperationID()
	details := map[string]any{
		"actor":       ActorFrom(ctx),
		"action":      action,
		"old_version": uint64(oldVersion),
		"new_version": uint64(newVersion),
	}
	if callErr != nil {
		details["error"] = service.redactor.RedactString(callErr.Error())
	}
	auditCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = service.audit.Append(auditCtx, audit.Record{
		TenantID: tenantID, AgentName: action, Decision: decision, Latency: service.now().Sub(started),
		ErrorType: errorType, ConfigVersion: newVersion, PolicyVersion: newVersion, TraceID: traceID, RequestID: operationID, Details: details,
	})
}

func (service *Service) recordRecoveryDecision(ctx context.Context, tenantID, action, id, oldStatus, newStatus, reason string, callErr error, started time.Time) {
	if service.audit == nil || tenantID == "" {
		return
	}
	decision, errorType := "allow", ""
	if callErr != nil {
		decision = "error"
		errorType = fmt.Sprintf("%T", callErr)
	}
	traceID := TraceFrom(ctx)
	operationID := newOperationID()
	reasonDigest := sha256.Sum256([]byte(reason))
	details := map[string]any{"actor": ActorFrom(ctx), "action": action, "message_id": id, "old_status": oldStatus, "new_status": newStatus, "reason_hash": hex.EncodeToString(reasonDigest[:])}
	if callErr != nil {
		details["error"] = service.redactor.RedactString(callErr.Error())
	}
	auditCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = service.audit.Append(auditCtx, audit.Record{TenantID: tenantID, AgentName: action, Decision: decision, Latency: service.now().Sub(started), ErrorType: errorType, TraceID: traceID, RequestID: operationID, Details: details})
}

func newOperationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("operation:%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("operation:%x", value[:])
}
