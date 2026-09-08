package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"golang.org/x/sync/singleflight"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	artifacts3 "trpc.group/trpc-go/trpc-agent-go/artifact/s3"
)

const artifactResourceType = "artifact"

type ArtifactRouter struct {
	repository controlplane.Repository
	secrets    secret.Store
	mu         sync.RWMutex
	closed     bool
	services   map[string]artifact.Service
	locks      map[string]*sync.Mutex
	group      singleflight.Group
	lockDB     *sql.DB
}

func NewArtifactRouter(
	repository controlplane.Repository,
	secretStore secret.Store,
) (*ArtifactRouter, error) {
	if repository == nil || secretStore == nil {
		return nil, fmt.Errorf("artifact router repository and secret store are required")
	}
	router := &ArtifactRouter{
		repository: repository,
		secrets:    secretStore,
		services:   make(map[string]artifact.Service),
		locks:      make(map[string]*sync.Mutex),
	}
	if provider, ok := repository.(interface{ SQLDB() *sql.DB }); ok {
		router.lockDB = provider.SQLDB()
	}
	return router, nil
}

func (r *ArtifactRouter) SaveArtifact(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
	value *artifact.Artifact,
) (int, error) {
	ctx, span := startStorageSpan(ctx, "artifact.save", info.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, info.AppName)
	if err != nil {
		return 0, err
	}
	// tRPC-Agent-Go's S3 adapter determines the next revision with list+put.
	// Serialize the same logical key inside a process; Agent session leases
	// provide cross-node serialization for normal Runner/Tool execution.
	lock := r.lockFor(info, filename)
	lock.Lock()
	defer lock.Unlock()
	if r.lockDB == nil {
		return service.SaveArtifact(ctx, info, filename, value)
	}
	var version int
	err = r.withDistributedLock(ctx, r.artifactLockKey(info, filename), func() error {
		var saveErr error
		version, saveErr = service.SaveArtifact(ctx, info, filename, value)
		return saveErr
	})
	return version, err
}

func (r *ArtifactRouter) LoadArtifact(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
	version *int,
) (*artifact.Artifact, error) {
	ctx, span := startStorageSpan(ctx, "artifact.load", info.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, info.AppName)
	if err != nil {
		return nil, err
	}
	return service.LoadArtifact(ctx, info, filename, version)
}

func (r *ArtifactRouter) ListArtifactKeys(
	ctx context.Context,
	info artifact.SessionInfo,
) ([]string, error) {
	service, err := r.serviceFor(ctx, info.AppName)
	if err != nil {
		return nil, err
	}
	return service.ListArtifactKeys(ctx, info)
}

func (r *ArtifactRouter) DeleteArtifact(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
) error {
	service, err := r.serviceFor(ctx, info.AppName)
	if err != nil {
		return err
	}
	lock := r.lockFor(info, filename)
	lock.Lock()
	defer lock.Unlock()
	return service.DeleteArtifact(ctx, info, filename)
}

func (r *ArtifactRouter) ListVersions(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
) ([]int, error) {
	service, err := r.serviceFor(ctx, info.AppName)
	if err != nil {
		return nil, err
	}
	return service.ListVersions(ctx, info, filename)
}

func (r *ArtifactRouter) Ready(ctx context.Context) error {
	if r == nil || r.repository == nil {
		return errors.New("artifact router is not initialized")
	}
	return r.repository.Ready(ctx)
}

func (r *ArtifactRouter) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := make([]artifact.Service, 0, len(r.services))
	for _, service := range r.services {
		services = append(services, service)
	}
	r.mu.Unlock()
	var closeErr error
	for _, service := range services {
		if closer, ok := service.(interface{ Close() error }); ok {
			closeErr = errors.Join(closeErr, closer.Close())
		}
	}
	return closeErr
}

func (r *ArtifactRouter) lockFor(info artifact.SessionInfo, filename string) *sync.Mutex {
	key := r.artifactLockKey(info, filename)
	r.mu.Lock()
	defer r.mu.Unlock()
	lock := r.locks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		r.locks[key] = lock
	}
	return lock
}

func (r *ArtifactRouter) artifactLockKey(info artifact.SessionInfo, filename string) string {
	return info.AppName + "\x00" + info.UserID + "\x00" + info.SessionID + "\x00" + filename
}

func (r *ArtifactRouter) withDistributedLock(
	ctx context.Context,
	key string,
	operation func() error,
) (err error) {
	// Composite artifact identities contain NUL separators. PostgreSQL TEXT
	// cannot represent those bytes. Hash in Go before crossing the SQL text
	// boundary, for both lock and unlock; raw identities also stay out of SQL
	// parameter diagnostics. Keep the same digest across every writer.
	digest := sha256.Sum256([]byte(key))
	key = "artifact-lock:v1:" + hex.EncodeToString(digest[:])
	conn, err := r.lockDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire artifact lock connection: %w", err)
	}
	defer conn.Close()
	var ignored any
	if err := conn.QueryRowContext(
		ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, key,
	).Scan(&ignored); err != nil {
		return fmt.Errorf("acquire artifact advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		var unlocked bool
		unlockErr := conn.QueryRowContext(
			unlockCtx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key,
		).Scan(&unlocked)
		if unlockErr != nil || !unlocked {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			err = errors.Join(err, fmt.Errorf("release artifact advisory lock: %v", unlockErr))
		}
	}()
	return operation()
}

func (r *ArtifactRouter) serviceFor(ctx context.Context, appName string) (artifact.Service, error) {
	tenantID, appID, err := runtimecontext.ParseStorageScope(appName)
	if err != nil {
		return nil, err
	}
	binding, err := resolveBackendBinding(ctx, r.repository, tenantID, appID, artifactResourceType)
	if err != nil {
		return nil, err
	}
	cacheKey := binding.ID + "\x00" + fmt.Sprint(binding.Version)
	r.mu.RLock()
	service := r.services[cacheKey]
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, errors.New("artifact router is closed")
	}
	if service != nil {
		return service, nil
	}
	value, err, _ := r.group.Do(cacheKey, func() (any, error) {
		r.mu.RLock()
		cached := r.services[cacheKey]
		closed := r.closed
		r.mu.RUnlock()
		if closed {
			return nil, errors.New("artifact router is closed")
		}
		if cached != nil {
			return cached, nil
		}
		built, err := r.build(ctx, binding)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			if closer, ok := built.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			return nil, errors.New("artifact router is closed")
		}
		r.services[cacheKey] = built
		r.mu.Unlock()
		return built, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(artifact.Service), nil
}

type artifactBackendConfig struct {
	Bucket    string `json:"bucket"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	PathStyle bool   `json:"path_style"`
	Retries   int    `json:"retries"`
}

type s3Credentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
}

func (r *ArtifactRouter) build(
	ctx context.Context,
	binding controlplane.BackendBinding,
) (artifact.Service, error) {
	var cfg artifactBackendConfig
	if err := decodeStorageConfig(binding.Config, &cfg); err != nil {
		return nil, fmt.Errorf("decode artifact binding %q: %w", binding.ID, err)
	}
	switch strings.ToLower(binding.BackendType) {
	case "inmemory":
		return artifactinmemory.NewService(), nil
	case "s3", "minio":
		if cfg.Bucket == "" {
			return nil, fmt.Errorf("artifact binding %q requires bucket", binding.ID)
		}
		options := []artifacts3.Option{
			artifacts3.WithPathStyle(cfg.PathStyle),
		}
		if cfg.Endpoint != "" {
			options = append(options, artifacts3.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Region != "" {
			options = append(options, artifacts3.WithRegion(cfg.Region))
		}
		if cfg.Retries > 0 {
			options = append(options, artifacts3.WithRetries(cfg.Retries))
		}
		if binding.SecretRef == "" {
			return nil, errors.New("S3 requires an explicit tenant credential reference")
		}
		if binding.SecretRef != "" {
			raw, err := r.secrets.Resolve(ctx, binding.TenantID, secret.Artifact, binding.SecretRef)
			if err != nil {
				return nil, err
			}
			var credentials s3Credentials
			if err := decodeStorageConfig(json.RawMessage(raw), &credentials); err != nil {
				return nil, fmt.Errorf("decode S3 credentials: %w", err)
			}
			if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" {
				return nil, errors.New("S3 access key and secret key are required")
			}
			options = append(options, artifacts3.WithCredentials(
				credentials.AccessKeyID, credentials.SecretAccessKey,
			))
			if credentials.SessionToken != "" {
				options = append(options, artifacts3.WithSessionToken(credentials.SessionToken))
			}
		}
		return artifacts3.NewService(ctx, cfg.Bucket, options...)
	default:
		return nil, fmt.Errorf("unsupported artifact backend %q", binding.BackendType)
	}
}

var _ artifact.Service = (*ArtifactRouter)(nil)
