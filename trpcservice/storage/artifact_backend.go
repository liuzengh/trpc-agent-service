package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactcos "trpc.group/trpc-go/trpc-agent-go/artifact/cos"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	artifacts3 "trpc.group/trpc-go/trpc-agent-go/artifact/s3"
)

const (
	ArtifactDriverInMemory       = "inmemory"
	ArtifactDriverPostgres       = "postgres"
	ArtifactDriverS3             = "s3"
	ArtifactDriverCOS            = "cos"
	MaxArtifactBytes       int64 = 16 << 20
)

type artifactDriver struct {
	name        string
	needsSecret bool
	owned       bool
	open        func(context.Context, string) (agentartifact.Service, error)
}

type artifactInstance struct {
	service agentartifact.Service
	closer  io.Closer
}

// ArtifactBackends is the registered set of artifact.Service constructors.
// Official framework backends are registered alongside the platform PostgreSQL
// adapter, which exists only because the framework has no PostgreSQL artifact module.
type ArtifactBackends struct {
	drivers   []artifactDriver
	byName    map[string]artifactDriver
	mu        sync.Mutex
	instances map[string]artifactInstance
	closed    bool
}

var ErrArtifactBackendsClosed = errors.New("artifact backends are closed")

func newArtifactBackends(postgres *PostgresArtifactService) *ArtifactBackends {
	backends := &ArtifactBackends{byName: make(map[string]artifactDriver), instances: make(map[string]artifactInstance)}
	inMemory := artifactinmemory.NewService()
	backends.register(artifactDriver{
		name: ArtifactDriverInMemory,
		open: func(context.Context, string) (agentartifact.Service, error) {
			return inMemory, nil
		},
	})
	backends.register(artifactDriver{
		name: ArtifactDriverPostgres,
		open: func(context.Context, string) (agentartifact.Service, error) {
			if postgres == nil {
				return nil, fmt.Errorf("postgres artifact service is not configured")
			}
			return postgres, nil
		},
	})
	backends.register(artifactDriver{
		name:        ArtifactDriverS3,
		needsSecret: true,
		owned:       true,
		open:        openOfficialS3ArtifactService,
	})
	backends.register(artifactDriver{
		name:        ArtifactDriverCOS,
		needsSecret: true,
		owned:       true,
		open:        openOfficialCOSArtifactService,
	})
	return backends
}

func (b *ArtifactBackends) register(driver artifactDriver) {
	b.drivers = append(b.drivers, driver)
	b.byName[driver.name] = driver
}

func (b *ArtifactBackends) DriverNames() []string {
	names := make([]string, 0, len(b.drivers))
	for _, driver := range b.drivers {
		names = append(names, driver.name)
	}
	return names
}

func (b *ArtifactBackends) Open(ctx context.Context, instanceKey, driver, connection string) (agentartifact.Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	instanceKey = strings.TrimSpace(instanceKey)
	if instanceKey == "" {
		return nil, errors.New("artifact backend instance key is required")
	}
	name := strings.TrimSpace(driver)
	if name == "" {
		name = ArtifactDriverPostgres
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrArtifactBackendsClosed
	}
	registered, ok := b.byName[name]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unsupported artifact driver %q", name)
	}
	if registered.needsSecret && strings.TrimSpace(connection) == "" {
		return nil, fmt.Errorf("artifact driver %q requires connection_ref", name)
	}
	fingerprint := sha256.Sum256([]byte(name + "\x00" + connection))
	cacheKey := fmt.Sprintf("%s\x00%x", instanceKey, fingerprint)
	b.mu.Lock()
	if cached, ok := b.instances[cacheKey]; ok {
		b.mu.Unlock()
		return cached.service, nil
	}
	if b.closed {
		b.mu.Unlock()
		return nil, ErrArtifactBackendsClosed
	}
	b.mu.Unlock()

	service, err := registered.open(ctx, connection)
	if err != nil {
		return nil, err
	}
	wrapped := validatingArtifactService{Service: service}
	instance := artifactInstance{service: wrapped}
	if registered.owned {
		instance.closer, _ = service.(io.Closer)
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		if instance.closer != nil {
			_ = instance.closer.Close()
		}
		return nil, ErrArtifactBackendsClosed
	}
	if cached, ok := b.instances[cacheKey]; ok {
		b.mu.Unlock()
		if instance.closer != nil {
			_ = instance.closer.Close()
		}
		return cached.service, nil
	}
	b.instances[cacheKey] = instance
	b.mu.Unlock()
	return wrapped, nil
}

func (b *ArtifactBackends) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	instances := b.instances
	b.instances = nil
	b.mu.Unlock()

	var result error
	for _, instance := range instances {
		if instance.closer != nil {
			result = errors.Join(result, instance.closer.Close())
		}
	}
	return result
}

type validatingArtifactService struct {
	agentartifact.Service
}

func (s validatingArtifactService) SaveArtifact(ctx context.Context, info agentartifact.SessionInfo, filename string, artifact *agentartifact.Artifact) (int, error) {
	if err := validateFrameworkArtifact(artifact); err != nil {
		return 0, err
	}
	return s.Service.SaveArtifact(ctx, info, filename, artifact)
}

func openOfficialS3ArtifactService(ctx context.Context, connection string) (agentartifact.Service, error) {
	configuration, err := ParseS3Config(connection)
	if err != nil {
		return nil, err
	}
	options := []artifacts3.Option{
		artifacts3.WithRegion(configuration.Region),
		artifacts3.WithCredentials(configuration.AccessKey, configuration.SecretKey),
		artifacts3.WithPathStyle(configuration.usePathStyle()),
	}
	if configuration.Endpoint != "" {
		options = append(options, artifacts3.WithEndpoint(configuration.Endpoint))
	}
	if configuration.SessionToken != "" {
		options = append(options, artifacts3.WithSessionToken(configuration.SessionToken))
	}
	return artifacts3.NewService(ctx, configuration.Bucket, options...)
}

func openOfficialCOSArtifactService(_ context.Context, connection string) (agentartifact.Service, error) {
	configuration, err := ParseCOSConfig(connection)
	if err != nil {
		return nil, err
	}
	return artifactcos.NewService(
		"artifact",
		configuration.BucketURL,
		artifactcos.WithSecretID(configuration.SecretID),
		artifactcos.WithSecretKey(configuration.SecretKey),
	)
}
