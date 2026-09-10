package storage

import (
	"context"
	"fmt"
	"strings"

	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactcos "trpc.group/trpc-go/trpc-agent-go/artifact/cos"
	artifacts3 "trpc.group/trpc-go/trpc-agent-go/artifact/s3"
)

const (
	ArtifactDriverPostgres       = "postgres"
	ArtifactDriverS3             = "s3"
	ArtifactDriverCOS            = "cos"
	MaxArtifactBytes       int64 = 16 << 20
)

type artifactDriver struct {
	name        string
	needsSecret bool
	open        func(context.Context, string) (agentartifact.Service, error)
}

// ArtifactBackends is the registered set of artifact.Service constructors.
// Official framework backends are registered alongside the platform PostgreSQL
// adapter, which exists only because the framework has no PostgreSQL artifact module.
type ArtifactBackends struct {
	drivers []artifactDriver
	byName  map[string]artifactDriver
}

func newArtifactBackends(postgres *PostgresArtifactService) *ArtifactBackends {
	backends := &ArtifactBackends{byName: make(map[string]artifactDriver)}
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
		open:        openOfficialS3ArtifactService,
	})
	backends.register(artifactDriver{
		name:        ArtifactDriverCOS,
		needsSecret: true,
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

func (b *ArtifactBackends) Open(ctx context.Context, driver, connection string) (agentartifact.Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name := strings.TrimSpace(driver)
	if name == "" {
		name = ArtifactDriverPostgres
	}
	registered, ok := b.byName[name]
	if !ok {
		return nil, fmt.Errorf("unsupported artifact driver %q", name)
	}
	if registered.needsSecret && strings.TrimSpace(connection) == "" {
		return nil, fmt.Errorf("artifact driver %q requires connection_ref", name)
	}
	service, err := registered.open(ctx, connection)
	if err != nil {
		return nil, err
	}
	return validatingArtifactService{Service: service}, nil
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
