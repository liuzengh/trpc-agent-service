package runtimeplane

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
)

type MigrationProfileInfo struct{ Backend, Identity, Compatibility string }

func (c *Catalog) MigrationProfile(tenantID, profileID, backend string) (MigrationProfileInfo, error) {
	p, err := c.resolve(tenantID, profileID, backend)
	if err != nil {
		return MigrationProfileInfo{}, err
	}
	d := p.definition
	identity := []any{backend, strings.ToLower(d.Endpoint), tenantID}
	compatibility := []any{backend, "platform/v1"}
	switch backend {
	case "qdrant":
		identity = append(identity, d.Collection)
		compatibility = append(compatibility, d.Dimension, d.EmbeddingModel, d.EmbeddingEndpoint)
	case "s3":
		identity = append(identity, d.Bucket)
		compatibility = append(compatibility, d.MaxBytes)
	default:
		return MigrationProfileInfo{}, ErrProfileTypeMismatch
	}
	hash := func(value any) string {
		body, _ := json.Marshal(value)
		sum := sha256.Sum256(body)
		return hex.EncodeToString(sum[:])
	}
	return MigrationProfileInfo{Backend: backend, Identity: hash(identity), Compatibility: hash(compatibility)}, nil
}

// ValidateMigrationProfiles checks capabilities without exposing credentials.
func (c *Catalog) ValidateMigrationProfiles(tenantID, backend, sourceID, targetID string) error {
	source, err := c.resolve(tenantID, sourceID, backend)
	if err != nil {
		return err
	}
	target, err := c.resolve(tenantID, targetID, backend)
	if err != nil {
		return err
	}
	s, t := source.definition, target.definition
	switch backend {
	case "qdrant":
		if s.Dimension != t.Dimension || s.EmbeddingModel != t.EmbeddingModel || s.EmbeddingEndpoint != t.EmbeddingEndpoint {
			return fmt.Errorf("%w: incompatible embedding configuration", ErrDataPlaneUnavailable)
		}
		if s.Endpoint == t.Endpoint && s.Collection == t.Collection {
			return fmt.Errorf("%w: source and target share a collection", ErrDataPlaneUnavailable)
		}
	case "s3":
		if t.MaxBytes != s.MaxBytes {
			return fmt.Errorf("%w: artifact object limits differ", ErrDataPlaneUnavailable)
		}
		if s.Endpoint == t.Endpoint && s.Bucket == t.Bucket {
			return fmt.Errorf("%w: source and target share a bucket", ErrDataPlaneUnavailable)
		}
	default:
		return ErrProfileTypeMismatch
	}
	return nil
}

func (c *Catalog) knowledgeConfig(tenantID, profileID string) (knowledgeplane.QdrantConfig, error) {
	profile, err := c.resolve(tenantID, profileID, "qdrant")
	if err != nil {
		return knowledgeplane.QdrantConfig{}, err
	}
	host, portText, err := net.SplitHostPort(profile.definition.Endpoint)
	if err != nil {
		return knowledgeplane.QdrantConfig{}, ErrDataPlaneUnavailable
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return knowledgeplane.QdrantConfig{}, ErrDataPlaneUnavailable
	}
	return knowledgeplane.QdrantConfig{Host: host, Port: port, APIKey: profile.apiKey,
		TLS: profile.definition.TLS, AllowInsecure: profile.definition.AllowInsecure,
		Collection: profile.definition.Collection, Dimension: profile.definition.Dimension}, nil
}

func (c *Catalog) OpenKnowledgeStore(ctx context.Context, tenantID, appName, profileID string) (*knowledgeplane.ScopedStore, error) {
	config, err := c.knowledgeConfig(tenantID, profileID)
	if err != nil {
		return nil, err
	}
	return knowledgeplane.NewSynchronousQdrantScopedStore(ctx, tenantID, appName, config)
}

func (c *Catalog) ScanKnowledge(ctx context.Context, tenantID, profileID, cursor string, limit int) ([]knowledgeplane.DocumentIdentity, string, bool, error) {
	config, err := c.knowledgeConfig(tenantID, profileID)
	if err != nil {
		return nil, "", false, err
	}
	return knowledgeplane.ScanQdrantDocuments(ctx, tenantID, config, cursor, limit)
}

func (c *Catalog) OpenArtifactService(ctx context.Context, tenantID, profileID string, db *sql.DB) (*artifactplane.Service, error) {
	profile, err := c.resolve(tenantID, profileID, "s3")
	if err != nil {
		return nil, err
	}
	p := profile.definition
	objects, err := artifactplane.NewMinIOStore(ctx, artifactplane.MinIOConfig{Endpoint: p.Endpoint,
		AccessKey: profile.accessKey, SecretKey: profile.secretKey, SessionToken: profile.sessionToken,
		Bucket: p.Bucket, Region: p.Region, Secure: p.TLS, AllowInsecure: p.AllowInsecure,
		CreateBucket: p.CreateBucket, MaxBytes: p.MaxBytes})
	if err != nil {
		return nil, err
	}
	return artifactplane.NewService(tenantID, db, objects, p.MaxBytes)
}
