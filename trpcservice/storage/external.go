package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledgebase"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifacts3 "trpc.group/trpc-go/trpc-agent-go/artifact/s3"
	embedderopenai "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	vectorpg "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/pgvector"
	vectorqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func (router *Router) memoryService(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (memory.Service, error) {
	switch route.Type {
	case tenant.BackendPostgres:
		target, err := router.ResolveForScope(ctx, tenantID, appID, route)
		if err != nil {
			return nil, err
		}
		return newPostgresMemory(target.DSN)
	case tenant.BackendExternal:
		if tenantID == "" || appID == "" || route.Endpoint == "" || route.Credential.IsZero() {
			return nil, errors.New("storage: external memory endpoint, scope, and credential are required")
		}
		token, err := router.resolveRef(ctx, tenantID, appID, route.Credential)
		if err != nil || token == "" {
			return nil, errors.New("storage: resolve external memory credential failed")
		}
		return &ExternalMemory{TenantID: tenantID, AppID: appID, Endpoint: route.Endpoint, Token: token}, nil
	default:
		return nil, fmt.Errorf("storage: memory backend %q is unavailable", route.Type)
	}
}

type s3Credential struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
	Region          string `json:"region"`
	PathStyle       bool   `json:"path_style"`
}

func (router *Router) artifactService(ctx context.Context, tenantID, appID string, route tenant.BackendConfig) (artifact.Service, error) {
	switch route.Type {
	case tenant.BackendPostgres:
		target, err := router.ResolveForScope(ctx, tenantID, appID, route)
		if err != nil {
			return nil, err
		}
		return &PostgresArtifactService{DB: target.DB}, nil
	case tenant.BackendS3:
		if route.Endpoint == "" || route.Namespace == "" || route.Credential.IsZero() {
			return nil, errors.New("storage: S3 endpoint, bucket namespace, and credential SecretRef are required")
		}
		secretValue, err := router.resolveRef(ctx, tenantID, appID, route.Credential)
		if err != nil {
			return nil, errors.New("storage: resolve S3 credential failed")
		}
		var credential s3Credential
		if json.Unmarshal([]byte(secretValue), &credential) != nil || credential.AccessKeyID == "" || credential.SecretAccessKey == "" || credential.Region == "" {
			return nil, errors.New("storage: S3 credential payload is invalid")
		}
		ctx = servicelog.WithRedactor(ctx, servicelog.NewRedactor(nil, []string{secretValue, credential.AccessKeyID, credential.SecretAccessKey, credential.SessionToken}))
		options := []artifacts3.Option{artifacts3.WithEndpoint(route.Endpoint), artifacts3.WithRegion(credential.Region), artifacts3.WithCredentials(credential.AccessKeyID, credential.SecretAccessKey), artifacts3.WithPathStyle(credential.PathStyle)}
		if credential.SessionToken != "" {
			options = append(options, artifacts3.WithSessionToken(credential.SessionToken))
		}
		service, err := artifacts3.NewService(ctx, route.Namespace, options...)
		if err != nil {
			return nil, errors.New("storage: initialize S3 artifact backend failed")
		}
		return &CoordinatedArtifact{DB: router.defaultTarget.DB, Delegate: service}, nil
	default:
		return nil, fmt.Errorf("storage: artifact backend %q is unavailable", route.Type)
	}
}

func (router *Router) knowledgeService(ctx context.Context, tenantID, appID string, route tenant.BackendConfig, policy tenant.KnowledgePolicy) (*knowledgebase.Service, error) {
	if !policy.Enabled {
		return nil, nil
	}
	apiKey, err := router.resolveRef(ctx, tenantID, appID, policy.Embedding.APIKey)
	if err != nil {
		return nil, errors.New("storage: resolve embedding credential failed")
	}
	ctx = servicelog.WithRedactor(ctx, servicelog.NewRedactor(nil, []string{apiKey}))
	embeddings := embedderopenai.New(embedderopenai.WithAPIKey(apiKey), embedderopenai.WithBaseURL(policy.Embedding.BaseURL), embedderopenai.WithModel(policy.Embedding.Model), embedderopenai.WithDimensions(policy.Embedding.Dimensions))
	var store vectorstore.VectorStore
	secretValues := []string{apiKey}
	switch route.Type {
	case tenant.BackendPostgres:
		target, resolveErr := router.ResolveForScope(ctx, tenantID, appID, route)
		if resolveErr != nil {
			return nil, errors.New("storage: resolve PGVector backend failed")
		}
		store, err = vectorpg.New(vectorpg.WithPGVectorClientDSN(target.DSN), vectorpg.WithTable(physicalNamespace(route.Namespace, tenantID, appID)), vectorpg.WithIndexDimension(policy.Embedding.Dimensions), vectorpg.WithEnableTSVector(false))
	case tenant.BackendQdrant:
		host, port, tls, parseErr := qdrantEndpoint(route.Endpoint)
		if parseErr != nil {
			return nil, parseErr
		}
		options := []vectorqdrant.Option{vectorqdrant.WithHost(host), vectorqdrant.WithPort(port), vectorqdrant.WithTLS(tls), vectorqdrant.WithCollectionName(physicalNamespace(route.Namespace, tenantID, appID)), vectorqdrant.WithDimension(policy.Embedding.Dimensions)}
		if !route.Credential.IsZero() {
			key, resolveErr := router.resolveRef(ctx, tenantID, appID, route.Credential)
			if resolveErr != nil {
				return nil, errors.New("storage: resolve Qdrant credential failed")
			}
			ctx = servicelog.WithRedactor(ctx, servicelog.NewRedactor(nil, []string{apiKey, key}))
			secretValues = append(secretValues, key)
			options = append(options, vectorqdrant.WithAPIKey(key))
		}
		store, err = vectorqdrant.New(ctx, options...)
	default:
		return nil, fmt.Errorf("storage: knowledge backend %q is unavailable", route.Type)
	}
	if err != nil {
		return nil, errors.New("storage: initialize knowledge vector backend failed")
	}
	service, err := knowledgebase.New(tenantID, appID, store, embeddings, secretValues...)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return service, nil
}

var namespacePattern = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func physicalNamespace(prefix, tenantID, appID string) string {
	prefix = strings.Trim(namespacePattern.ReplaceAllString(prefix, "_"), "_")
	if prefix == "" {
		prefix = "knowledge"
	}
	if len(prefix) > 32 {
		prefix = prefix[:32]
	}
	digest := sha256.Sum256([]byte(tenantID + "\x00" + appID))
	return strings.ToLower(prefix + "_" + hex.EncodeToString(digest[:8]))
}

func qdrantEndpoint(value string) (string, int, bool, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Hostname() == "" {
		return "", 0, false, errors.New("storage: Qdrant endpoint is invalid")
	}
	tls := parsed.Scheme == "grpcs" || parsed.Scheme == "https"
	if parsed.Scheme != "grpc" && parsed.Scheme != "grpcs" && parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", 0, false, errors.New("storage: Qdrant endpoint scheme must be grpc, grpcs, http, or https")
	}
	port := 6334
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil || port <= 0 || port > 65535 {
			return "", 0, false, errors.New("storage: Qdrant endpoint port is invalid")
		}
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", 0, false, errors.New("storage: Qdrant endpoint must not contain a path")
	}
	return parsed.Hostname(), port, tls, nil
}

func validExternalEndpoint(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == ""
}
