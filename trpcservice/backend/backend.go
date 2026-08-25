// Package backend provides tenant-routed Qdrant knowledge and MinIO artifact stores.
package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const vectorSize = 4

type KnowledgeItem struct {
	ID      string         `json:"id"`
	Vector  []float32      `json:"vector"`
	Payload map[string]any `json:"payload,omitempty"`
	Score   float64        `json:"score,omitempty"`
}

type KnowledgeStore interface {
	Upsert(context.Context, string, KnowledgeItem) error
	Search(context.Context, string, []float32) ([]KnowledgeItem, error)
	Delete(context.Context, string, string) error
}

type ArtifactStore interface {
	Put(context.Context, string, string, io.Reader) error
	Get(context.Context, string, string) (io.ReadCloser, error)
	DeleteArtifact(context.Context, string, string) error
}

type Router struct {
	profiles map[string]tenant.BackendProfile
	qdrant   *qdrantStore
	minio    *minioStore
}

func NewRouter(ctx context.Context, settings config.ExternalBackends, tenants []tenant.Tenant, provider secrets.Provider) (*Router, error) {
	profiles := make(map[string]tenant.BackendProfile, len(tenants))
	for _, item := range tenants {
		profiles[item.ID] = item.Backend
	}
	router := &Router{profiles: profiles}
	if settings.QdrantURL != "" {
		if _, err := url.ParseRequestURI(settings.QdrantURL); err != nil {
			return nil, fmt.Errorf("invalid qdrant URL: %w", err)
		}
		router.qdrant = &qdrantStore{baseURL: strings.TrimRight(settings.QdrantURL, "/"), client: &http.Client{Timeout: 10 * time.Second}, ready: make(map[string]bool)}
	}
	if settings.MinIOEndpoint != "" {
		if provider == nil {
			return nil, errors.New("secret provider is required for MinIO")
		}
		access, err := resolveOne(ctx, provider, settings.MinIOAccessKeyRef, "MinIO access key")
		if err != nil {
			return nil, err
		}
		secret, err := resolveOne(ctx, provider, settings.MinIOSecretKeyRef, "MinIO secret key")
		if err != nil {
			return nil, err
		}
		client, err := minio.New(settings.MinIOEndpoint, &minio.Options{
			Creds: credentials.NewStaticV4(access, secret, ""), Secure: settings.MinIOSecure,
		})
		if err != nil {
			return nil, fmt.Errorf("create MinIO client: %w", err)
		}
		bucket := settings.MinIOBucket
		if bucket == "" {
			bucket = "trpc-agent-artifacts"
		}
		router.minio = &minioStore{client: client, bucket: bucket}
	}
	return router, nil
}

func resolveOne(ctx context.Context, provider secrets.Provider, ref, label string) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("%s reference is required", label)
	}
	values, err := provider.Resolve(ctx, ref)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", label, err)
	}
	if len(values) != 1 {
		return "", fmt.Errorf("%s must resolve to exactly one value", label)
	}
	return values[0], nil
}

func (r *Router) namespace(tenantID string) (string, error) {
	profile, ok := r.profiles[tenantID]
	if !ok {
		return "", fmt.Errorf("backend profile for tenant %s not found", tenantID)
	}
	namespace := profile.Namespace
	if namespace == "" {
		namespace = tenantID
	}
	return sanitize(namespace), nil
}

func (r *Router) Upsert(ctx context.Context, tenantID string, item KnowledgeItem) error {
	if r.qdrant == nil {
		return errors.New("qdrant is not configured")
	}
	namespace, err := r.namespace(tenantID)
	if err != nil {
		return err
	}
	return r.qdrant.upsert(ctx, tenantID, namespace, item)
}

func (r *Router) Search(ctx context.Context, tenantID string, vector []float32) ([]KnowledgeItem, error) {
	if r.qdrant == nil {
		return nil, errors.New("qdrant is not configured")
	}
	namespace, err := r.namespace(tenantID)
	if err != nil {
		return nil, err
	}
	return r.qdrant.search(ctx, tenantID, namespace, vector)
}

func (r *Router) Delete(ctx context.Context, tenantID, id string) error {
	if r.qdrant == nil {
		return errors.New("qdrant is not configured")
	}
	namespace, err := r.namespace(tenantID)
	if err != nil {
		return err
	}
	return r.qdrant.delete(ctx, namespace, id)
}

func (r *Router) Put(ctx context.Context, tenantID, key string, body io.Reader) error {
	if r.minio == nil {
		return errors.New("MinIO is not configured")
	}
	namespace, err := r.namespace(tenantID)
	if err != nil {
		return err
	}
	return r.minio.put(ctx, namespace, key, body)
}

func (r *Router) Get(ctx context.Context, tenantID, key string) (io.ReadCloser, error) {
	if r.minio == nil {
		return nil, errors.New("MinIO is not configured")
	}
	namespace, err := r.namespace(tenantID)
	if err != nil {
		return nil, err
	}
	return r.minio.get(ctx, namespace, key)
}

func (r *Router) DeleteArtifact(ctx context.Context, tenantID, key string) error {
	if r.minio == nil {
		return errors.New("MinIO is not configured")
	}
	namespace, err := r.namespace(tenantID)
	if err != nil {
		return err
	}
	return r.minio.delete(ctx, namespace, key)
}

var unsafeNamespace = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

func sanitize(value string) string {
	value = unsafeNamespace.ReplaceAllString(strings.TrimSpace(value), "_")
	value = strings.Trim(value, "_-")
	if value == "" {
		return "tenant"
	}
	return strings.ToLower(value)
}

type qdrantStore struct {
	baseURL string
	client  *http.Client
	mu      sync.Mutex
	ready   map[string]bool
}

func (s *qdrantStore) collection(namespace string) string { return "trpc_" + namespace }

func (s *qdrantStore) ensure(ctx context.Context, namespace string) error {
	collection := s.collection(namespace)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ready[collection] {
		return nil
	}
	body := map[string]any{"vectors": map[string]any{"size": vectorSize, "distance": "Cosine"}}
	status, err := s.request(ctx, http.MethodPut, "/collections/"+url.PathEscape(collection), body, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusCreated && status != http.StatusConflict {
		return fmt.Errorf("create Qdrant collection returned HTTP %d", status)
	}
	s.ready[collection] = true
	return nil
}

func (s *qdrantStore) upsert(ctx context.Context, tenantID, namespace string, item KnowledgeItem) error {
	if item.ID == "" || len(item.Vector) != vectorSize {
		return fmt.Errorf("knowledge item requires id and a %d-dimensional vector", vectorSize)
	}
	if err := s.ensure(ctx, namespace); err != nil {
		return err
	}
	payload := clonePayload(item.Payload)
	payload["tenant_id"] = tenantID
	status, err := s.request(ctx, http.MethodPut, "/collections/"+url.PathEscape(s.collection(namespace))+"/points?wait=true",
		map[string]any{"points": []any{map[string]any{"id": item.ID, "vector": item.Vector, "payload": payload}}}, nil)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("qdrant upsert returned HTTP %d", status)
	}
	return nil
}

func (s *qdrantStore) search(ctx context.Context, tenantID, namespace string, vector []float32) ([]KnowledgeItem, error) {
	if len(vector) != vectorSize {
		return nil, fmt.Errorf("search vector must have %d dimensions", vectorSize)
	}
	if err := s.ensure(ctx, namespace); err != nil {
		return nil, err
	}
	var response struct {
		Result []struct {
			ID      any            `json:"id"`
			Score   float64        `json:"score"`
			Payload map[string]any `json:"payload"`
		} `json:"result"`
	}
	status, err := s.request(ctx, http.MethodPost, "/collections/"+url.PathEscape(s.collection(namespace))+"/points/search",
		map[string]any{"vector": vector, "limit": 10, "with_payload": true,
			"filter": map[string]any{"must": []any{map[string]any{"key": "tenant_id", "match": map[string]any{"value": tenantID}}}}}, &response)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("qdrant search returned HTTP %d", status)
	}
	result := make([]KnowledgeItem, 0, len(response.Result))
	for _, point := range response.Result {
		result = append(result, KnowledgeItem{ID: fmt.Sprint(point.ID), Score: point.Score, Payload: point.Payload})
	}
	return result, nil
}

func (s *qdrantStore) delete(ctx context.Context, namespace, id string) error {
	if id == "" {
		return errors.New("knowledge id is required")
	}
	status, err := s.request(ctx, http.MethodPost, "/collections/"+url.PathEscape(s.collection(namespace))+"/points/delete?wait=true",
		map[string]any{"points": []string{id}}, nil)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("qdrant delete returned HTTP %d", status)
	}
	return nil
}

func (s *qdrantStore) request(ctx context.Context, method, requestPath string, body, destination any) (int, error) {
	ctx, span := otel.Tracer("trpc-agent-service/backend").Start(ctx, "qdrant.request")
	defer span.End()
	span.SetAttributes(attribute.String("http.request.method", method), attribute.String("server.address", s.baseURL))
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+requestPath, reader)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		span.RecordError(err)
		return 0, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if destination != nil && len(payload) > 0 && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.Unmarshal(payload, destination); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func clonePayload(input map[string]any) map[string]any {
	result := make(map[string]any, len(input)+1)
	for key, value := range input {
		result[key] = value
	}
	return result
}

type minioStore struct {
	client *minio.Client
	bucket string
	once   sync.Once
	err    error
}

func (s *minioStore) ensure(ctx context.Context) error {
	s.once.Do(func() {
		exists, err := s.client.BucketExists(ctx, s.bucket)
		if err != nil {
			s.err = err
			return
		}
		if !exists {
			s.err = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
		}
	})
	return s.err
}

func objectKey(namespace, key string) (string, error) {
	clean := strings.TrimPrefix(path.Clean("/"+key), "/")
	if clean == "." || clean == "" || strings.HasPrefix(clean, "../") {
		return "", errors.New("artifact key is invalid")
	}
	return namespace + "/" + clean, nil
}

func (s *minioStore) put(ctx context.Context, namespace, key string, body io.Reader) error {
	ctx, span := otel.Tracer("trpc-agent-service/backend").Start(ctx, "minio.put")
	defer span.End()
	if err := s.ensure(ctx); err != nil {
		return err
	}
	object, err := objectKey(namespace, key)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, s.bucket, object, body, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (s *minioStore) get(ctx context.Context, namespace, key string) (io.ReadCloser, error) {
	ctx, span := otel.Tracer("trpc-agent-service/backend").Start(ctx, "minio.get")
	defer span.End()
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	object, err := objectKey(namespace, key)
	if err != nil {
		return nil, err
	}
	result, err := s.client.GetObject(ctx, s.bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	if _, err := result.Stat(); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

func (s *minioStore) delete(ctx context.Context, namespace, key string) error {
	ctx, span := otel.Tracer("trpc-agent-service/backend").Start(ctx, "minio.delete")
	defer span.End()
	object, err := objectKey(namespace, key)
	if err != nil {
		return err
	}
	return s.client.RemoveObject(ctx, s.bucket, object, minio.RemoveObjectOptions{})
}
