// Package cos resolves tenant-scoped COS artifact services.
package cos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"strings"
	"sync"

	sharedcos "github.com/liuzengh/trpc-agent-service/internal/cosclient"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	cosclient "github.com/tencentyun/cos-go-sdk-v5"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	frameworkcos "trpc.group/trpc-go/trpc-agent-go/artifact/cos"
)

const (
	providerName = "cos"
)

// EndpointResolver returns the operator-controlled COS bucket endpoint for a
// logical backend name. It must not use tenant-provided connection settings.
type EndpointResolver interface {
	ResolveCOSEndpoint(context.Context, string) (string, error)
}

// Resolver creates COS artifact services selected by immutable application
// configuration versions.
type Resolver struct {
	secrets   platformsecret.SecretProvider
	endpoints EndpointResolver

	mu       sync.Mutex
	closed   bool
	services map[string]artifact.Service
	clients  map[string]*cosclient.Client
}

// NewResolver creates a COS artifact resolver.
func NewResolver(secrets platformsecret.SecretProvider, endpoints EndpointResolver) (*Resolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if endpoints == nil {
		return nil, errors.New("endpoint resolver is required")
	}
	return &Resolver{
		secrets:   secrets,
		endpoints: endpoints,
		services:  make(map[string]artifact.Service),
		clients:   make(map[string]*cosclient.Client),
	}, nil
}

// ResolveArtifact returns the COS artifact service selected by exec. A nil
// service means the application has not configured an artifact backend.
func (r *Resolver) ResolveArtifact(ctx context.Context, exec worker.Execution) (artifact.Service, error) {
	if r == nil || r.secrets == nil || r.endpoints == nil {
		return nil, errors.New("cos artifact resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Artifact
	if ref.IsZero() {
		return nil, nil
	}
	scope := exec.Tenant.Scope()
	endpoint, err := r.resolveEndpoint(ctx, ref)
	if err != nil {
		return nil, err
	}
	serviceKey, err := artifactServiceKey(scope, exec.Tenant.ConfigVersion, ref, endpoint)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("cos artifact resolver is closed")
	}
	if service := r.services[serviceKey]; service != nil {
		r.mu.Unlock()
		return service, nil
	}
	r.mu.Unlock()

	client, err := r.resolveClient(ctx, scope, ref, endpoint, serviceKey)
	if err != nil {
		return nil, err
	}
	frameworkService, err := frameworkcos.NewService(
		ref.Name,
		endpoint,
		frameworkcos.WithClient(client),
	)
	if err != nil {
		return nil, fmt.Errorf("create cos artifact service: %w", err)
	}
	service := &versionedService{
		Service: frameworkService,
		client:  client,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("cos artifact resolver is closed")
	}
	if existing := r.services[serviceKey]; existing != nil {
		return existing, nil
	}
	r.services[serviceKey] = service
	return service, nil
}

// InboundObjectStore uploads media before channel admission has allocated a
// session identity. The returned object key is later attached to SQL artifact
// metadata in the admission transaction.
type InboundObjectStore struct {
	client *cosclient.Client
	prefix string
}

// ResolveInboundStore creates a scoped object writer for pre-admission media.
func (r *Resolver) ResolveInboundStore(
	ctx context.Context,
	scope tenant.Scope,
	configVersion string,
	ref tenant.BackendRef,
) (*InboundObjectStore, error) {
	if r == nil || r.secrets == nil || r.endpoints == nil {
		return nil, errors.New("cos artifact resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if configVersion == "" {
		return nil, errors.New("artifact config version is required")
	}
	endpoint, err := r.resolveEndpoint(ctx, ref)
	if err != nil {
		return nil, err
	}
	serviceKey, err := artifactServiceKey(scope, configVersion, ref, endpoint)
	if err != nil {
		return nil, err
	}
	client, err := r.resolveClient(ctx, scope, ref, endpoint, serviceKey)
	if err != nil {
		return nil, err
	}
	prefix, err := scope.Key("inbound-artifact", configVersion)
	if err != nil {
		return nil, err
	}
	return &InboundObjectStore{client: client, prefix: prefix}, nil
}

// DeleteExactObject deletes one SQL-authorized object using the immutable
// artifact backend selected by configVersion. The object key is never used to
// derive tenant scope or to discover platform state.
func (r *Resolver) DeleteExactObject(
	ctx context.Context,
	scope tenant.Scope,
	configVersion string,
	ref tenant.BackendRef,
	objectKey string,
) error {
	if r == nil || r.secrets == nil || r.endpoints == nil {
		return errors.New("cos artifact resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	if configVersion == "" {
		return errors.New("artifact config version is required")
	}
	if objectKey == "" {
		return errors.New("artifact object key is required")
	}
	endpoint, err := r.resolveEndpoint(ctx, ref)
	if err != nil {
		return err
	}
	serviceKey, err := artifactServiceKey(scope, configVersion, ref, endpoint)
	if err != nil {
		return err
	}
	client, err := r.resolveClient(ctx, scope, ref, endpoint, serviceKey)
	if err != nil {
		return err
	}
	if _, err := client.Object.Delete(ctx, objectKey); err != nil && !cosclient.IsNotFoundError(err) {
		return fmt.Errorf("delete cos artifact object: %w", err)
	}
	return nil
}

func (r *Resolver) resolveEndpoint(ctx context.Context, ref tenant.BackendRef) (string, error) {
	if err := ValidateBackend(ref); err != nil {
		return "", err
	}
	endpoint, err := r.endpoints.ResolveCOSEndpoint(ctx, ref.Name)
	if err != nil {
		return "", fmt.Errorf("resolve cos endpoint: %w", err)
	}
	if err := sharedcos.ValidateEndpoint(endpoint); err != nil {
		return "", err
	}
	return endpoint, nil
}

func (r *Resolver) resolveClient(
	ctx context.Context,
	scope tenant.Scope,
	ref tenant.BackendRef,
	endpoint string,
	serviceKey string,
) (*cosclient.Client, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("cos artifact resolver is closed")
	}
	if client := r.clients[serviceKey]; client != nil {
		r.mu.Unlock()
		return client, nil
	}
	r.mu.Unlock()

	credential, err := r.secrets.ResolveSecret(ctx, scope, ref.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("resolve cos credentials: %w", err)
	}
	client, err := sharedcos.New(endpoint, credential)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("cos artifact resolver is closed")
	}
	if existing := r.clients[serviceKey]; existing != nil {
		return existing, nil
	}
	r.clients[serviceKey] = client
	return client, nil
}

// ObjectKey returns the scoped key for one pre-admission object. Callers can
// persist this key before uploading so a crash cannot leave an untracked blob.
func (s *InboundObjectStore) ObjectKey(objectID string) (string, error) {
	if s == nil || s.prefix == "" {
		return "", errors.New("inbound object store is not initialized")
	}
	if objectID == "" || strings.ContainsAny(objectID, "/\\:\x00\r\n") {
		return "", errors.New("inbound object id is invalid")
	}
	return s.prefix + ":" + objectID, nil
}

// Put stores one pre-admission object under an internally generated key.
func (s *InboundObjectStore) Put(
	ctx context.Context,
	objectID string,
	value *artifact.Artifact,
) (string, error) {
	if s == nil || s.client == nil || s.prefix == "" {
		return "", errors.New("inbound object store is not initialized")
	}
	if objectID == "" {
		return "", errors.New("inbound object id is required")
	}
	if value == nil || len(value.Data) == 0 {
		return "", errors.New("inbound artifact data is required")
	}
	key, err := s.ObjectKey(objectID)
	if err != nil {
		return "", err
	}
	_, err = s.client.Object.Put(ctx, key, bytes.NewReader(value.Data), &cosclient.ObjectPutOptions{
		ObjectPutHeaderOptions: &cosclient.ObjectPutHeaderOptions{
			ContentType:        value.MimeType,
			ContentDisposition: mime.FormatMediaType("attachment", map[string]string{"filename": value.Name}),
		},
	})
	if err != nil {
		return "", fmt.Errorf("upload inbound artifact: %w", err)
	}
	return key, nil
}

// Delete removes one pre-admission object and is safe for an already removed
// object.
func (s *InboundObjectStore) Delete(ctx context.Context, objectKey string) error {
	if s == nil || s.client == nil || s.prefix == "" {
		return errors.New("inbound object store is not initialized")
	}
	if objectKey == "" || !strings.HasPrefix(objectKey, s.prefix+":") {
		return errors.New("inbound object key is outside its scope")
	}
	_, err := s.client.Object.Delete(ctx, objectKey)
	if err != nil && !cosclient.IsNotFoundError(err) {
		return fmt.Errorf("delete inbound artifact: %w", err)
	}
	return nil
}

type versionedService struct {
	artifact.Service
	client *cosclient.Client
}

func (s *versionedService) LoadArtifactObject(
	ctx context.Context,
	objectKey, mimeType string,
	size int64,
) (*artifact.Artifact, error) {
	if s == nil || s.client == nil {
		return nil, errors.New("cos artifact object service is not initialized")
	}
	if objectKey == "" || size < 0 {
		return nil, errors.New("cos artifact object key and size are invalid")
	}
	response, err := s.client.Object.Get(ctx, objectKey, nil)
	if err != nil {
		return nil, fmt.Errorf("get cos artifact object: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, size+1))
	if err != nil {
		return nil, fmt.Errorf("read cos artifact object: %w", err)
	}
	if int64(len(data)) != size {
		return nil, errors.New("cos artifact object size does not match metadata")
	}
	filename := ""
	if disposition := response.Header.Get("Content-Disposition"); disposition != "" {
		if _, params, parseErr := mime.ParseMediaType(disposition); parseErr == nil {
			filename = params["filename"]
		}
	}
	return &artifact.Artifact{Data: data, MimeType: mimeType, Name: filename}, nil
}

func (s *versionedService) DeleteArtifactObject(ctx context.Context, objectKey string) error {
	if s == nil || s.client == nil {
		return errors.New("cos artifact object service is not initialized")
	}
	if objectKey == "" {
		return errors.New("cos artifact object key is required")
	}
	_, err := s.client.Object.Delete(ctx, objectKey)
	if err != nil && !cosclient.IsNotFoundError(err) {
		return fmt.Errorf("delete cos artifact object: %w", err)
	}
	return nil
}

func (s *versionedService) SaveArtifactVersion(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
	version int,
	value *artifact.Artifact,
) error {
	if s == nil || s.Service == nil || s.client == nil {
		return errors.New("cos versioned artifact service is not initialized")
	}
	if value == nil {
		return errors.New("artifact is required")
	}
	keyer, ok := s.Service.(interface {
		ObjectKey(artifact.SessionInfo, string, int) (string, error)
	})
	if !ok {
		return errors.New("cos artifact service does not expose version object keys")
	}
	key, err := keyer.ObjectKey(info, filename, version)
	if err != nil {
		return fmt.Errorf("resolve cos artifact version key: %w", err)
	}
	_, err = s.client.Object.Put(ctx, key, bytes.NewReader(value.Data), &cosclient.ObjectPutOptions{
		ObjectPutHeaderOptions: &cosclient.ObjectPutHeaderOptions{
			ContentType: value.MimeType,
			ContentDisposition: mime.FormatMediaType("attachment", map[string]string{
				"filename": filename,
			}),
		},
	})
	if err != nil {
		return fmt.Errorf("upload cos artifact version: %w", err)
	}
	return nil
}

func (s *versionedService) DeleteArtifactVersion(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
	version int,
) error {
	if s == nil || s.Service == nil || s.client == nil {
		return errors.New("cos versioned artifact service is not initialized")
	}
	keyer, ok := s.Service.(interface {
		ObjectKey(artifact.SessionInfo, string, int) (string, error)
	})
	if !ok {
		return errors.New("cos artifact service does not expose version object keys")
	}
	key, err := keyer.ObjectKey(info, filename, version)
	if err != nil {
		return fmt.Errorf("resolve cos artifact version key: %w", err)
	}
	_, err = s.client.Object.Delete(ctx, key)
	if err != nil && !cosclient.IsNotFoundError(err) {
		return fmt.Errorf("delete cos artifact version: %w", err)
	}
	return nil
}

// ValidateBackend checks that ref selects the COS artifact adapter.
func ValidateBackend(ref tenant.BackendRef) error {
	if ref.Kind != tenant.BackendObject {
		return errors.New("cos artifact backend kind must be object")
	}
	if ref.Provider != providerName {
		return fmt.Errorf("unsupported artifact provider %q", ref.Provider)
	}
	if ref.SecretRef == (tenant.SecretRef{}) {
		return errors.New("cos artifact backend secret_ref is required")
	}
	return nil
}

// Close releases cached service and client references. COS clients do not own
// a closeable connection, so callers may safely call Close multiple times.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.services = nil
	r.clients = nil
	return nil
}

func artifactServiceKey(scope tenant.Scope, version string, ref tenant.BackendRef, endpoint string) (string, error) {
	parts := []string{version, ref.Provider, ref.Name, ref.SecretRef.Name}
	if ref.SecretRef.Version != "" {
		parts = append(parts, ref.SecretRef.Version)
	}
	parts = append(parts, endpoint)
	return scope.Key("artifact", parts...)
}
