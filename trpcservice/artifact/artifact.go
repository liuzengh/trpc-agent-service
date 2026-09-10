package artifact

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

var (
	// ErrNotFound reports that no accessible artifact metadata exists.
	ErrNotFound = errors.New("artifact metadata not found")
)

// Record is the SQL-authoritative metadata for one session-scoped artifact
// version. ObjectKey identifies storage only and is never an authorization
// input from an external caller.
type Record struct {
	ID                 string
	TenantID           string
	AppID              string
	ConfigVersion      string
	SessionPrincipalID string
	SessionID          string
	Filename           string
	Version            int
	ObjectKey          string
	MIMEType           string
	Size               int64
	Status             Status
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Status identifies the lifecycle state of an artifact record.
type Status string

const (
	// StatusPending reserves an immutable object version before its storage
	// upload completes. Pending records never authorize reads.
	StatusPending Status = "PENDING"
	// StatusAvailable permits a trusted session-scoped read.
	StatusAvailable Status = "AVAILABLE"
	// StatusDeleted prevents subsequent reads while retaining metadata history.
	StatusDeleted Status = "DELETED"
)

// Access binds an artifact service to one trusted execution session.
type Access struct {
	Scope              tenant.Scope
	ConfigVersion      string
	SessionPrincipalID string
	SessionID          string
}

// Validate checks that Access can bind a framework artifact request to one
// tenant application session.
func (a Access) Validate() error {
	if err := a.Scope.Validate(); err != nil {
		return err
	}
	if a.ConfigVersion == "" || a.SessionPrincipalID == "" || a.SessionID == "" {
		return errors.New("artifact access config and session are required")
	}
	return nil
}

// MetadataStore persists and authorizes session-scoped artifact metadata.
type MetadataStore interface {
	ReserveArtifact(context.Context, Access, string, string, int64) (Record, error)
	BindArtifactObject(context.Context, Record, string) error
	PublishArtifact(context.Context, Record) error
	AbandonArtifact(context.Context, Record) error
	FindArtifact(context.Context, Access, string, *int) (Record, error)
	ListArtifactKeys(context.Context, Access) ([]string, error)
	ListArtifactVersions(context.Context, Access, string) ([]int, error)
	MarkArtifactsDeleted(context.Context, Access, string) ([]Record, error)
}

// VersionedStorage can remove exactly one immutable artifact version. Platform
// production storage must implement it so failed metadata writes cannot delete
// older available versions of the same filename.
type VersionedStorage interface {
	frameworkartifact.Service
	SaveArtifactVersion(context.Context, frameworkartifact.SessionInfo, string, int, *frameworkartifact.Artifact) error
	DeleteArtifactVersion(context.Context, frameworkartifact.SessionInfo, string, int) error
}

// ExactObjectStorage is implemented by object stores that can load and delete
// the object key recorded by platform metadata. It is used for inbound media,
// which is uploaded before the final session identity is allocated.
type ExactObjectStorage interface {
	LoadArtifactObject(context.Context, string, string, int64) (*frameworkartifact.Artifact, error)
	DeleteArtifactObject(context.Context, string) error
}

// ExecutionResolver combines a configured backing service with authoritative
// SQL metadata and the trusted execution session scope.
type ExecutionResolver struct {
	storage  func(context.Context, worker.Execution) (frameworkartifact.Service, error)
	metadata MetadataStore
}

// NewExecutionResolver creates a resolver for SQL-guarded artifact services.
func NewExecutionResolver(
	storage func(context.Context, worker.Execution) (frameworkartifact.Service, error),
	metadata MetadataStore,
) (*ExecutionResolver, error) {
	if storage == nil {
		return nil, errors.New("artifact storage provider is required")
	}
	if metadata == nil {
		return nil, errors.New("artifact metadata store is required")
	}
	return &ExecutionResolver{storage: storage, metadata: metadata}, nil
}

// ResolveArtifact returns a service bound to exec's trusted session. It
// returns nil when the immutable app configuration has no artifact backend.
func (r *ExecutionResolver) ResolveArtifact(ctx context.Context, exec worker.Execution) (frameworkartifact.Service, error) {
	if r == nil || r.storage == nil || r.metadata == nil {
		return nil, errors.New("artifact execution resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := exec.Tenant.Validate(); err != nil {
		return nil, err
	}
	if exec.Config.BackendConfig.Artifact.IsZero() {
		return nil, nil
	}
	storage, err := r.storage(ctx, exec)
	if err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, errors.New("configured artifact storage service is required")
	}
	return NewService(storage, r.metadata, Access{
		Scope:              exec.Tenant.Scope(),
		ConfigVersion:      exec.Tenant.ConfigVersion,
		SessionPrincipalID: exec.Tenant.SessionPrincipalID,
		SessionID:          exec.Tenant.SessionID,
	})
}

// Service guards a framework Artifact service with SQL-authoritative metadata
// and one trusted execution session.
type Service struct {
	storage  frameworkartifact.Service
	metadata MetadataStore
	access   Access
}

// NewService creates an artifact service scoped to one trusted execution.
func NewService(storage frameworkartifact.Service, metadata MetadataStore, access Access) (*Service, error) {
	if storage == nil {
		return nil, errors.New("artifact storage service is required")
	}
	if metadata == nil {
		return nil, errors.New("artifact metadata store is required")
	}
	if err := access.Validate(); err != nil {
		return nil, err
	}
	return &Service{storage: storage, metadata: metadata, access: access}, nil
}

// SaveArtifact writes one trusted session artifact and records its metadata.
// The underlying object remains inaccessible through this service until its
// SQL record has been written successfully.
func (s *Service) SaveArtifact(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	value *frameworkartifact.Artifact,
) (int, error) {
	if err := s.validateRequest(info, filename); err != nil {
		return 0, err
	}
	if value == nil {
		return 0, errors.New("artifact is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	storageInfo, storageFilename, err := s.storageRequest(filename)
	if err != nil {
		return 0, err
	}
	record, err := s.metadata.ReserveArtifact(ctx, s.access, filename, value.MimeType, int64(len(value.Data)))
	if err != nil {
		return 0, err
	}
	objectKey, err := objectKey(s.storage, storageInfo, storageFilename, record.Version)
	if err != nil {
		return 0, s.abandonReservedArtifact(ctx, record, err)
	}
	if err := s.metadata.BindArtifactObject(ctx, record, objectKey); err != nil {
		return 0, s.abandonReservedArtifact(ctx, record, err)
	}
	record.ObjectKey = objectKey
	if err := s.saveArtifactVersion(ctx, storageInfo, storageFilename, record.Version, value); err != nil {
		return 0, s.compensateSave(ctx, storageInfo, storageFilename, record, err)
	}
	if err := s.metadata.PublishArtifact(ctx, record); err != nil {
		return 0, s.compensateSave(ctx, storageInfo, storageFilename, record, err)
	}
	return record.Version, nil
}

// LoadArtifact authorizes the requested session artifact before loading its
// exact recorded version from the backing storage service.
func (s *Service) LoadArtifact(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	version *int,
) (*frameworkartifact.Artifact, error) {
	if err := s.validateRequest(info, filename); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	record, err := s.metadata.FindArtifact(ctx, s.access, filename, version)
	if err != nil {
		return nil, err
	}
	if record.ConfigVersion != s.access.ConfigVersion {
		return nil, errors.New("artifact metadata config version does not match execution")
	}
	storageInfo, storageFilename, err := s.storageRequest(filename)
	if err != nil {
		return nil, err
	}
	if exact, ok := s.storage.(ExactObjectStorage); ok {
		return exact.LoadArtifactObject(ctx, record.ObjectKey, record.MIMEType, record.Size)
	}
	resolvedVersion := record.Version
	return s.storage.LoadArtifact(ctx, storageInfo, storageFilename, &resolvedVersion)
}

// ListArtifactKeys returns only filenames with available SQL metadata in the
// trusted session scope.
func (s *Service) ListArtifactKeys(ctx context.Context, info frameworkartifact.SessionInfo) ([]string, error) {
	if err := s.validateRequest(info, "listed"); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.metadata.ListArtifactKeys(ctx, s.access)
}

// DeleteArtifact removes all storage versions for one trusted session filename
// and prevents subsequent reads by marking its metadata deleted.
func (s *Service) DeleteArtifact(ctx context.Context, info frameworkartifact.SessionInfo, filename string) error {
	if err := s.validateRequest(info, filename); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records, err := s.metadata.MarkArtifactsDeleted(ctx, s.access, filename)
	if err != nil {
		return err
	}
	storageInfo, storageFilename, err := s.storageRequest(filename)
	if err != nil {
		return err
	}
	var failures []error
	for _, record := range records {
		if record.ConfigVersion != s.access.ConfigVersion {
			failures = append(failures, errors.New("artifact metadata config version does not match execution"))
			continue
		}
		var err error
		if exact, ok := s.storage.(ExactObjectStorage); ok {
			err = exact.DeleteArtifactObject(ctx, record.ObjectKey)
		} else {
			err = s.deleteArtifactVersion(ctx, storageInfo, storageFilename, record.Version)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("delete artifact storage: %w", err))
		}
	}
	return errors.Join(failures...)
}

func (s *Service) compensateSave(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	record Record,
	metadataErr error,
) error {
	metadataFailure := fmt.Errorf("record artifact metadata: %w", metadataErr)
	if err := s.metadata.AbandonArtifact(context.WithoutCancel(ctx), record); err != nil {
		metadataFailure = errors.Join(metadataFailure, fmt.Errorf("abandon artifact metadata: %w", err))
	}
	if err := s.deleteArtifactVersion(context.WithoutCancel(ctx), info, filename, record.Version); err == nil {
		return metadataFailure
	} else {
		return errors.Join(metadataFailure, fmt.Errorf("compensate artifact storage: %w", err))
	}
}

func (s *Service) abandonReservedArtifact(ctx context.Context, record Record, cause error) error {
	if err := s.metadata.AbandonArtifact(context.WithoutCancel(ctx), record); err != nil {
		return errors.Join(cause, fmt.Errorf("abandon artifact reservation: %w", err))
	}
	return cause
}

func (s *Service) saveArtifactVersion(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	version int,
	value *frameworkartifact.Artifact,
) error {
	storage, ok := s.storage.(VersionedStorage)
	if !ok {
		return errors.New("artifact storage does not support reserved versions")
	}
	return storage.SaveArtifactVersion(ctx, info, filename, version, value)
}

func (s *Service) deleteArtifactVersion(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	version int,
) error {
	storage, ok := s.storage.(VersionedStorage)
	if !ok {
		return errors.New("artifact storage does not support versioned deletion")
	}
	return storage.DeleteArtifactVersion(ctx, info, filename, version)
}

// ListVersions returns only versions with available SQL metadata in the
// trusted session scope.
func (s *Service) ListVersions(ctx context.Context, info frameworkartifact.SessionInfo, filename string) ([]int, error) {
	if err := s.validateRequest(info, filename); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	versions, err := s.metadata.ListArtifactVersions(ctx, s.access, filename)
	if err != nil {
		return nil, err
	}
	sort.Ints(versions)
	return versions, nil
}

func (s *Service) validateRequest(info frameworkartifact.SessionInfo, filename string) error {
	if s == nil || s.storage == nil || s.metadata == nil {
		return errors.New("artifact service is not initialized")
	}
	if err := s.access.Validate(); err != nil {
		return err
	}
	expectedAppName, err := s.artifactAppName()
	if err != nil {
		return err
	}
	if info.AppName != expectedAppName || info.UserID != s.access.SessionPrincipalID ||
		info.SessionID != s.access.SessionID {
		return errors.New("artifact session does not match execution scope")
	}
	if filename != "listed" && strings.TrimSpace(filename) == "" {
		return errors.New("artifact filename is required")
	}
	if strings.HasPrefix(filename, "user:") {
		return errors.New("user-scoped artifacts are not supported")
	}
	return nil
}

func (s *Service) storageSessionInfo() (frameworkartifact.SessionInfo, error) {
	appName, err := s.artifactAppName()
	if err != nil {
		return frameworkartifact.SessionInfo{}, err
	}
	return frameworkartifact.SessionInfo{
		AppName:   appName,
		UserID:    storageSegment(s.access.SessionPrincipalID),
		SessionID: storageSegment(s.access.SessionID),
	}, nil
}

func (s *Service) storageRequest(filename string) (frameworkartifact.SessionInfo, string, error) {
	info, err := s.storageSessionInfo()
	if err != nil {
		return frameworkartifact.SessionInfo{}, "", err
	}
	return info, storageSegment(filename), nil
}

func (s *Service) artifactAppName() (string, error) {
	return s.access.Scope.Key("runner")
}

func storageSegment(value string) string {
	return "artifact-v1-" + base64.RawURLEncoding.EncodeToString([]byte(value))
}

type objectKeyService interface {
	ObjectKey(frameworkartifact.SessionInfo, string, int) (string, error)
}

func objectKey(storage frameworkartifact.Service, info frameworkartifact.SessionInfo, filename string, version int) (string, error) {
	keyer, ok := storage.(objectKeyService)
	if !ok {
		return "", errors.New("artifact storage does not expose object keys")
	}
	return keyer.ObjectKey(info, filename, version)
}

var _ frameworkartifact.Service = (*Service)(nil)
