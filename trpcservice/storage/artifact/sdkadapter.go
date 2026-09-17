package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sdkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

// SDKUnscanned is the compliance sentinel stamped on artifacts written through
// the SDK path. The upstream artifact.Service has no Malware/DLP gating, so
// bytes stored here have NOT passed the preprocess scan pipeline. Downstream
// consumers must treat this value as "unscanned", never as a scan-clean
// credential. It exists so SDK-path records can never be mistaken for
// scan-clean media staged through the preprocess pipeline.
const SDKUnscanned = "sdk-unscanned"

// SDKService adapts the tenant-scoped media artifact Store to the upstream
// artifact.Service contract so a Runner can attach it via
// runner.WithArtifactService.
//
// Mapping:
//   - SessionInfo.AppName -> tenant ID (or MapTenant(AppName) when set)
//   - SessionInfo.SessionID -> request scope
//   - filename             -> stable artifact identity (ordinal fixed at 0)
//
// The upstream contract is version-oriented while this store is content-addressed
// and idempotent, so SaveArtifact always reports revision 0 (the first version)
// and ListVersions reports the single version 0. ListArtifactKeys/DeleteArtifact
// are not supported by the immutable store and return ErrCapabilityUnsupported.
type SDKService struct {
	// Store is the backing tenant-scoped artifact store.
	Store Store
	// MapTenant maps an upstream AppName to a tenant ID. Nil means identity.
	MapTenant func(appName string) string
}

// TenantFromAppName recovers the tenant ID from a control-plane AppName encoded
// as "tenantID/agentAppID". It fails closed: an AppName with no non-empty tenant
// segment is rejected instead of being treated as a tenant of its own, which
// would let a malformed binding address an unintended scope.
func TenantFromAppName(appName string) (string, error) {
	index := strings.Index(appName, "/")
	if index <= 0 {
		return "", runtime.ErrInvalidEnvelope
	}
	return appName[:index], nil
}

func (s *SDKService) tenant(appName string) string {
	if s.MapTenant != nil {
		return s.MapTenant(appName)
	}
	// Without a mapping the AppName is already the tenant. Callers that publish
	// an encoded "tenantID/agentAppID" AppName must supply MapTenant, and should
	// build it from TenantFromAppName so a malformed binding fails closed.
	return appName
}

// identityFor derives the stable content-addressed identity from the upstream
// session scope and filename. The ordinal is fixed at 0 because the SDK has no
// notion of request ordinal; identity uniqueness comes from the source digest.
func (s *SDKService) identityFor(info sdkartifact.SessionInfo, filename string) (id, ref string, sourceDigest string, err error) {
	tenantID := s.tenant(info.AppName)
	sum := sha256.Sum256([]byte(info.SessionID + "\x00" + filename))
	sourceDigest = hex.EncodeToString(sum[:])
	id, ref, err = StableIdentity(tenantID, info.SessionID, 0, sourceDigest)
	return id, ref, sourceDigest, err
}

// SaveArtifact writes an SDK artifact, deriving the content-addressed identity
// from SessionInfo and filename. The record is stamped SDKUnscanned to signal
// that it has not been gated by the Malware/DLP scan pipeline.
func (s *SDKService) SaveArtifact(ctx context.Context, info sdkartifact.SessionInfo, filename string, a *sdkartifact.Artifact) (int, error) {
	if s == nil || s.Store == nil {
		return 0, runtime.ErrCapabilityUnsupported
	}
	if a == nil || len(a.Data) == 0 || filename == "" || info.SessionID == "" {
		return 0, runtime.ErrInvalidEnvelope
	}
	id, ref, sourceDigest, err := s.identityFor(info, filename)
	if err != nil {
		return 0, err
	}
	contentSum := sha256.Sum256(a.Data)
	contentDigest := hex.EncodeToString(contentSum[:])
	mediaType := a.MimeType
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	record, err := s.Store.PutArtifact(ctx, Record{
		TenantID:           s.tenant(info.AppName),
		RequestID:          info.SessionID,
		ArtifactID:         id,
		ArtifactRef:        ref,
		Ordinal:            0,
		SourceDigest:       sourceDigest,
		ContentDigest:      contentDigest,
		MediaType:          mediaType,
		Kind:               "file",
		Content:            a.Data,
		MalwareScanVersion: SDKUnscanned,
		DLPVersion:         SDKUnscanned,
	})
	if err != nil {
		return 0, err
	}
	return record.Ordinal, nil
}

// LoadArtifact returns the stored artifact. The version argument is ignored
// because the store is content-addressed and single-versioned.
func (s *SDKService) LoadArtifact(ctx context.Context, info sdkartifact.SessionInfo, filename string, version *int) (*sdkartifact.Artifact, error) {
	if s == nil || s.Store == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if filename == "" || info.SessionID == "" {
		return nil, runtime.ErrInvalidEnvelope
	}
	id, _, _, err := s.identityFor(info, filename)
	if err != nil {
		return nil, err
	}
	record, err := s.Store.GetArtifact(ctx, s.tenant(info.AppName), id)
	if err != nil {
		return nil, err
	}
	return &sdkartifact.Artifact{Data: record.Content, MimeType: record.MediaType, Name: filename}, nil
}

// ListArtifactKeys is not supported by the immutable store.
func (s *SDKService) ListArtifactKeys(context.Context, sdkartifact.SessionInfo) ([]string, error) {
	return nil, runtime.ErrCapabilityUnsupported
}

// DeleteArtifact is not supported by the immutable store.
func (s *SDKService) DeleteArtifact(context.Context, sdkartifact.SessionInfo, string) error {
	return runtime.ErrCapabilityUnsupported
}

// ListVersions reports the single version 0 of the content-addressed store.
func (s *SDKService) ListVersions(context.Context, sdkartifact.SessionInfo, string) ([]int, error) {
	return []int{0}, nil
}

var _ sdkartifact.Service = (*SDKService)(nil)
