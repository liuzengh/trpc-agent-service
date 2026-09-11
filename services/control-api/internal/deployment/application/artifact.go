package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"strings"
	"unicode/utf8"
)

const MaxArtifactContentBytes = 16 << 20

var (
	ErrArtifactInvalid     = errors.New("invalid artifact request")
	ErrArtifactForbidden   = errors.New("artifact forbidden")
	ErrArtifactNotFound    = errors.New("artifact not found")
	ErrArtifactTooLarge    = errors.New("artifact too large")
	ErrArtifactUnavailable = errors.New("artifact backend unavailable")
)

type ArtifactCredentialResolver interface {
	ResolveArtifactForOwner(context.Context, profileapp.CheckProfileCredentialsCommand) (profileapp.CredentialBatch, error)
}
type ArtifactBackend interface {
	ExecuteArtifact(context.Context, ArtifactBackendRequest) (ArtifactResult, error)
}
type ArtifactCommand struct {
	TenantID, DeploymentID, ActorUserID, RunID, Name, Operation, MIMEType string
	RevisionNumber                                                        int64
	Version                                                               *int
	Content                                                               []byte
}
type ArtifactSecrets = executionv1.ArtifactSecrets
type ArtifactBackendRequest = executionv1.ArtifactRequest
type ArtifactResult = executionv1.ArtifactResponse

func ValidArtifactName(n string) bool {
	return n != "" && n != "." && n != ".." && len(n) <= 255 && utf8.ValidString(n) && !strings.ContainsAny(n, "/\\\x00\r\n")
}
func (s *Service) AccessArtifact(ctx context.Context, c ArtifactCommand) (ArtifactResult, error) {
	if err := s.authorizeOwner(ctx, c.TenantID, c.ActorUserID); err != nil {
		return ArtifactResult{}, err
	}
	if !ValidArtifactName(c.Name) || c.RunID == "" || len(c.RunID) > 256 || strings.ContainsAny(c.RunID, "\x00\r\n") || c.RevisionNumber < 1 || (c.Operation != "save" && c.Operation != "load") || (c.Version != nil && (*c.Version < 0 || c.Operation != "load")) || len(c.MIMEType) > 256 || strings.ContainsAny(c.MIMEType, "\r\n\x00") {
		return ArtifactResult{}, ErrArtifactInvalid
	}
	if len(c.Content) > MaxArtifactContentBytes {
		return ArtifactResult{}, ErrArtifactTooLarge
	}
	if s.deps.ArtifactBackend == nil || s.deps.ArtifactCredentials == nil {
		return ArtifactResult{}, ErrArtifactUnavailable
	}
	published, err := s.GetDeploymentRevision(ctx, c.TenantID, c.DeploymentID, c.ActorUserID, c.RevisionNumber)
	if err != nil {
		return ArtifactResult{}, err
	}
	content, err := domain.ValidateManifestContent(published.Manifest.Content, published.Manifest.ContentDigest)
	if err != nil {
		return ArtifactResult{}, ErrPublicationIntegrity
	}
	a, ok := content.Resources.Storage["artifact"]
	if !ok || a.Credentials == nil || a.Backend == nil {
		return ArtifactResult{}, ErrArtifactForbidden
	}
	enabled := false
	for _, n := range content.AgentPlan.Nodes {
		if n.Artifact != nil && n.Artifact.Enabled {
			enabled = true
		}
	}
	if !enabled {
		return ArtifactResult{}, ErrArtifactForbidden
	}
	if int64(len(c.Content)) > a.Backend.Limits.MaxBytes {
		return ArtifactResult{}, ErrArtifactTooLarge
	}
	uses := []profileapp.CredentialUse{{CredentialID: a.Credentials.AccessKeyID.CredentialID, Purpose: "access_key_id", AudienceDigest: a.Credentials.AccessKeyID.AudienceDigest}, {CredentialID: a.Credentials.SecretAccessKey.CredentialID, Purpose: "secret_access_key", AudienceDigest: a.Credentials.SecretAccessKey.AudienceDigest}}
	batch, err := s.deps.ArtifactCredentials.ResolveArtifactForOwner(ctx, profileapp.CheckProfileCredentialsCommand{TenantID: c.TenantID, ProfileID: content.Sources.Profile.ProfileID, ActorUserID: c.ActorUserID, ProfileRevisionNumber: content.Sources.Profile.RevisionNumber, Uses: uses})
	if err != nil {
		return ArtifactResult{}, ErrArtifactUnavailable
	}
	defer batch.Clear()
	if len(batch.Credentials) != 2 {
		return ArtifactResult{}, ErrArtifactUnavailable
	}
	request := ArtifactBackendRequest{TenantID: c.TenantID, RunID: c.RunID, ManifestRef: published.Manifest.ID, ManifestDigest: published.Manifest.ContentDigest, DeploymentRevisionID: published.Revision.ID, Name: c.Name, Version: c.Version, Operation: c.Operation, Content: c.Content, MimeType: c.MIMEType, Credentials: ArtifactSecrets{AccessKeyID: string(batch.Credentials[0].Value), SecretAccessKey: string(batch.Credentials[1].Value)}}
	result, err := s.deps.ArtifactBackend.ExecuteArtifact(ctx, request)
	request.Credentials = ArtifactSecrets{}
	if err != nil {
		return ArtifactResult{}, err
	}
	if result.Name != c.Name || result.Version < 0 || result.SizeBytes < 0 || int64(result.SizeBytes) > a.Backend.Limits.MaxBytes || result.SizeBytes > MaxArtifactContentBytes || len(result.MimeType) > 256 || strings.ContainsAny(result.MimeType, "\r\n\x00") {
		return ArtifactResult{}, ErrArtifactUnavailable
	}
	if c.Operation == "load" {
		sum := sha256.Sum256(result.Content)
		if len(result.Content) != result.SizeBytes || result.SHA256 != hex.EncodeToString(sum[:]) || (c.Version != nil && result.Version != *c.Version) {
			clear(result.Content)
			return ArtifactResult{}, ErrArtifactUnavailable
		}
	} else {
		result.Content = nil
	}
	return result, nil
}
