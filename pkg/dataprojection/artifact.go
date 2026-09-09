package dataprojection

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"strings"
	"unicode"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const artifactKeyPrefix = "artifact/v1/"

type artifactIdentity struct {
	AppName         string `json:"app_name"`
	UserID          string `json:"user_id"`
	SessionID       string `json:"session_id"`
	Filename        string `json:"filename"`
	ArtifactVersion int    `json:"artifact_version"`
	MIMEType        string `json:"mime_type"`
}

type ArtifactVersionStore interface {
	ProjectVersion(context.Context, artifact.SessionInfo, string, int, *artifact.Artifact) error
	DeleteProjectedVersion(context.Context, artifact.SessionInfo, string, int) error
}

type ArtifactStoreResolver func(context.Context, string) (ArtifactVersionStore, error)

type ArtifactProjector struct{ resolve ArtifactStoreResolver }

func NewArtifactProjector(resolve ArtifactStoreResolver) (*ArtifactProjector, error) {
	if resolve == nil {
		return nil, fmt.Errorf("%w: artifact resolver", ErrInvalidProjection)
	}
	return &ArtifactProjector{resolve: resolve}, nil
}

func (p *ArtifactProjector) Domain() datamigration.Domain { return datamigration.DomainArtifact }

func (p *ArtifactProjector) Apply(ctx context.Context, tenantID string, record datamigration.Record) error {
	if p == nil || p.resolve == nil {
		return fmt.Errorf("%w: artifact projector", ErrInvalidProjection)
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: tenant", ErrInvalidProjection)
	}
	if err := record.Validate(); err != nil {
		return err
	}
	identity, err := decodeArtifactIdentity(record.Key)
	if err != nil {
		return err
	}
	store, err := p.resolve(nonNilProjectionContext(ctx), tenantID)
	if err != nil {
		return fmt.Errorf("resolve artifact projection store: %w", err)
	}
	if store == nil {
		return fmt.Errorf("%w: nil artifact store", ErrInvalidProjection)
	}
	info := artifact.SessionInfo{AppName: identity.AppName, UserID: identity.UserID, SessionID: identity.SessionID}
	if record.Deleted {
		return store.DeleteProjectedVersion(nonNilProjectionContext(ctx), info, identity.Filename, identity.ArtifactVersion)
	}
	if len(record.Payload) == 0 {
		return fmt.Errorf("%w: empty artifact body", ErrInvalidProjection)
	}
	if err := store.ProjectVersion(nonNilProjectionContext(ctx), info, identity.Filename, identity.ArtifactVersion,
		&artifact.Artifact{Data: append([]byte(nil), record.Payload...), MimeType: identity.MIMEType, Name: identity.Filename}); err != nil {
		return fmt.Errorf("project artifact version: %w", err)
	}
	return nil
}

func NewArtifactRecord(info artifact.SessionInfo, filename string, artifactVersion int, value *artifact.Artifact, migrationVersion int64) (datamigration.Record, error) {
	if value == nil || len(value.Data) == 0 || migrationVersion <= 0 {
		return datamigration.Record{}, fmt.Errorf("%w: artifact body or migration version", ErrInvalidProjection)
	}
	mediaType, _, err := mime.ParseMediaType(value.MimeType)
	if err != nil {
		return datamigration.Record{}, fmt.Errorf("%w: artifact MIME type", ErrInvalidProjection)
	}
	identity := artifactIdentity{
		AppName: info.AppName, UserID: info.UserID, SessionID: info.SessionID,
		Filename: filename, ArtifactVersion: artifactVersion, MIMEType: mediaType,
	}
	key, err := encodeArtifactIdentity(identity)
	if err != nil {
		return datamigration.Record{}, err
	}
	payload := append([]byte(nil), value.Data...)
	record := datamigration.Record{Key: key, Version: migrationVersion, Payload: payload, Hash: projectionHash(payload)}
	if err := record.Validate(); err != nil {
		return datamigration.Record{}, err
	}
	return record, nil
}

func NewArtifactTombstone(info artifact.SessionInfo, filename string, artifactVersion int, mimeType string, migrationVersion int64) (datamigration.Record, error) {
	mediaType, _, err := mime.ParseMediaType(mimeType)
	if err != nil || migrationVersion <= 0 {
		return datamigration.Record{}, fmt.Errorf("%w: artifact tombstone", ErrInvalidProjection)
	}
	key, err := encodeArtifactIdentity(artifactIdentity{
		AppName: info.AppName, UserID: info.UserID, SessionID: info.SessionID,
		Filename: filename, ArtifactVersion: artifactVersion, MIMEType: mediaType,
	})
	if err != nil {
		return datamigration.Record{}, err
	}
	record := datamigration.Record{Key: key, Version: migrationVersion, Payload: []byte{}, Hash: projectionHash(nil), Deleted: true}
	if err := record.Validate(); err != nil {
		return datamigration.Record{}, err
	}
	return record, nil
}

func encodeArtifactIdentity(identity artifactIdentity) (string, error) {
	if !validArtifactIdentity(identity) {
		return "", fmt.Errorf("%w: artifact identity", ErrInvalidProjection)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("%w: encode artifact identity", ErrInvalidProjection)
	}
	return artifactKeyPrefix + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeArtifactIdentity(key string) (artifactIdentity, error) {
	if !strings.HasPrefix(key, artifactKeyPrefix) {
		return artifactIdentity{}, fmt.Errorf("%w: artifact record key", ErrInvalidProjection)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, artifactKeyPrefix))
	if err != nil {
		return artifactIdentity{}, fmt.Errorf("%w: artifact record key", ErrInvalidProjection)
	}
	var identity artifactIdentity
	if err := decodeStrictJSON(raw, &identity); err != nil {
		return artifactIdentity{}, fmt.Errorf("%w: artifact record key", ErrInvalidProjection)
	}
	canonical, err := encodeArtifactIdentity(identity)
	if err != nil || canonical != key {
		return artifactIdentity{}, fmt.Errorf("%w: non-canonical artifact record key", ErrInvalidProjection)
	}
	return identity, nil
}

func validArtifactIdentity(identity artifactIdentity) bool {
	if err := tenant.ValidateAgentAppName(identity.AppName); err != nil || identity.ArtifactVersion < 0 ||
		!projectionSafeText(identity.UserID, 255) || !projectionSafeText(identity.SessionID, 512) ||
		!projectionSafeText(identity.Filename, 512) || strings.ContainsAny(identity.Filename, "/\\") ||
		identity.Filename == "." || identity.Filename == ".." {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(identity.MIMEType)
	return err == nil && mediaType == identity.MIMEType && strings.Contains(mediaType, "/") && len(mediaType) <= 255
}

func projectionSafeText(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}
