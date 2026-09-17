package preprocess

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
)

var (
	ErrMediaRejected        = errors.New("media rejected by security scan")
	ErrMediaScanUnavailable = errors.New("media security scan unavailable")
)

type ScanVerdict string

const (
	ScanClean    ScanVerdict = "clean"
	ScanRejected ScanVerdict = "rejected"
	ScanUnknown  ScanVerdict = "unknown"
)

type ScanResult struct {
	Verdict ScanVerdict
	Version string
}

type MalwareScanner interface {
	ScanMedia(context.Context, []byte, string) (ScanResult, error)
}

type InputDLPScanner interface {
	ScanMediaInput(context.Context, string, []byte, string) (ScanResult, error)
}

type MediaStageRequest struct {
	TenantID, RequestID, Channel, ChannelBindingID, ExternalAccountID string
	ConfigVersion                                                     int64
	Ordinal                                                           int
	Media                                                             channel.MediaRef
}

type StagedMedia struct {
	ArtifactID, ArtifactRef, ContentDigest, MediaType, Kind string
	Size                                                    int64
}

type MediaStager struct {
	Fetcher      MediaFetcher
	Malware      MalwareScanner
	DLP          InputDLPScanner
	Artifacts    artifact.Store
	MaxBytes     int64
	AllowedTypes map[string]struct{}
}

func (s MediaStager) Stage(ctx context.Context, request MediaStageRequest) (StagedMedia, error) {
	if s.Fetcher == nil || s.Malware == nil || s.DLP == nil || s.Artifacts == nil ||
		request.TenantID == "" || request.RequestID == "" || request.Channel == "" || request.Ordinal < 0 ||
		request.Media.ID == "" || (request.Media.Kind != "image" && request.Media.Kind != "file") {
		return StagedMedia{}, runtime.ErrInvalidEnvelope
	}
	maximum := s.MaxBytes
	if maximum <= 0 {
		maximum = 10 << 20
	}
	if request.Media.Size < 0 || request.Media.Size > maximum {
		return StagedMedia{}, runtime.ErrInvalidEnvelope
	}
	download, err := s.Fetcher.Fetch(ctx, MediaFetchRequest{TenantID: request.TenantID, RequestID: request.RequestID,
		Channel: request.Channel, ChannelBindingID: request.ChannelBindingID, ExternalAccountID: request.ExternalAccountID,
		ConfigVersion: request.ConfigVersion, Ordinal: request.Ordinal, Media: request.Media})
	if err != nil {
		return StagedMedia{}, err
	}
	if download.Body == nil {
		return StagedMedia{}, runtime.ErrBackendUnavailable
	}
	defer download.Body.Close()
	if download.DeclaredSize > maximum || download.DeclaredSize < -1 || download.ContentEncoding != "" {
		return StagedMedia{}, runtime.ErrInvalidEnvelope
	}
	content, err := io.ReadAll(io.LimitReader(download.Body, maximum+1))
	if err != nil {
		return StagedMedia{}, runtime.ErrBackendUnavailable
	}
	if len(content) == 0 || int64(len(content)) > maximum ||
		(download.DeclaredSize >= 0 && download.DeclaredSize != int64(len(content))) ||
		(request.Media.Size > 0 && request.Media.Size != int64(len(content))) {
		return StagedMedia{}, runtime.ErrInvalidEnvelope
	}
	mediaType, err := validateMediaType(content, download.ContentType, request.Media.ContentType, request.Media.Kind, s.AllowedTypes)
	if err != nil {
		return StagedMedia{}, err
	}
	malware, err := s.Malware.ScanMedia(ctx, content, mediaType)
	if err != nil || malware.Verdict == ScanUnknown || malware.Version == "" {
		return StagedMedia{}, ErrMediaScanUnavailable
	}
	if malware.Verdict != ScanClean {
		return StagedMedia{}, ErrMediaRejected
	}
	dlp, err := s.DLP.ScanMediaInput(ctx, request.TenantID, content, mediaType)
	if err != nil || dlp.Verdict == ScanUnknown || dlp.Version == "" {
		return StagedMedia{}, ErrMediaScanUnavailable
	}
	if dlp.Verdict != ScanClean {
		return StagedMedia{}, ErrMediaRejected
	}
	sourceDigest := mediaSourceDigest(request)
	artifactID, artifactRef, err := artifact.StableIdentity(request.TenantID, request.RequestID, request.Ordinal, sourceDigest)
	if err != nil {
		return StagedMedia{}, err
	}
	contentSum := sha256.Sum256(content)
	contentDigest := hex.EncodeToString(contentSum[:])
	record, err := s.Artifacts.PutArtifact(ctx, artifact.Record{TenantID: request.TenantID, RequestID: request.RequestID,
		ArtifactID: artifactID, ArtifactRef: artifactRef, Ordinal: request.Ordinal, SourceDigest: sourceDigest,
		ContentDigest: contentDigest, MediaType: mediaType, Kind: request.Media.Kind, Content: content,
		MalwareScanVersion: malware.Version, DLPVersion: dlp.Version})
	if err != nil {
		return StagedMedia{}, err
	}
	if record.ArtifactID != artifactID || record.ArtifactRef != artifactRef || record.ContentDigest != contentDigest ||
		record.MediaType != mediaType || record.Kind != request.Media.Kind {
		return StagedMedia{}, runtime.ErrInvariantViolation
	}
	return StagedMedia{ArtifactID: artifactID, ArtifactRef: artifactRef, ContentDigest: contentDigest,
		MediaType: mediaType, Kind: request.Media.Kind, Size: int64(len(content))}, nil
}

func mediaSourceDigest(request MediaStageRequest) string {
	value, _ := json.Marshal(struct {
		Channel, ID, MessageID, Kind, ContentType string
		Size                                      int64
	}{Channel: request.Channel, ID: request.Media.ID, MessageID: request.Media.MessageID, Kind: request.Media.Kind,
		ContentType: request.Media.ContentType, Size: request.Media.Size})
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func validateMediaType(content []byte, responseType, claimedType, kind string, allowed map[string]struct{}) (string, error) {
	detected, _, err := mime.ParseMediaType(http.DetectContentType(content))
	if err != nil {
		return "", runtime.ErrInvalidEnvelope
	}
	response, err := canonicalMediaType(responseType)
	if err != nil || (response != "" && response != "application/octet-stream" && response != detected) {
		return "", runtime.ErrInvalidEnvelope
	}
	claimed, err := canonicalMediaType(claimedType)
	if err != nil || (claimed != "" && claimed != detected) {
		return "", runtime.ErrInvalidEnvelope
	}
	accepted := allowed
	if len(accepted) == 0 {
		accepted = map[string]struct{}{"image/jpeg": {}, "image/png": {}, "image/gif": {}, "image/webp": {},
			"application/pdf": {}, "text/plain": {}}
	}
	if _, ok := accepted[detected]; !ok {
		return "", runtime.ErrCapabilityUnsupported
	}
	if kind == "image" && !strings.HasPrefix(detected, "image/") {
		return "", runtime.ErrInvalidEnvelope
	}
	if kind == "file" && strings.HasPrefix(detected, "image/") {
		return "", runtime.ErrInvalidEnvelope
	}
	if detected == "text/plain" && (!utf8.Valid(content) || strings.IndexByte(string(content), 0) >= 0) {
		return "", runtime.ErrInvalidEnvelope
	}
	return detected, nil
}

func canonicalMediaType(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return "", runtime.ErrInvalidEnvelope
	}
	return strings.ToLower(mediaType), nil
}
