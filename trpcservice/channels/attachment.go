package channels

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strings"
)

const defaultMaxInboundArtifactBytes int64 = 32 << 20

// DownloadedMedia is the short-lived result of a provider media download.
// Its bytes must not be copied into ChannelInput, Gateway, or execution
// events.
type DownloadedMedia struct {
	Filename string
	MIMEType string
	Data     []byte
}

// DetectMediaMIMEType trusts content bytes first. Office Open XML files are
// ZIP containers, so their known extension is used only for that ambiguous
// ZIP result; a wrong image/audio extension cannot upgrade arbitrary bytes.
func DetectMediaMIMEType(filename string, data []byte) string {
	sniffed := strings.ToLower(strings.TrimSpace(http.DetectContentType(data)))
	extensionMIME := ""
	if extension := path.Ext(strings.TrimSpace(filename)); extension != "" {
		extensionMIME = strings.ToLower(strings.TrimSpace(mime.TypeByExtension(strings.ToLower(extension))))
		if extensionMIME == "application/x-zip-compressed" {
			extensionMIME = "application/zip"
		}
	}
	if sniffed != "" && sniffed != "application/octet-stream" && sniffed != "application/zip" {
		return sniffed
	}
	if sniffed == "application/zip" && extensionMIME != "" {
		return extensionMIME
	}
	if sniffed != "" {
		return sniffed
	}
	return "application/octet-stream"
}

// InboundArtifact identifies one deterministic, tenant-scoped artifact write.
// The writer owns persistence and must make the external message/item tuple
// idempotent.
type InboundArtifact struct {
	TenantID          string
	AppID             string
	BindingID         string
	ExternalMessageID string
	ItemNo            int
	ConfigVersion     string
	Kind              MessageType
	Filename          string
	MIMEType          string
	Data              []byte
}

// Validate checks the controlled artifact write request.
func (a InboundArtifact) Validate() error {
	if a.TenantID == "" || a.AppID == "" || a.BindingID == "" || a.ExternalMessageID == "" {
		return errors.New("inbound artifact scope and message are required")
	}
	if a.ItemNo < 0 {
		return errors.New("inbound artifact item number must not be negative")
	}
	if err := a.Kind.Validate(); err != nil {
		return err
	}
	if a.Kind == MessageTypeText || a.Kind == MessageTypeUnsupported {
		return errors.New("inbound artifact kind is invalid")
	}
	if len(a.Data) == 0 {
		return errors.New("inbound artifact data is required")
	}
	if strings.ContainsAny(a.Filename, "\r\n\x00") || strings.ContainsAny(a.MIMEType, "\r\n\x00") {
		return errors.New("inbound artifact metadata is invalid")
	}
	return nil
}

// MediaDownloader resolves a provider media reference within the already
// verified Binding scope.
type MediaDownloader interface {
	Download(context.Context, ChannelInput, ProviderMediaRef) (DownloadedMedia, error)
}

// ArtifactWriter stores one inbound artifact and returns a tenant-scoped
// ArtifactRef. Implementations should use the external message and ItemNo as
// an idempotency key. owned is true only when this call created the durable
// stage; a shared idempotent result must not be compensated by this caller.
type ArtifactWriter interface {
	WriteInboundArtifact(context.Context, InboundArtifact) (artifactRef string, owned bool, err error)
}

// ArtifactCompensator removes a pre-admission artifact without trusting the
// provider reference. Implementations must be idempotent and must not remove
// an artifact that has already been atomically attached by admission.
type ArtifactCompensator interface {
	DeleteInboundArtifact(context.Context, InboundArtifact, string) error
}

// ArtifactIngestor materializes provider media before PostgreSQL admission.
// It is independent of a concrete object store; the supplied ArtifactWriter
// is the only persistence boundary.
type ArtifactIngestor struct {
	downloader MediaDownloader
	writer     ArtifactWriter
	maxBytes   int64
}

// NewArtifactIngestor creates the IM-07 pre-admission media pipeline.
func NewArtifactIngestor(downloader MediaDownloader, writer ArtifactWriter) (*ArtifactIngestor, error) {
	if downloader == nil || writer == nil {
		return nil, errors.New("media downloader and artifact writer are required")
	}
	return &ArtifactIngestor{
		downloader: downloader,
		writer:     writer,
		maxBytes:   defaultMaxInboundArtifactBytes,
	}, nil
}

// WithMaxBytes limits one inbound provider media item in memory.
func (i *ArtifactIngestor) WithMaxBytes(maxBytes int64) error {
	if i == nil {
		return errors.New("artifact ingestor is not initialized")
	}
	if maxBytes <= 0 {
		return errors.New("max inbound artifact bytes must be positive")
	}
	i.maxBytes = maxBytes
	return nil
}

// Prepare downloads and writes media, then returns ChannelInput containing
// only the resulting ArtifactRefs.
func (i *ArtifactIngestor) Prepare(
	ctx context.Context,
	input ChannelInput,
	media []ProviderMediaRef,
) (ChannelInput, error) {
	prepared, _, err := i.prepare(ctx, input, media, "")
	return prepared, err
}

// PreparePinned is the production attachment path. Every write is pinned to
// the immutable config selected by Gateway, and the returned compensator can
// remove all staged objects if admission fails.
func (i *ArtifactIngestor) PreparePinned(
	ctx context.Context,
	input ChannelInput,
	media []ProviderMediaRef,
	configVersion string,
) (ChannelInput, func(context.Context) error, error) {
	if strings.TrimSpace(configVersion) == "" {
		return ChannelInput{}, nil, errors.New("attachment config version is required")
	}
	return i.prepare(ctx, input, media, configVersion)
}

func (i *ArtifactIngestor) prepare(
	ctx context.Context,
	input ChannelInput,
	media []ProviderMediaRef,
	configVersion string,
) (ChannelInput, func(context.Context) error, error) {
	if i == nil || i.downloader == nil || i.writer == nil {
		return ChannelInput{}, nil, errors.New("artifact ingestor is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ChannelInput{}, nil, err
	}
	if err := input.Validate(); err != nil {
		return ChannelInput{}, nil, err
	}
	if len(media) == 0 {
		return input.Clone(), func(context.Context) error { return nil }, nil
	}
	if _, ok := i.writer.(ArtifactCompensator); !ok && configVersion != "" {
		return ChannelInput{}, nil, errors.New("artifact compensation is required for pinned preparation")
	}
	prepared := input.Clone()
	type writtenArtifact struct {
		input InboundArtifact
		ref   string
		owned bool
	}
	written := make([]writtenArtifact, 0, len(media))
	cleanup := func(cleanupCtx context.Context) error {
		compensator, ok := i.writer.(ArtifactCompensator)
		if !ok || len(written) == 0 {
			return nil
		}
		if cleanupCtx == nil {
			cleanupCtx = context.Background()
		}
		var cleanupErr error
		for index := len(written) - 1; index >= 0; index-- {
			if !written[index].owned {
				continue
			}
			cleanupErr = errors.Join(cleanupErr, compensator.DeleteInboundArtifact(
				cleanupCtx,
				written[index].input,
				written[index].ref,
			))
		}
		return cleanupErr
	}
	for itemNo, reference := range media {
		if err := reference.Validate(); err != nil {
			return ChannelInput{}, nil, errors.Join(
				fmt.Errorf("provider media %d: %w", itemNo, err),
				cleanup(context.WithoutCancel(ctx)),
			)
		}
		downloaded, err := i.downloader.Download(ctx, input, reference)
		if err != nil {
			return ChannelInput{}, nil, errors.Join(
				fmt.Errorf("download provider media %d: %w", itemNo, err),
				cleanup(context.WithoutCancel(ctx)),
			)
		}
		if int64(len(downloaded.Data)) > i.maxBytes {
			return ChannelInput{}, nil, errors.Join(
				fmt.Errorf("provider media %d exceeds size limit", itemNo),
				cleanup(context.WithoutCancel(ctx)),
			)
		}
		mimeType := DetectMediaMIMEType(downloaded.Filename, downloaded.Data)
		// The provider-normalized kind is the only reliable fallback when an
		// image format cannot be sniffed. Keep it in the MIME namespace so the
		// worker cannot silently downgrade the attachment to a generic file.
		if reference.Kind == MessageTypeImage && mimeType == "application/octet-stream" {
			mimeType = "image/octet-stream"
		}
		artifact := InboundArtifact{
			TenantID:          input.TenantID,
			AppID:             input.AppID,
			BindingID:         input.BindingID,
			ExternalMessageID: input.ExternalMessageID,
			ItemNo:            itemNo,
			ConfigVersion:     configVersion,
			Kind:              reference.Kind,
			Filename:          downloaded.Filename,
			MIMEType:          mimeType,
			Data:              downloaded.Data,
		}
		artifactRef, owned, err := i.writer.WriteInboundArtifact(ctx, artifact)
		if err != nil {
			return ChannelInput{}, nil, errors.Join(
				fmt.Errorf("write inbound artifact %d: %w", itemNo, err),
				cleanup(context.WithoutCancel(ctx)),
			)
		}
		if !strings.HasPrefix(artifactRef, "artifact://") || strings.TrimSpace(strings.TrimPrefix(artifactRef, "artifact://")) == "" {
			return ChannelInput{}, nil, errors.Join(
				fmt.Errorf("inbound artifact %d returned invalid artifact ref", itemNo),
				cleanup(context.WithoutCancel(ctx)),
			)
		}
		written = append(written, writtenArtifact{
			input: InboundArtifact{
				TenantID: input.TenantID, AppID: input.AppID, BindingID: input.BindingID,
				ExternalMessageID: input.ExternalMessageID, ItemNo: itemNo,
				ConfigVersion: configVersion,
			},
			ref:   artifactRef,
			owned: owned,
		})
		prepared.ArtifactRefs = append(prepared.ArtifactRefs, artifactRef)
	}
	return prepared, cleanup, nil
}

var _ AttachmentIngestor = (*ArtifactIngestor)(nil)
var _ PinnedAttachmentIngestor = (*ArtifactIngestor)(nil)
