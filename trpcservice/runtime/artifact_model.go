package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// ErrUnsupportedModelAttachment identifies a model capability mismatch. It
// is intentionally separate from provider and infrastructure failures.
var ErrUnsupportedModelAttachment = worker.ErrUnsupportedAttachment

// UnsupportedModelAttachmentError reports the content type rejected by the
// configured model capability allowlist.
type UnsupportedModelAttachmentError struct {
	ContentType model.ContentType
}

func (e UnsupportedModelAttachmentError) Error() string {
	contentType := string(e.ContentType)
	if contentType == "" {
		contentType = "unknown"
	}
	return fmt.Sprintf("%s: %s", ErrUnsupportedModelAttachment, contentType)
}

func (e UnsupportedModelAttachmentError) Unwrap() error { return ErrUnsupportedModelAttachment }

// artifactHydratingModel keeps the durable model request reference-only while
// restoring bytes immediately before the provider call. This keeps provider
// media out of session events, execution commands, and logs.
type artifactHydratingModel struct {
	model.Model
	artifacts                    frameworkartifact.Service
	info                         frameworkartifact.SessionInfo
	capabilities                 tenant.ModelAttachmentCapabilities
	currentArtifactRefs          []string
	currentMessageHasAttachments bool
}

func (m *artifactHydratingModel) GenerateContent(
	ctx context.Context,
	request *model.Request,
) (<-chan *model.Response, error) {
	hydratedRequest, err := cloneRequest(request)
	if err != nil {
		return nil, err
	}
	if m == nil || m.Model == nil {
		return nil, errors.New("artifact hydrating model is not initialized")
	}
	if err := m.sanitizeHistoricalAttachments(hydratedRequest); err != nil {
		return nil, worker.NewPermanentExecutionError(err)
	}
	if err := validateModelAttachmentCapabilities(hydratedRequest, m.capabilities); err != nil {
		return nil, worker.NewPermanentExecutionError(err)
	}
	if err := m.hydrateRequest(ctx, hydratedRequest); err != nil {
		if errors.Is(err, ErrUnsupportedModelAttachment) {
			return nil, worker.NewPermanentExecutionError(err)
		}
		return nil, err
	}
	if decision := guardrail.CheckInputRequest(*hydratedRequest); decision.Blocked {
		return nil, worker.NewPermanentExecutionError(guardrail.InputBlockedError{RuleID: decision.RuleID})
	}
	return m.Model.GenerateContent(ctx, hydratedRequest)
}

// validateCurrentMessage performs the same attachment checks as the model
// boundary, but before Runner persists the user message into the session.
// This is the important failure boundary: a permanently unsupported inbound
// attachment must never poison all later text turns in that session.
func (m *artifactHydratingModel) validateCurrentMessage(ctx context.Context, message model.Message) error {
	if m == nil || m.Model == nil {
		return errors.New("artifact hydrating model is not initialized")
	}
	if !messageHasAttachments(message) {
		return validateInputMessage(message)
	}
	validator := *m
	validator.currentMessageHasAttachments = true
	validator.currentArtifactRefs = artifactRefsFromMessage(message)
	request, err := cloneRequest(&model.Request{Messages: []model.Message{message}})
	if err != nil {
		return err
	}
	if err := validator.sanitizeHistoricalAttachments(request); err != nil {
		return worker.NewPermanentExecutionError(err)
	}
	if err := validateModelAttachmentCapabilities(request, validator.capabilities); err != nil {
		return worker.NewPermanentExecutionError(err)
	}
	if err := validator.hydrateRequest(ctx, request); err != nil {
		if errors.Is(err, ErrUnsupportedModelAttachment) {
			return worker.NewPermanentExecutionError(err)
		}
		return err
	}
	if err := validateInputMessage(request.Messages[0]); err != nil {
		return err
	}
	return nil
}

func validateInputMessage(message model.Message) error {
	decision := guardrail.CheckInputMessage(message)
	if !decision.Blocked {
		return nil
	}
	return worker.NewPermanentExecutionError(guardrail.InputBlockedError{RuleID: decision.RuleID})
}

func messageHasAttachments(message model.Message) bool {
	for partIndex := range message.ContentParts {
		part := &message.ContentParts[partIndex]
		if part.ContentRef != nil || part.Image != nil || part.Audio != nil || part.File != nil {
			return true
		}
		contentType, known := resolvedAttachmentType(part, "")
		if known && contentType != model.ContentTypeText {
			return true
		}
	}
	return false
}

func artifactRefsFromMessage(message model.Message) []string {
	refs := make([]string, 0, len(message.ContentParts))
	for partIndex := range message.ContentParts {
		part := &message.ContentParts[partIndex]
		if part.ContentRef != nil && part.ContentRef.ArtifactRef != "" {
			refs = append(refs, part.ContentRef.ArtifactRef)
		}
	}
	return refs
}

func (m *artifactHydratingModel) isCurrentAttachment(part *model.ContentPart) bool {
	if m == nil || !m.currentMessageHasAttachments || part == nil {
		return false
	}
	if part.ContentRef == nil {
		return true
	}
	for _, ref := range m.currentArtifactRefs {
		if ref != "" && ref == part.ContentRef.ArtifactRef {
			return true
		}
	}
	return false
}

// sanitizeHistoricalAttachments keeps unsupported historical media from
// reaching a model that cannot consume it. Current media is never sanitized:
// it must fail before session persistence instead.
func (m *artifactHydratingModel) sanitizeHistoricalAttachments(request *model.Request) error {
	if request == nil {
		return errors.New("request cannot be nil")
	}
	for messageIndex := range request.Messages {
		message := &request.Messages[messageIndex]
		parts := message.ContentParts
		kept := parts[:0]
		for partIndex := range parts {
			part := &parts[partIndex]
			contentType, known := resolvedAttachmentType(part, "")
			if !known {
				kept = append(kept, *part)
				continue
			}
			if err := validateModelAttachmentType(contentType, m.capabilities); err != nil {
				if m.isCurrentAttachment(part) {
					return fmt.Errorf("message %d content part %d: %w", messageIndex, partIndex, err)
				}
				appendUnsupportedAttachmentNotice(message, contentType)
				continue
			}
			kept = append(kept, *part)
		}
		message.ContentParts = kept
	}
	return nil
}

func appendUnsupportedAttachmentNotice(message *model.Message, contentType model.ContentType) {
	if message == nil {
		return
	}
	notice := fmt.Sprintf("[历史附件已省略：当前模型不支持 %s]", contentType)
	if strings.TrimSpace(message.Content) == "" {
		message.Content = notice
		return
	}
	message.Content += "\n" + notice
}

func cloneRequest(request *model.Request) (*model.Request, error) {
	if request == nil {
		return nil, errors.New("request cannot be nil")
	}
	clone := *request
	clone.Messages = append([]model.Message(nil), request.Messages...)
	for messageIndex := range clone.Messages {
		message := &clone.Messages[messageIndex]
		message.ContentParts = append([]model.ContentPart(nil), message.ContentParts...)
		for partIndex := range message.ContentParts {
			part := &message.ContentParts[partIndex]
			if part.ContentRef != nil {
				contentRef := *part.ContentRef
				part.ContentRef = &contentRef
			}
			if part.File != nil {
				file := *part.File
				file.Data = bytes.Clone(part.File.Data)
				part.File = &file
			}
			if part.Image != nil {
				image := *part.Image
				image.Data = bytes.Clone(part.Image.Data)
				part.Image = &image
			}
			if part.Audio != nil {
				audio := *part.Audio
				audio.Data = bytes.Clone(part.Audio.Data)
				part.Audio = &audio
			}
			if part.Video != nil {
				video := *part.Video
				video.Data = bytes.Clone(part.Video.Data)
				part.Video = &video
			}
		}
	}
	return &clone, nil
}

func (m *artifactHydratingModel) hydrateRequest(ctx context.Context, request *model.Request) error {
	if request == nil {
		return errors.New("request cannot be nil")
	}
	if m == nil || m.Model == nil {
		return errors.New("artifact hydrating model is not initialized")
	}
	for messageIndex := range request.Messages {
		message := &request.Messages[messageIndex]
		parts := message.ContentParts
		kept := parts[:0]
		for partIndex := range parts {
			part := &parts[partIndex]
			if part.ContentRef == nil {
				kept = append(kept, *part)
				continue
			}
			if m.artifacts == nil {
				return errors.New("artifact service is required for content references")
			}
			if err := m.hydratePart(ctx, part); err != nil {
				var unsupported UnsupportedModelAttachmentError
				if errors.As(err, &unsupported) && !m.isCurrentAttachment(part) {
					appendUnsupportedAttachmentNotice(message, unsupported.ContentType)
					continue
				}
				return fmt.Errorf("hydrate message %d content part %d: %w", messageIndex, partIndex, err)
			}
			kept = append(kept, *part)
		}
		message.ContentParts = kept
	}
	return nil
}

func (m *artifactHydratingModel) hydratePart(ctx context.Context, part *model.ContentPart) error {
	name, version, err := parseArtifactRef(part.ContentRef)
	if err != nil {
		return err
	}
	artifactValue, err := m.artifacts.LoadArtifact(ctx, m.info, name, &version)
	if err != nil {
		return fmt.Errorf("load %s@%d: %w", name, version, err)
	}
	if artifactValue == nil {
		return fmt.Errorf("artifact not found: %s@%d", name, version)
	}
	if err := validateLoadedArtifact(part.ContentRef, artifactValue.Data); err != nil {
		return err
	}
	contentType, _ := resolvedAttachmentType(part, chooseArtifactMimeType(part.ContentRef, artifactValue.MimeType))
	if err := validateModelAttachmentType(contentType, m.capabilities); err != nil {
		return err
	}
	part.Type = contentType
	switch part.Type {
	case model.ContentTypeFile:
		file := part.File
		if file == nil {
			file = &model.File{}
		}
		file.Data = append(file.Data[:0], artifactValue.Data...)
		if file.Name == "" {
			file.Name = chooseArtifactName(part.ContentRef, artifactValue.Name, name)
		}
		if file.MimeType == "" {
			file.MimeType = chooseArtifactMimeType(part.ContentRef, artifactValue.MimeType)
		}
		part.File = file
	case model.ContentTypeImage:
		image := part.Image
		if image == nil {
			image = &model.Image{}
		}
		image.Data = append(image.Data[:0], artifactValue.Data...)
		if image.Format == "" {
			image.Format = chooseArtifactMimeType(part.ContentRef, artifactValue.MimeType)
		}
		part.Image = image
	case model.ContentTypeAudio:
		audio := part.Audio
		if audio == nil {
			audio = &model.Audio{}
		}
		audio.Data = append(audio.Data[:0], artifactValue.Data...)
		if audio.Format == "" {
			audio.Format = chooseArtifactMimeType(part.ContentRef, artifactValue.MimeType)
		}
		part.Audio = audio
	default:
		return fmt.Errorf("unsupported content reference type %q", part.Type)
	}
	return nil
}

func validateModelAttachmentCapabilities(
	request *model.Request,
	capabilities tenant.ModelAttachmentCapabilities,
) error {
	if request == nil {
		return errors.New("request cannot be nil")
	}
	for messageIndex := range request.Messages {
		for partIndex := range request.Messages[messageIndex].ContentParts {
			part := &request.Messages[messageIndex].ContentParts[partIndex]
			contentType, known := resolvedAttachmentType(part, "")
			if !known {
				continue
			}
			if err := validateModelAttachmentType(contentType, capabilities); err != nil {
				return fmt.Errorf("message %d content part %d: %w", messageIndex, partIndex, err)
			}
		}
	}
	return nil
}

func validateModelAttachmentType(
	contentType model.ContentType,
	capabilities tenant.ModelAttachmentCapabilities,
) error {
	supported := false
	switch contentType {
	case model.ContentTypeText:
		return nil
	case model.ContentTypeImage:
		supported = capabilities.Image
	case model.ContentTypeAudio:
		supported = capabilities.Audio
	case model.ContentTypeFile:
		supported = capabilities.File
	default:
		// Video and unknown content parts have no configured capability in this
		// service, so they fail closed instead of being treated as generic files.
	}
	if supported {
		return nil
	}
	return UnsupportedModelAttachmentError{ContentType: contentType}
}

// resolvedAttachmentType uses the artifact MIME metadata only to distinguish
// image/audio from a generic file. It does not inspect, parse, or transform
// the bytes.
func resolvedAttachmentType(part *model.ContentPart, fallbackMime string) (model.ContentType, bool) {
	if part == nil {
		return "", false
	}
	if part.Type == model.ContentTypeText {
		return model.ContentTypeText, true
	}
	if part.Type == model.ContentTypeImage || part.Type == model.ContentTypeAudio || part.Type == model.ContentTypeVideo {
		return part.Type, true
	}
	if part.Type == model.ContentTypeFile {
		mimeType := strings.ToLower(strings.TrimSpace(fallbackMime))
		if mimeType == "" && part.ContentRef != nil {
			mimeType = strings.ToLower(strings.TrimSpace(part.ContentRef.MimeType))
		}
		if mimeType == "" && part.File != nil {
			mimeType = strings.ToLower(strings.TrimSpace(part.File.MimeType))
		}
		switch {
		case strings.HasPrefix(mimeType, "image/"):
			return model.ContentTypeImage, true
		case strings.HasPrefix(mimeType, "audio/"):
			return model.ContentTypeAudio, true
		case mimeType != "":
			return model.ContentTypeFile, true
		case part.ContentRef != nil && part.File == nil:
			// Artifact metadata is authoritative but is only available after
			// LoadArtifact; defer this check until hydratePart.
			return model.ContentTypeFile, false
		default:
			return model.ContentTypeFile, true
		}
	}
	if part.Type == "" {
		switch {
		case part.Image != nil:
			return model.ContentTypeImage, true
		case part.Audio != nil:
			return model.ContentTypeAudio, true
		case part.File != nil:
			return model.ContentTypeFile, true
		case part.ContentRef != nil:
			mimeType := strings.ToLower(strings.TrimSpace(part.ContentRef.MimeType))
			switch {
			case strings.HasPrefix(mimeType, "image/"):
				return model.ContentTypeImage, true
			case strings.HasPrefix(mimeType, "audio/"):
				return model.ContentTypeAudio, true
			case mimeType != "":
				return model.ContentTypeFile, true
			default:
				return model.ContentTypeFile, false
			}
		default:
			return model.ContentTypeText, true
		}
	}
	return part.Type, true
}

func parseArtifactRef(ref *model.ContentRef) (string, int, error) {
	if ref == nil {
		return "", 0, errors.New("content ref is required")
	}
	if ref.ArtifactName != "" {
		return gateway.ParseArtifactRef(
			"artifact://" + ref.ArtifactName + "@" + strconv.Itoa(ref.ArtifactVersion),
		)
	}
	return gateway.ParseArtifactRef(ref.ArtifactRef)
}

func validateLoadedArtifact(ref *model.ContentRef, data []byte) error {
	if ref.SizeBytes > 0 && int64(len(data)) != ref.SizeBytes {
		return errors.New("artifact size does not match content reference")
	}
	return nil
}

func chooseArtifactName(ref *model.ContentRef, objectName, fallback string) string {
	if ref.OriginalName != "" {
		return ref.OriginalName
	}
	if objectName != "" {
		return objectName
	}
	if ref.ArtifactName != "" {
		return ref.ArtifactName
	}
	return fallback
}

func chooseArtifactMimeType(ref *model.ContentRef, fallback string) string {
	if strings.TrimSpace(fallback) != "" {
		return fallback
	}
	if ref != nil && ref.MimeType != "" {
		return ref.MimeType
	}
	return ""
}
