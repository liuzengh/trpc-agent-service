package agent

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"path/filepath"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func requiredModelInputKinds(files []channels.InboundFile, documents documentInputExtractor) []config.ModelInputKind {
	if len(files) == 0 {
		return nil
	}
	seen := make(map[config.ModelInputKind]struct{}, 3)
	for _, file := range files {
		mimeType := inputMimeType(file.Name, file.MimeType)
		kind := config.ModelInputFile
		switch {
		case strings.HasPrefix(mimeType, "image/"):
			kind = config.ModelInputImage
		case strings.HasPrefix(mimeType, "audio/"):
			kind = config.ModelInputAudio
		case isTextInput(file.Name, mimeType):
			continue
		case documents != nil && documents.SupportsDocument(file.Name):
			continue
		}
		seen[kind] = struct{}{}
	}
	ordered := make([]config.ModelInputKind, 0, len(seen))
	for _, kind := range []config.ModelInputKind{config.ModelInputImage, config.ModelInputAudio, config.ModelInputFile} {
		if _, ok := seen[kind]; ok {
			ordered = append(ordered, kind)
		}
	}
	return ordered
}

func (r *Runtime) buildUserMessage(ctx context.Context, tenantConfig config.TenantConfig, sessionKey string, inbound channels.InboundMessage) (model.Message, error) {
	message := model.NewUserMessage(inbound.Text)
	if len(inbound.Files) == 0 {
		return message, nil
	}
	if r.artifacts == nil {
		return model.Message{}, errors.New("artifact provider is required for file inputs")
	}
	service, err := r.artifacts.ArtifactService(ctx, tenantConfig)
	if err != nil {
		return model.Message{}, fmt.Errorf("resolve file input storage: %w", err)
	}
	info := agentartifact.SessionInfo{AppName: tenantConfig.AppName(), UserID: inbound.SubjectID, SessionID: sessionKey}
	for _, file := range inbound.Files {
		version := file.Version
		artifact, err := service.LoadArtifact(ctx, info, file.ArtifactName, &version)
		if err != nil {
			return model.Message{}, fmt.Errorf("load file input %q: %w", file.Name, err)
		}
		if artifact == nil {
			return model.Message{}, fmt.Errorf("load file input %q: artifact not found", file.Name)
		}
		name := strings.TrimSpace(file.Name)
		if name == "" {
			name = strings.TrimSpace(artifact.Name)
		}
		if name == "" {
			name = file.ArtifactName
		}
		mimeType := inputMimeType(name, firstNonEmpty(artifact.MimeType, file.MimeType))
		switch {
		case strings.HasPrefix(mimeType, "image/"):
			message.AddImageData(artifact.Data, "auto", mimeType)
		case strings.HasPrefix(mimeType, "audio/"):
			format, err := audioInputFormat(name, mimeType)
			if err != nil {
				return model.Message{}, err
			}
			message.AddAudioData(artifact.Data, format)
		case isTextInput(name, mimeType):
			appendTextAttachment(&message, name, string(artifact.Data))
		case r.documentInputs != nil && r.documentInputs.SupportsDocument(name):
			content, err := r.documentInputs.ExtractDocument(ctx, name, artifact.Data)
			if err != nil {
				return model.Message{}, fmt.Errorf("extract file input %q: %w", name, err)
			}
			appendTextAttachment(&message, name, content)
		default:
			message.AddFileData(name, artifact.Data, mimeType)
		}
	}
	return message, nil
}

func inputMimeType(name, declared string) string {
	mimeType := strings.ToLower(strings.TrimSpace(strings.SplitN(declared, ";", 2)[0]))
	if mimeType != "" && mimeType != "application/octet-stream" {
		return mimeType
	}
	if inferred := mime.TypeByExtension(filepath.Ext(name)); inferred != "" {
		return strings.ToLower(strings.TrimSpace(strings.SplitN(inferred, ";", 2)[0]))
	}
	return mimeType
}

func isTextInput(name, mimeType string) bool {
	if strings.HasPrefix(mimeType, "text/") {
		return true
	}
	switch mimeType {
	case "application/json", "application/ld+json", "application/xml", "application/yaml", "application/x-yaml", "application/javascript":
		return true
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".txt", ".md", ".markdown", ".json", ".jsonl", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".log",
		".go", ".rs", ".c", ".h", ".cc", ".cpp", ".hpp", ".java", ".kt", ".kts", ".py", ".js", ".jsx", ".ts", ".tsx",
		".css", ".scss", ".html", ".htm", ".xml", ".sql", ".sh", ".bash", ".zsh", ".fish", ".proto", ".properties", ".env":
		return true
	default:
		return false
	}
}

func audioInputFormat(name, mimeType string) (string, error) {
	switch mimeType {
	case "audio/wav", "audio/x-wav", "audio/wave":
		return "wav", nil
	case "audio/mpeg", "audio/mp3":
		return "mp3", nil
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".wav":
		return "wav", nil
	case ".mp3":
		return "mp3", nil
	default:
		return "", fmt.Errorf("audio file input %q has unsupported format", name)
	}
}

func appendTextAttachment(message *model.Message, name, content string) {
	content = strings.TrimSpace(content)
	if content == "" {
		return
	}
	if strings.TrimSpace(message.Content) != "" {
		message.Content += "\n\n"
	}
	message.Content += "附件「" + name + "」内容：\n" + content
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (r *Runtime) deleteInboundFiles(ctx context.Context, tenantConfig config.TenantConfig, sessionKey string, inbound channels.InboundMessage) error {
	if len(inbound.Files) == 0 || r.artifacts == nil {
		return nil
	}
	service, err := r.artifacts.ArtifactService(ctx, tenantConfig)
	if err != nil {
		return fmt.Errorf("resolve inbound file storage: %w", err)
	}
	info := agentartifact.SessionInfo{AppName: tenantConfig.AppName(), UserID: inbound.SubjectID, SessionID: sessionKey}
	var failures []error
	for _, file := range inbound.Files {
		if err := service.DeleteArtifact(ctx, info, file.ArtifactName); err != nil {
			failures = append(failures, fmt.Errorf("delete %q: %w", file.Name, err))
		}
	}
	return errors.Join(failures...)
}
