package worker

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
)

const (
	maxWorkerContentBytes       = 64 << 10
	maxWorkerMetadataItems      = 64
	maxWorkerMetadataBytes      = 64 << 10
	maxWorkerAttachments        = 32
	maxWorkerAttachmentURL      = 2048
	maxWorkerAttachmentName     = 256
	maxWorkerAttachmentMimeType = 128
)

// ValidateRequest applies the common request boundary shared by local and
// production Worker callers. HTTP gateways should run it after copying any
// trusted traceparent header and before control-plane binding or execution
// records are created.
func ValidateRequest(req *Request) error {
	return validateProcessRequest(req, false)
}

// validateProcessRequest is shared by local and production Worker callers.
// The HTTP handler has an outer JSON body limit, but direct Go callers must
// receive the same prompt and identity bounds before any backend or model
// work starts.
func validateProcessRequest(req *Request, strict bool) error {
	if req == nil {
		return fmt.Errorf("worker request is required")
	}
	fields := []struct {
		name     string
		value    string
		maxBytes int
		required bool
	}{
		{"tenant", req.TenantID, 64, strict},
		{"channel", req.ChannelType, 32, false},
		{"channel account", req.ChannelAccountID, 128, false},
		{"conversation", req.ConversationID, 256, false},
		{"message", req.MessageID, 256, false},
		{"agent app", req.AgentApp, 128, strict},
		{"agent version", req.AgentVersion, 128, strict},
		{"deployment", req.DeploymentID, 128, strict},
		{"idempotency key", req.IdempotencyKey, 256, strict},
		{"user", req.UserID, 255, true},
		{"session owner", req.SessionOwnerID, 255, false},
		{"session", req.SessionID, 255, true},
	}
	for _, field := range fields {
		if field.value == "" && !field.required {
			continue
		}
		if !validWorkerText(field.value, field.maxBytes, false) {
			return fmt.Errorf("%s is invalid", field.name)
		}
	}
	if len(req.Content) > maxWorkerContentBytes || !utf8.ValidString(req.Content) || strings.ContainsRune(req.Content, '\x00') {
		return fmt.Errorf("content must be valid UTF-8 without NUL and at most %d bytes", maxWorkerContentBytes)
	}
	if err := validateWorkerAttachments(req.Attachments, strict); err != nil {
		return err
	}
	if err := validateWorkerMetadata(req.Metadata); err != nil {
		return err
	}
	if req.ApprovalToken != "" {
		if err := governance.ValidateApprovalToken(req.ApprovalToken); err != nil {
			return fmt.Errorf("approval token is invalid")
		}
	}
	if rawTraceParent, exists := req.Metadata["traceparent"]; exists {
		traceParent, ok := rawTraceParent.(string)
		if !ok || (traceParent != "" && !validTraceParent(traceParent)) {
			return fmt.Errorf("traceparent is invalid")
		}
	}
	return nil
}

func validateWorkerAttachments(attachments []channel.Attachment, strict bool) error {
	if len(attachments) > maxWorkerAttachments {
		return fmt.Errorf("attachments exceed %d items", maxWorkerAttachments)
	}
	for _, attachment := range attachments {
		typ := strings.ToLower(strings.TrimSpace(attachment.Type))
		switch typ {
		case "image", "file", "audio", "video":
		default:
			return fmt.Errorf("attachment type is invalid")
		}
		if len(attachment.URL) == 0 || len(attachment.URL) > maxWorkerAttachmentURL {
			return fmt.Errorf("attachment URL is invalid")
		}
		if err := validateAttachmentURL(attachment.URL, strict); err != nil {
			return err
		}
		if !validWorkerText(attachment.Name, maxWorkerAttachmentName, true) ||
			!validWorkerText(attachment.MimeType, maxWorkerAttachmentMimeType, true) || attachment.Size < 0 {
			return fmt.Errorf("attachment metadata is invalid")
		}
	}
	return nil
}

func validWorkerText(value string, maxBytes int, allowEmpty bool) bool {
	if value == "" {
		return allowEmpty
	}
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Cf) {
			return false
		}
	}
	return true
}

func validateWorkerMetadata(metadata map[string]interface{}) error {
	if len(metadata) > maxWorkerMetadataItems {
		return fmt.Errorf("metadata exceeds %d entries", maxWorkerMetadataItems)
	}
	if len(metadata) == 0 {
		return nil
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("metadata is not serializable: %w", err)
	}
	if len(encoded) > maxWorkerMetadataBytes {
		return fmt.Errorf("metadata exceeds %d bytes", maxWorkerMetadataBytes)
	}
	for key := range metadata {
		if !validWorkerText(key, 128, false) {
			return fmt.Errorf("metadata key is invalid")
		}
	}
	return nil
}

func validTraceParent(value string) bool {
	if len(value) != 55 || value[2] != '-' || value[35] != '-' || value[52] != '-' {
		return false
	}
	if strings.EqualFold(value[:2], "ff") {
		return false
	}
	for _, part := range []string{value[:2], value[3:35], value[36:52], value[53:55]} {
		if _, err := hex.DecodeString(part); err != nil {
			return false
		}
	}
	if strings.Trim(value[3:35], "0") == "" || strings.Trim(value[36:52], "0") == "" {
		return false
	}
	return true
}
