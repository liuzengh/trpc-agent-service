// media.go turns the attachments of a normalized inbound message into model
// content parts.
//
// Downloading an attachment is not enough: the framework model.Message carries
// non-text input in ContentParts (image / file), so the gateway is the place
// that has both the bytes and the message under construction. Attachments are
// therefore inlined here as real content parts — the model sees the picture
// itself, not a dangling platform URL it could never fetch — while a short
// manifest in the message text names every attachment so that
//
//   - the model knows how many attachments came in and what they are called,
//   - an unreadable attachment is still visible to the model and to the audit
//     trail instead of vanishing,
//   - a message whose only payload is an image is not an empty prompt.
package channels

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// mediaManifestHeader introduces the attachment manifest appended to the user
// text. Kept in Chinese to match the user-facing language of the platform.
const mediaManifestHeader = "\n\n[附件清单]"

// buildUserContent maps one normalized inbound message onto the model message
// the agent runs on, plus the attachment manifest the worker archives and
// audits. A plain text message returns exactly the old behavior.
func buildUserContent(in *InboundMessage) (model.Message, []bus.MediaRef) {
	msg := model.NewUserMessage(in.Content)
	if in == nil || len(in.Media) == 0 {
		return msg, nil
	}
	refs := make([]bus.MediaRef, 0, len(in.Media))
	lines := make([]string, 0, len(in.Media))
	for i, att := range in.Media {
		kind := att.Kind
		if kind == "" {
			kind = MediaFile
		}
		name := mediaName(att, i)
		if len(att.Data) == 0 {
			reason := att.FetchError
			if reason == "" {
				reason = "内容为空"
			}
			refs = append(refs, bus.MediaRef{
				Kind: kind, Name: name, MimeType: att.MimeType, FetchError: reason,
			})
			lines = append(lines, fmt.Sprintf("- %s %s：读取失败（%s）", kind, name, reason))
			continue
		}
		refs = append(refs, bus.MediaRef{
			Kind: kind, Name: name, MimeType: att.MimeType, Size: len(att.Data),
		})
		if kind == MediaImage {
			if format := imageFormat(att.Data, att.MimeType, name); format != "" {
				msg.AddImageData(att.Data, "", format)
				lines = append(lines, fmt.Sprintf("- 图片 %s（%d 字节，图片内容已随本条消息提供）", name, len(att.Data)))
				continue
			}
			// Unrecognized image encoding: the model provider needs a media
			// subtype to build a data URL, so fall through to a file part
			// rather than sending "data:image/;base64".
		}
		msg.AddFileData(name, att.Data, att.MimeType)
		lines = append(lines, fmt.Sprintf("- %s %s（%d 字节，文件内容已随本条消息提供）", kind, name, len(att.Data)))
	}
	msg.Content = strings.TrimRight(msg.Content, "\n") + mediaManifestHeader + "\n" + strings.Join(lines, "\n")
	return msg, refs
}

// mediaName keeps the platform's filename when it has one, otherwise generates a
// stable one from the kind and type, so the manifest, the audit row and the
// archived artifact always agree on a name.
func mediaName(att MediaAttachment, index int) string {
	if n := strings.TrimSpace(att.Name); n != "" {
		return n
	}
	kind := att.Kind
	if kind == "" {
		kind = MediaFile
	}
	return fmt.Sprintf("%s-%d%s", kind, index+1, extensionForMime(att.MimeType))
}

// imageFormat returns the media subtype the model provider needs to build a
// data URL ("png", "jpeg", …), or "" when it cannot be determined — in which
// case the caller sends the payload as a file part instead.
//
// The bytes are sniffed first because they are the only source that cannot lie
// about the actual encoding; the declared MIME type and the filename are
// fallbacks for images the sniffer does not recognize.
func imageFormat(data []byte, mimeType, name string) string {
	if sniffed := http.DetectContentType(data); strings.HasPrefix(sniffed, "image/") {
		switch subtype := strings.TrimPrefix(sniffed, "image/"); subtype {
		case "png", "jpeg", "gif", "webp":
			return subtype
		}
	}
	if declared := strings.TrimSpace(mimeType); strings.HasPrefix(declared, "image/") {
		return strings.TrimPrefix(declared, "image/")
	}
	switch ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), "."); ext {
	case "png", "jpg", "jpeg", "gif", "webp":
		return ext
	}
	return ""
}

// extensionForMime maps a MIME type onto a file extension for generated names.
func extensionForMime(mimeType string) string {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "image/png":
		return ".png"
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "application/pdf":
		return ".pdf"
	case "text/plain":
		return ".txt"
	case "application/zip":
		return ".zip"
	case "audio/amr":
		return ".amr"
	case "audio/mpeg":
		return ".mp3"
	case "audio/wav", "audio/x-wav":
		return ".wav"
	}
	return ""
}
