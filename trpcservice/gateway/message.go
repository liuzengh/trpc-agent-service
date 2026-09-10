package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func buildUserMessage(ctx context.Context, reader attachment.Reader, tenantID, eventID string, inbound InboundMessage) (trpcmodel.Message, error) {
	message := trpcmodel.Message{Role: trpcmodel.RoleUser, Content: inbound.Content}
	if len(inbound.Attachments) == 0 {
		return message, nil
	}
	if reader == nil {
		return trpcmodel.Message{}, fmt.Errorf("attachment reader is required")
	}
	for index, reference := range inbound.Attachments {
		content, err := reader.Load(ctx, tenantID, eventID, reference)
		if err != nil {
			return trpcmodel.Message{}, fmt.Errorf("load attachment %d: %w", index, err)
		}
		if err := content.Validate(reference); err != nil {
			return trpcmodel.Message{}, fmt.Errorf("validate attachment %d: %w", index, err)
		}
		message.ContentParts = append(message.ContentParts, contentPart(reference, content))
	}
	return message, nil
}

func contentPart(reference attachment.Reference, content attachment.Content) trpcmodel.ContentPart {
	switch reference.Kind {
	case attachment.KindImage:
		return trpcmodel.ContentPart{Type: trpcmodel.ContentTypeImage, Image: &trpcmodel.Image{Data: content.Data, Detail: "auto", Format: mediaSubtype(reference.MIMEType)}}
	case attachment.KindAudio:
		return trpcmodel.ContentPart{Type: trpcmodel.ContentTypeAudio, Audio: &trpcmodel.Audio{Data: content.Data, Format: mediaSubtype(reference.MIMEType)}}
	case attachment.KindVideo:
		return trpcmodel.ContentPart{Type: trpcmodel.ContentTypeVideo, Video: &trpcmodel.Video{Data: content.Data, Format: mediaSubtype(reference.MIMEType)}}
	default:
		return trpcmodel.ContentPart{Type: trpcmodel.ContentTypeFile, File: &trpcmodel.File{Name: reference.Name, Data: content.Data, MimeType: reference.MIMEType}}
	}
}

func mediaSubtype(contentType string) string {
	if _, subtype, found := strings.Cut(contentType, "/"); found {
		return subtype
	}
	return contentType
}
