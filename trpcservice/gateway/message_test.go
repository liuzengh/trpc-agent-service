package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestInboundMessageNormalizesAttachmentOnlyInput(t *testing.T) {
	reference := testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))
	message, err := (InboundMessage{Attachments: []attachment.Reference{reference}, ExternalMessageID: "message", ExternalUserID: "user", ConversationKind: ConversationDirect, ExternalPeerID: "peer"}).Normalize()
	if err != nil {
		t.Fatalf("Normalize = %v", err)
	}
	if message.ContentType != ContentTypeMedia || len(message.Attachments) != 1 {
		t.Fatalf("normalized message = %+v", message)
	}
}

func TestInboundMessageRejectsAttachmentBoundaries(t *testing.T) {
	reference := testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))
	attachments := make([]attachment.Reference, maxInboundAttachments+1)
	for index := range attachments {
		attachments[index] = reference
	}
	if _, err := (InboundMessage{Attachments: attachments, ExternalMessageID: "message", ExternalUserID: "user", ConversationKind: ConversationDirect, ExternalPeerID: "peer"}).Normalize(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("too many attachments error = %v", err)
	}
	invalid := reference
	invalid.ID = ""
	if _, err := (InboundMessage{Attachments: []attachment.Reference{invalid}, ExternalMessageID: "message", ExternalUserID: "user", ConversationKind: ConversationDirect, ExternalPeerID: "peer"}).Normalize(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid attachment error = %v", err)
	}
}

func TestBuildUserMessageUsesVerifiedContentParts(t *testing.T) {
	data := []byte("image")
	reference := testAttachmentReference(t, attachment.KindImage, "image/png", data)
	reader := attachmentReaderFunc(func(context.Context, string, string, attachment.Reference) (attachment.Content, error) {
		return attachment.Content{Data: data}, nil
	})
	message, err := buildUserMessage(context.Background(), reader, "tenant", "event", InboundMessage{Content: "describe", Attachments: []attachment.Reference{reference}})
	if err != nil {
		t.Fatalf("buildUserMessage = %v", err)
	}
	if message.Content != "describe" || len(message.ContentParts) != 1 || message.ContentParts[0].Type != trpcmodel.ContentTypeImage || string(message.ContentParts[0].Image.Data) != string(data) {
		t.Fatalf("message = %+v", message)
	}
}

func TestContentPartCoversAttachmentFamilies(t *testing.T) {
	for _, test := range []struct {
		name string
		ref  attachment.Reference
		want trpcmodel.ContentType
	}{
		{name: "audio", ref: testAttachmentReference(t, attachment.KindAudio, "audio/mpeg", []byte("mp3")), want: trpcmodel.ContentTypeAudio},
		{name: "video", ref: testAttachmentReference(t, attachment.KindVideo, "video/mp4", []byte("mp4")), want: trpcmodel.ContentTypeVideo},
		{name: "document", ref: testAttachmentReference(t, attachment.KindDocument, "application/pdf", []byte("pdf")), want: trpcmodel.ContentTypeFile},
	} {
		t.Run(test.name, func(t *testing.T) {
			part := contentPart(test.ref, attachment.Content{Data: []byte("content")})
			if part.Type != test.want {
				t.Fatalf("part type = %q, want %q", part.Type, test.want)
			}
		})
	}
	if got := mediaSubtype("application"); got != "application" {
		t.Fatalf("mediaSubtype without slash = %q", got)
	}
}

func TestBuildUserMessageRejectsUnavailableOrTamperedAttachment(t *testing.T) {
	reference := testAttachmentReference(t, attachment.KindDocument, "application/pdf", []byte("document"))
	message := InboundMessage{Attachments: []attachment.Reference{reference}}
	if _, err := buildUserMessage(context.Background(), nil, "tenant", "event", message); err == nil {
		t.Fatal("buildUserMessage accepted nil reader")
	}
	reader := attachmentReaderFunc(func(context.Context, string, string, attachment.Reference) (attachment.Content, error) {
		return attachment.Content{Data: []byte("tampered")}, nil
	})
	if _, err := buildUserMessage(context.Background(), reader, "tenant", "event", message); err == nil {
		t.Fatal("buildUserMessage accepted tampered content")
	}
	reader = attachmentReaderFunc(func(context.Context, string, string, attachment.Reference) (attachment.Content, error) {
		return attachment.Content{}, errors.New("unavailable")
	})
	if _, err := buildUserMessage(context.Background(), reader, "tenant", "event", message); err == nil {
		t.Fatal("buildUserMessage accepted reader error")
	}
}

type attachmentReaderFunc func(context.Context, string, string, attachment.Reference) (attachment.Content, error)

func (function attachmentReaderFunc) Load(ctx context.Context, tenantID, eventID string, reference attachment.Reference) (attachment.Content, error) {
	return function(ctx, tenantID, eventID, reference)
}

func testAttachmentReference(t *testing.T, kind attachment.Kind, contentType string, data []byte) attachment.Reference {
	t.Helper()
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: "attachment-1", Kind: kind, MIMEType: contentType, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if _, err := reference.Normalize(); err != nil {
		t.Fatalf("test reference = %v", err)
	}
	return reference
}
