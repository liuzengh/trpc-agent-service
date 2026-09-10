package storage_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

func TestStorageErrorIdentityRemainsSharedWithPostgresPrimitive(t *testing.T) {
	if storage.ErrStorage != storagepostgres.ErrStorage {
		t.Fatal("runtime and low-level PostgreSQL storage errors diverged")
	}
}

func TestValidationContracts(t *testing.T) {
	if !errors.Is(storage.ValidateTenant(""), storage.ErrInvalid) || storage.ValidateTenant("tenant-a") != nil {
		t.Fatal("tenant validation contract changed")
	}
	if !errors.Is(storage.ValidateSession("", "session"), storage.ErrInvalid) || !errors.Is(storage.ValidateSession("tenant", ""), storage.ErrInvalid) || storage.ValidateSession("tenant", "session") != nil {
		t.Fatal("session validation contract changed")
	}
	valid := [][2]string{{storage.ReplyPending, storage.ReplySending}, {storage.ReplyPending, storage.ReplyRetryable}, {storage.ReplySending, storage.ReplySent}, {storage.ReplySending, storage.ReplyRetryable}, {storage.ReplySending, storage.ReplyDeadLetter}, {storage.ReplyRetryable, storage.ReplySending}, {storage.ReplyRetryable, storage.ReplyDeadLetter}}
	for _, edge := range valid {
		if !storage.ValidateTransition(edge[0], edge[1]) {
			t.Fatalf("valid transition %q -> %q rejected", edge[0], edge[1])
		}
	}
	invalid := [][2]string{{storage.ReplySent, storage.ReplySending}, {storage.ReplyDeadLetter, storage.ReplySending}, {"unknown", storage.ReplySending}}
	for _, edge := range invalid {
		if storage.ValidateTransition(edge[0], edge[1]) {
			t.Fatalf("invalid transition %q -> %q accepted", edge[0], edge[1])
		}
	}
}

func TestReplyTargetValidation(t *testing.T) {
	valid := storage.ReplyTarget{BindingID: "binding-1", ConversationKind: "direct", ReceiverID: "user-1", ThreadID: "topic-1"}
	if err := storage.ValidateReplyTarget(valid); err != nil {
		t.Fatalf("valid target = %v", err)
	}
	for _, target := range []storage.ReplyTarget{
		{BindingID: "binding-1"},
		{BindingID: "binding-1", ConversationKind: "unknown", ReceiverID: "user-1"},
		{BindingID: "binding-1", ConversationKind: "group"},
		{BindingID: "binding-1", ConversationKind: "group", ReceiverID: "user-1", ThreadID: "bad\nthread"},
	} {
		if !errors.Is(storage.ValidateReplyTarget(target), storage.ErrInvalid) {
			t.Fatalf("invalid target accepted: %+v", target)
		}
	}
	if err := storage.ValidateReplyTarget(storage.ReplyTarget{}); err != nil {
		t.Fatalf("legacy zero target = %v", err)
	}
}

func TestNormalizeReplyOutboxMediaContract(t *testing.T) {
	if got, err := storage.NormalizeReplyOutbox(storage.ReplyOutbox{Payload: "hello"}); err != nil || got.Kind != storage.ReplyKindText {
		t.Fatalf("legacy text normalize = %+v, %v", got, err)
	}
	textWithMedia := storage.ReplyOutbox{Kind: storage.ReplyKindText, Fallback: "fallback", Attachment: mediaReference(t, attachment.KindImage, "image/png", "chart.png", []byte("png"))}
	if _, err := storage.NormalizeReplyOutbox(textWithMedia); !errors.Is(err, storage.ErrInvalid) {
		t.Fatalf("text with media = %v", err)
	}

	for _, test := range []struct {
		name string
		kind storage.ReplyKind
		ref  attachment.Reference
	}{
		{name: "image", kind: " Image ", ref: mediaReference(t, attachment.KindImage, "IMAGE/PNG", " chart.png ", []byte("png"))},
		{name: "video", kind: storage.ReplyKindVideo, ref: mediaReference(t, attachment.KindVideo, "video/mp4", "clip.mp4", []byte("mp4"))},
		{name: "audio", kind: storage.ReplyKindAudio, ref: mediaReference(t, attachment.KindAudio, "audio/mpeg", "voice.mp3", []byte("mp3"))},
		{name: "document", kind: storage.ReplyKindDocument, ref: mediaReference(t, attachment.KindDocument, "application/pdf", "brief.pdf", []byte("pdf"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := storage.NormalizeReplyOutbox(storage.ReplyOutbox{Kind: test.kind, Attachment: test.ref, Fallback: "fallback"})
			if err != nil {
				t.Fatalf("NormalizeReplyOutbox = %v", err)
			}
			if got.Kind != storage.ReplyKind(strings.ToLower(strings.TrimSpace(string(test.kind)))) || got.Attachment.MIMEType != strings.ToLower(strings.TrimSpace(test.ref.MIMEType)) || got.Fallback != "fallback" {
				t.Fatalf("normalized media reply = %+v", got)
			}
		})
	}

	for _, test := range []struct {
		name  string
		value storage.ReplyOutbox
	}{
		{name: "invalid kind", value: storage.ReplyOutbox{Kind: "sticker"}},
		{name: "invalid reference", value: storage.ReplyOutbox{Kind: storage.ReplyKindImage, Attachment: attachment.Reference{ID: "bad"}, Fallback: "fallback"}},
		{name: "kind mismatch", value: storage.ReplyOutbox{Kind: storage.ReplyKindImage, Attachment: mediaReference(t, attachment.KindDocument, "application/pdf", "brief.pdf", []byte("pdf")), Fallback: "fallback"}},
		{name: "missing fallback", value: storage.ReplyOutbox{Kind: storage.ReplyKindImage, Attachment: mediaReference(t, attachment.KindImage, "image/png", "chart.png", []byte("png"))}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := storage.NormalizeReplyOutbox(test.value); !errors.Is(err, storage.ErrInvalid) && !errors.Is(err, attachment.ErrInvalid) {
				t.Fatalf("NormalizeReplyOutbox accepted invalid value: %+v err=%v", test.value, err)
			}
		})
	}
}

func mediaReference(t *testing.T, kind attachment.Kind, contentType, name string, data []byte) attachment.Reference {
	t.Helper()
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: "attachment-" + string(kind), Kind: kind, MIMEType: contentType, Name: name, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if _, err := reference.Normalize(); err != nil {
		t.Fatalf("reference = %v", err)
	}
	return reference
}
