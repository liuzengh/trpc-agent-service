package channels

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// pngBytes returns a real 1x1 PNG so image sniffing sees actual image data.
func pngBytes(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

func TestBuildUserContentPlainTextIsUnchanged(t *testing.T) {
	msg, refs := buildUserContent(&InboundMessage{Content: "你好"})
	if msg.Content != "你好" {
		t.Errorf("plain text content = %q, want 你好", msg.Content)
	}
	if len(msg.ContentParts) != 0 || len(refs) != 0 {
		t.Errorf("plain text must not grow parts/refs: %+v %+v", msg.ContentParts, refs)
	}
}

// TestBuildUserContentInlinesReadableImage: the model must receive the picture
// itself, not a platform URL it could never fetch, and the manifest must name it.
func TestBuildUserContentInlinesReadableImage(t *testing.T) {
	data := pngBytes(t)
	msg, refs := buildUserContent(&InboundMessage{
		Content: "这是什么？",
		Media: []MediaAttachment{
			{Kind: MediaImage, Name: "photo.png", MimeType: "image/png", Data: data},
		},
	})
	if len(msg.ContentParts) != 1 {
		t.Fatalf("content parts = %d, want 1", len(msg.ContentParts))
	}
	part := msg.ContentParts[0]
	if part.Type != model.ContentTypeImage || part.Image == nil {
		t.Fatalf("expected an image part, got %+v", part)
	}
	if !bytes.Equal(part.Image.Data, data) {
		t.Error("image bytes must be carried inline")
	}
	if part.Image.Format != "png" {
		t.Errorf("image format = %q, want png (the provider builds data:image/<format>)", part.Image.Format)
	}
	if !strings.Contains(msg.Content, "photo.png") {
		t.Errorf("manifest must name the attachment: %q", msg.Content)
	}
	if !strings.Contains(msg.Content, "这是什么？") {
		t.Errorf("user text must be preserved: %q", msg.Content)
	}
	if len(refs) != 1 || refs[0].Size != len(data) || refs[0].Name != "photo.png" {
		t.Errorf("media ref = %+v, want one sized/name descriptor", refs)
	}
	if refs[0].FetchError != "" {
		t.Errorf("readable attachment must not carry a fetch error: %+v", refs[0])
	}
}

// TestBuildUserContentFallsBackToFilePartForUnknownImage: the OpenAI-compatible
// providers build "data:image/<format>", so an image whose encoding cannot be
// determined must travel as a file part instead of producing a broken URL.
func TestBuildUserContentFallsBackToFilePartForUnknownImage(t *testing.T) {
	msg, refs := buildUserContent(&InboundMessage{
		Media: []MediaAttachment{
			{Kind: MediaImage, Name: "weird.bin", Data: []byte("not-an-image")},
		},
	})
	if len(msg.ContentParts) != 1 || msg.ContentParts[0].Type != model.ContentTypeFile {
		t.Fatalf("expected a file part fallback, got %+v", msg.ContentParts)
	}
	if msg.ContentParts[0].File.Name != "weird.bin" {
		t.Errorf("file part name = %q", msg.ContentParts[0].File.Name)
	}
	if len(refs) != 1 || refs[0].Size != len("not-an-image") {
		t.Errorf("media ref = %+v", refs)
	}
}

// TestBuildUserContentReportsUnreadableAttachment: a failed download must not be
// silent — the model gets a manifest line saying so and the worker gets a ref it
// can turn into a user-visible receipt.
func TestBuildUserContentReportsUnreadableAttachment(t *testing.T) {
	msg, refs := buildUserContent(&InboundMessage{
		Content: "看下这个文件",
		Media: []MediaAttachment{
			{Kind: MediaFile, Name: "report.pdf", FetchError: "download failed"},
		},
	})
	if len(msg.ContentParts) != 0 {
		t.Errorf("unreadable attachment must not become a content part: %+v", msg.ContentParts)
	}
	if !strings.Contains(msg.Content, "report.pdf") || !strings.Contains(msg.Content, "读取失败") {
		t.Errorf("manifest must report the failure: %q", msg.Content)
	}
	if len(refs) != 1 || refs[0].FetchError != "download failed" || refs[0].Size != 0 {
		t.Errorf("media ref must carry the fetch error: %+v", refs)
	}
}

// TestBuildUserContentImageOnlyMessageKeepsPromptNonEmpty: a bare image with no
// caption must still produce a usable prompt.
func TestBuildUserContentImageOnlyMessageKeepsPromptNonEmpty(t *testing.T) {
	msg, _ := buildUserContent(&InboundMessage{
		Media: []MediaAttachment{{Kind: MediaImage, Name: "p.png", MimeType: "image/png", Data: pngBytes(t)}},
	})
	if strings.TrimSpace(msg.Content) == "" {
		t.Fatal("image-only message produced an empty prompt")
	}
}

// TestBuildUserContentGeneratesNamesForUnnamedAttachments keeps the manifest, the
// audit row and the archived artifact agreeing on a filename.
func TestBuildUserContentGeneratesNamesForUnnamedAttachments(t *testing.T) {
	_, refs := buildUserContent(&InboundMessage{
		Media: []MediaAttachment{{Kind: MediaImage, MimeType: "image/jpeg", Data: pngBytes(t)}},
	})
	if len(refs) != 1 {
		t.Fatalf("refs = %+v", refs)
	}
	if refs[0].Name != "image-1.jpg" {
		t.Errorf("generated name = %q, want image-1.jpg", refs[0].Name)
	}
}
