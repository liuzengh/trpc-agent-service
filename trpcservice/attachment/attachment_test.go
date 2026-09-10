package attachment

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestReferenceNormalizeAndContentValidate(t *testing.T) {
	data := []byte("image")
	digest := sha256.Sum256(data)
	reference := Reference{ID: " attachment-1 ", Kind: KindImage, MIMEType: " IMAGE/PNG ", Name: " chart.png ", Size: int64(len(data)), SHA256: " " + hex.EncodeToString(digest[:]) + " ", Provider: " Telegram ", ProviderID: " file-1 "}
	normalized, err := reference.Normalize()
	if err != nil || normalized.ID != "attachment-1" || normalized.MIMEType != "image/png" || normalized.Name != "chart.png" || normalized.Provider != "telegram" || normalized.ProviderID != "file-1" {
		t.Fatalf("Normalize = %+v, %v", normalized, err)
	}
	if err := (Content{Data: data}).Validate(normalized); err != nil {
		t.Fatalf("Validate = %v", err)
	}
	wrongSize := normalized
	wrongSize.Size++
	if err := (Content{Data: data}).Validate(wrongSize); !errors.Is(err, ErrInvalid) {
		t.Fatalf("size mismatch content error = %v", err)
	}
	if err := (Content{Data: []byte("other")}).Validate(normalized); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched content error = %v", err)
	}
	clone := (Content{Data: data}).Clone()
	clone.Data[0] = 'I'
	if string(data) != "image" {
		t.Fatalf("Clone aliased source data: %q", data)
	}
}

func TestUploadNormalizeAppliesBoundedRetentionAndMediaFamily(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	normalized, err := (Upload{ID: " attachment-1 ", Kind: KindVideo, MIMEType: " VIDEO/MP4 ", Name: " clip.mp4 ", Size: 1, Provider: " Telegram ", ProviderID: " file-1 "}).Normalize(now)
	if err != nil || normalized.ID != "attachment-1" || normalized.MIMEType != "video/mp4" || normalized.Name != "clip.mp4" || normalized.Provider != "telegram" || normalized.ProviderID != "file-1" || !normalized.ExpiresAt.Equal(now.Add(DefaultRetention)) {
		t.Fatalf("Normalize = %+v, %v", normalized, err)
	}
	zeroNow, err := (Upload{ID: "attachment-1", Kind: KindDocument, MIMEType: "application/pdf", Size: 1}).Normalize(time.Time{})
	if err != nil || zeroNow.ExpiresAt.IsZero() {
		t.Fatalf("zero-now Normalize = %+v, %v", zeroNow, err)
	}
	if _, err := (Upload{ID: "attachment-1", Kind: KindImage, MIMEType: "application/pdf", Size: 1}).Normalize(now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched media family error = %v", err)
	}
	if _, err := (Upload{ID: "attachment-1", Kind: KindDocument, MIMEType: "application/pdf", Size: 1, ExpiresAt: now.Add(DefaultRetention + time.Second)}).Normalize(now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("excess retention error = %v", err)
	}
}

func TestReferenceNormalizeRejectsMalformedMetadata(t *testing.T) {
	data := []byte("x")
	digest := sha256.Sum256(data)
	valid := Reference{ID: "attachment-1", Kind: KindImage, MIMEType: "image/png", Name: "chart.png", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]), Provider: "telegram", ProviderID: "file-1"}
	for _, test := range []struct {
		name      string
		mutate    func(*Reference)
		wantError string
	}{
		{name: "empty id", mutate: func(reference *Reference) { reference.ID = "" }, wantError: "reference ID"},
		{name: "control id", mutate: func(reference *Reference) { reference.ID = "bad\nid" }, wantError: "reference ID"},
		{name: "unknown kind", mutate: func(reference *Reference) { reference.Kind = "sticker" }, wantError: "kind"},
		{name: "bad optional text", mutate: func(reference *Reference) { reference.Name = "bad\nname" }, wantError: "name or MIME type"},
		{name: "mime parameters", mutate: func(reference *Reference) { reference.MIMEType = "image/png; charset=utf-8" }, wantError: "MIME type"},
		{name: "mime family mismatch", mutate: func(reference *Reference) { reference.Kind, reference.MIMEType = KindAudio, "image/png" }, wantError: "MIME type"},
		{name: "zero size", mutate: func(reference *Reference) { reference.Size = 0 }, wantError: "size"},
		{name: "oversized", mutate: func(reference *Reference) { reference.Size = maxSizeBytes + 1 }, wantError: "size"},
		{name: "short digest", mutate: func(reference *Reference) { reference.SHA256 = "abc" }, wantError: "digest"},
		{name: "non-hex digest", mutate: func(reference *Reference) { reference.SHA256 = strings.Repeat("g", sha256.Size*2) }, wantError: "digest"},
	} {
		t.Run(test.name, func(t *testing.T) {
			reference := valid
			test.mutate(&reference)
			_, err := reference.Normalize()
			if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Normalize error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestUploadNormalizeRejectsMalformedMetadata(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	valid := Upload{ID: "attachment-1", Kind: KindDocument, MIMEType: "application/pdf", Name: "brief.pdf", Size: 1, Provider: "wecom", ProviderID: "media-1", ExpiresAt: now.Add(time.Hour)}
	for _, test := range []struct {
		name   string
		mutate func(*Upload)
	}{
		{name: "empty id", mutate: func(upload *Upload) { upload.ID = "" }},
		{name: "control provider id", mutate: func(upload *Upload) { upload.ProviderID = "bad\nmedia" }},
		{name: "unknown kind", mutate: func(upload *Upload) { upload.Kind = "sticker" }},
		{name: "mime parameters", mutate: func(upload *Upload) { upload.MIMEType = "application/pdf; charset=utf-8" }},
		{name: "media family mismatch", mutate: func(upload *Upload) { upload.Kind, upload.MIMEType = KindDocument, "audio/mpeg" }},
		{name: "zero size", mutate: func(upload *Upload) { upload.Size = 0 }},
		{name: "past expiry", mutate: func(upload *Upload) { upload.ExpiresAt = now }},
	} {
		t.Run(test.name, func(t *testing.T) {
			upload := valid
			test.mutate(&upload)
			if _, err := upload.Normalize(now); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Normalize accepted invalid upload: %+v err=%v", upload, err)
			}
		})
	}
}

func TestMatchesKindCoversMediaFamilies(t *testing.T) {
	for _, test := range []struct {
		kind Kind
		mime string
		want bool
	}{
		{kind: KindImage, mime: "image/png", want: true},
		{kind: KindVideo, mime: "video/mp4", want: true},
		{kind: KindAudio, mime: "audio/mpeg", want: true},
		{kind: KindDocument, mime: "application/pdf", want: true},
		{kind: KindDocument, mime: "image/png"},
		{kind: Kind("unknown"), mime: "application/octet-stream"},
	} {
		if got := matchesKind(test.kind, test.mime); got != test.want {
			t.Fatalf("matchesKind(%q, %q) = %t, want %t", test.kind, test.mime, got, test.want)
		}
	}
}
