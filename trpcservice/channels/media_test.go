package channels

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

type mediaDownloadFunc func(context.Context, gateway.MediaReference) (MediaDownload, error)

func (download mediaDownloadFunc) Download(ctx context.Context, ref gateway.MediaReference) (MediaDownload, error) {
	return download(ctx, ref)
}

func TestLoadMediaExtractsDocumentAndRemovesTemporaryFile(t *testing.T) {
	directory := t.TempDir()
	downloader := mediaDownloadFunc(func(context.Context, gateway.MediaReference) (MediaDownload, error) {
		return MediaDownload{Body: io.NopCloser(strings.NewReader("safe document text")), ContentType: "text/plain", Name: "../notes.txt", Size: 18}, nil
	})
	attachment, err := LoadMedia(context.Background(), downloader, gateway.MediaReference{Kind: "file", Key: "private-key"}, MediaPolicy{TempDir: directory, MaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if attachment.Name != "notes.txt" || attachment.ExtractedText != "safe document text" || len(attachment.Data) != 0 {
		t.Fatalf("attachment=%+v", attachment)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files were not removed: entries=%v err=%v", entries, err)
	}
}

func TestLoadMediaValidatesImageTypeAndSizeWithoutLeakingReference(t *testing.T) {
	secret := "private-media-key"
	tooLarge := mediaDownloadFunc(func(context.Context, gateway.MediaReference) (MediaDownload, error) {
		return MediaDownload{Body: io.NopCloser(bytes.NewReader(make([]byte, 9))), ContentType: "image/png"}, nil
	})
	_, err := LoadMedia(context.Background(), tooLarge, gateway.MediaReference{Kind: "image", Key: secret}, MediaPolicy{MaxBytes: 8})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("size error=%v", err)
	}
	failed := mediaDownloadFunc(func(context.Context, gateway.MediaReference) (MediaDownload, error) {
		return MediaDownload{}, errors.New("provider failed for " + secret)
	})
	_, err = LoadMedia(context.Background(), failed, gateway.MediaReference{Kind: "image", Key: secret}, MediaPolicy{})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("provider error leaked reference: %v", err)
	}
}
