package channels

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const (
	defaultMediaLimit = int64(10 << 20)
	maxExtractedText  = int64(256 << 10)
)

// MediaDownload is a provider response whose body is owned by the caller.
type MediaDownload struct {
	Body        io.ReadCloser
	ContentType string
	Name        string
	Size        int64
}

// MediaDownloader obtains one authenticated provider object. Implementations
// must never include the provider key, token, URL, or response body in errors.
type MediaDownloader interface {
	Download(context.Context, gateway.MediaReference) (MediaDownload, error)
}

// MediaPolicy bounds one callback download. TempDir is optional and useful in
// tests; production uses the operating system's private temporary directory.
type MediaPolicy struct {
	MaxBytes int64
	Timeout  time.Duration
	TempDir  string
}

// LoadMedia downloads, validates, temporarily stages, extracts, and removes
// one object. The opaque provider reference is never copied into the result.
func LoadMedia(ctx context.Context, downloader MediaDownloader, ref gateway.MediaReference, policy MediaPolicy) (gateway.Attachment, error) {
	if ctx == nil || downloader == nil || ref.Key == "" || (ref.Kind != "image" && ref.Kind != "file") {
		return gateway.Attachment{}, errors.New("media: valid context, downloader, and reference are required")
	}
	limit := policy.MaxBytes
	if limit <= 0 || limit > defaultMediaLimit {
		limit = defaultMediaLimit
	}
	timeout := policy.Timeout
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = 8 * time.Second
	}
	downloadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	download, err := downloader.Download(downloadCtx, ref)
	if err != nil {
		return gateway.Attachment{}, errors.New("media: provider download failed")
	}
	if download.Body == nil {
		return gateway.Attachment{}, errors.New("media: provider returned an empty body")
	}
	defer download.Body.Close()
	if download.Size > limit {
		return gateway.Attachment{}, errors.New("media: file exceeds size limit")
	}
	temporary, err := os.CreateTemp(policy.TempDir, "trpc-agent-media-*")
	if err != nil {
		return gateway.Attachment{}, errors.New("media: create secure temporary file")
	}
	path := temporary.Name()
	defer os.Remove(path)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return gateway.Attachment{}, errors.New("media: secure temporary file")
	}
	written, copyErr := io.Copy(temporary, io.LimitReader(download.Body, limit+1))
	closeErr := temporary.Close()
	if copyErr != nil || closeErr != nil {
		return gateway.Attachment{}, errors.New("media: stage downloaded file")
	}
	if written > limit {
		return gateway.Attachment{}, errors.New("media: file exceeds size limit")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return gateway.Attachment{}, errors.New("media: read staged file")
	}
	detected := http.DetectContentType(data)
	declared, _, _ := mime.ParseMediaType(download.ContentType)
	if declared == "" || declared == "application/octet-stream" {
		declared = detected
	}
	name := safeMediaName(firstNonEmpty(download.Name, ref.Name))
	if ref.Kind == "image" {
		if !allowedImage(declared) || !allowedImage(detected) {
			return gateway.Attachment{}, errors.New("media: image type is not allowed")
		}
		return gateway.Attachment{Kind: "image", Name: name, MIME: detected, Data: data}, nil
	}
	if !allowedDocument(declared, name) || !utf8.Valid(data) {
		return gateway.Attachment{}, errors.New("media: document type is not allowed")
	}
	if int64(len(data)) > maxExtractedText {
		return gateway.Attachment{}, errors.New("media: extracted document text exceeds limit")
	}
	return gateway.Attachment{Kind: "file", Name: name, MIME: declared, ExtractedText: strings.TrimSpace(string(data))}, nil
}

func allowedImage(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}

func allowedDocument(contentType, name string) bool {
	switch strings.ToLower(strings.TrimSpace(contentType)) {
	case "text/plain", "text/markdown", "text/csv", "application/json":
		return true
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".txt", ".md", ".csv", ".json":
		return contentType == "text/plain"
	default:
		return false
	}
}

func safeMediaName(value string) string {
	value = filepath.Base(strings.TrimSpace(value))
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, value)
	if value == "." || value == "" {
		return "attachment"
	}
	runes := []rune(value)
	if len(runes) > 128 {
		value = string(runes[:128])
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// MediaError is deliberately metadata-only; provider response content is not
// retained in logs, audit records, traces, or DLQ errors.
func MediaError(provider string, status int) error {
	return fmt.Errorf("%s: media download HTTP status %d", provider, status)
}
