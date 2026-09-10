package knowledgeingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/netpolicy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/csv"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/docx"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/golang"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/json"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/markdown"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/proto"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/python"
	_ "trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader/text"
	frameworkextractor "trpc.group/trpc-go/trpc-agent-go/knowledge/extractor"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/extractor/docling"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
	dirsource "trpc.group/trpc-go/trpc-agent-go/knowledge/source/dir"
	filesource "trpc.group/trpc-go/trpc-agent-go/knowledge/source/file"
	reposource "trpc.group/trpc-go/trpc-agent-go/knowledge/source/repo"
	urlsource "trpc.group/trpc-go/trpc-agent-go/knowledge/source/url"
)

var ErrSourceRejected = errors.New("knowledge source is outside the platform allowlist")

const (
	defaultMaxSourceDocuments = 5000
	defaultMaxSourceBytes     = int64(64 << 20)
)

type SourceFactory struct {
	allowedHosts map[string]struct{}
	allowedRoots []string
	extractor    frameworkextractor.Extractor
	httpClient   *http.Client
	timeout      time.Duration
	maxDocuments int
	maxBytes     int64
}

func NewSourceFactory(knowledge config.KnowledgeConfig, doclingEndpoint string, timeout time.Duration) (*SourceFactory, error) {
	factory := &SourceFactory{
		allowedHosts: make(map[string]struct{}, len(knowledge.AllowedSourceHosts)),
		timeout:      timeout,
		maxDocuments: defaultMaxSourceDocuments,
		maxBytes:     defaultMaxSourceBytes,
	}
	for _, host := range knowledge.AllowedSourceHosts {
		factory.allowedHosts[strings.ToLower(strings.TrimSpace(host))] = struct{}{}
	}
	for _, root := range knowledge.AllowedSourceRoots {
		resolved, err := filepath.EvalSymlinks(filepath.Clean(root))
		if err != nil {
			return nil, fmt.Errorf("resolve allowed knowledge source root %q: %w", root, err)
		}
		factory.allowedRoots = append(factory.allowedRoots, resolved)
	}
	factory.httpClient = netpolicy.NewPublicHTTPSClient(timeout)
	baseRedirect := factory.httpClient.CheckRedirect
	factory.httpClient.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if err := baseRedirect(request, via); err != nil {
			return err
		}
		return factory.validateRemoteURL(request.Context(), request.URL.String())
	}
	if endpoint := strings.TrimSpace(doclingEndpoint); endpoint != "" {
		options := []docling.Option{docling.WithEndpoint(endpoint)}
		if timeout > 0 {
			options = append(options, docling.WithTimeout(timeout))
		}
		doclingExtractor := docling.New(options...)
		formats := append([]string(nil), doclingExtractor.SupportedFormats()...)
		legacyDOC := true
		for _, format := range formats {
			if format == ".doc" {
				legacyDOC = false
				break
			}
		}
		if legacyDOC {
			formats = append(formats, ".doc")
			options = append(options, docling.WithFormats(formats))
			doclingExtractor = docling.New(options...)
		}
		factory.extractor = doclingExtractor
	}
	return factory, nil
}

// SupportsDocument reports whether the configured extractor can turn the
// named attachment into text for a model invocation.
func (f *SourceFactory) SupportsDocument(name string) bool {
	if f == nil || f.extractor == nil {
		return false
	}
	extension := strings.ToLower(filepath.Ext(filepath.Base(strings.TrimSpace(name))))
	return extension != "" && frameworkextractor.Supports(f.extractor, extension)
}

// ExtractDocument converts a chat attachment to markdown using the same
// framework extractor as Knowledge ingestion.
func (f *SourceFactory) ExtractDocument(ctx context.Context, name string, data []byte) (string, error) {
	if !f.SupportsDocument(name) {
		return "", fmt.Errorf("document extractor does not support %q", filepath.Ext(name))
	}
	result, err := f.extractor.Extract(ctx, data, frameworkextractor.WithOutputFormat(frameworkextractor.FormatMarkdown))
	if err != nil {
		return "", fmt.Errorf("extract document %q: %w", filepath.Base(name), err)
	}
	if result == nil || result.Reader == nil {
		return "", fmt.Errorf("extract document %q: empty result", filepath.Base(name))
	}
	content, err := io.ReadAll(io.LimitReader(result.Reader, storage.MaxArtifactBytes+1))
	if err != nil {
		return "", fmt.Errorf("read extracted document %q: %w", filepath.Base(name), err)
	}
	if int64(len(content)) > storage.MaxArtifactBytes {
		return "", fmt.Errorf("extracted document %q exceeds %d bytes", filepath.Base(name), storage.MaxArtifactBytes)
	}
	if strings.TrimSpace(string(content)) == "" {
		return "", fmt.Errorf("extract document %q: no readable content", filepath.Base(name))
	}
	return string(content), nil
}

// ValidateRemoteSource applies the network boundary before a durable remote
// ingestion job is accepted. Build applies the same policy again immediately
// before framework Source access.
func (f *SourceFactory) ValidateRemoteSource(ctx context.Context, sourceType, rawURL string) error {
	switch strings.ToLower(strings.TrimSpace(sourceType)) {
	case "url", "repo":
		return f.validateRemoteURL(ctx, rawURL)
	default:
		return permanent(fmt.Errorf("%w: unsupported remote source type %q", ErrSourceRejected, sourceType))
	}
}

// Build returns one native framework Source and a cleanup for uploaded
// content. Readers, structured chunking, OCR, and source metadata remain owned
// by the framework.
func (f *SourceFactory) Build(ctx context.Context, job storage.KnowledgeIngestJob) (source.Source, func(), error) {
	metadata := make(map[string]any, len(job.Metadata)+2)
	for key, value := range job.Metadata {
		metadata[key] = value
	}
	metadata["document_name"] = job.Name

	switch sourceType := strings.ToLower(strings.TrimSpace(job.Metadata["source_type"])); sourceType {
	case "url":
		rawURL := strings.TrimSpace(job.Metadata["source_url"])
		if err := f.ValidateRemoteSource(ctx, sourceType, rawURL); err != nil {
			return nil, nil, err
		}
		options := []urlsource.Option{
			urlsource.WithName(job.DocumentID), urlsource.WithMetadata(metadata), urlsource.WithHTTPClient(f.httpClient),
		}
		if job.ChunkSize > 0 {
			options = append(options, urlsource.WithChunkSize(job.ChunkSize))
		}
		if job.Overlap > 0 {
			options = append(options, urlsource.WithChunkOverlap(job.Overlap))
		}
		if f.extractor != nil {
			options = append(options, urlsource.WithExtractor(f.extractor))
		}
		return f.bound(urlsource.New([]string{rawURL}, options...)), func() {}, nil
	case "repo":
		rawURL := strings.TrimSpace(job.Metadata["source_url"])
		if err := f.ValidateRemoteSource(ctx, sourceType, rawURL); err != nil {
			return nil, nil, err
		}
		return f.bound(reposource.New(
			reposource.WithRepository(reposource.Repository{URL: rawURL, Branch: strings.TrimSpace(job.Metadata["branch"])}),
			reposource.WithName(job.DocumentID),
			reposource.WithMetadata(metadata),
			reposource.WithFileExtensions(reader.GetRegisteredExtensions()),
		)), func() {}, nil
	case "dir":
		path, err := f.validateDirectory(job.Metadata["source_url"])
		if err != nil {
			return nil, nil, err
		}
		options := []dirsource.Option{
			dirsource.WithName(job.DocumentID), dirsource.WithMetadata(metadata),
			dirsource.WithRecursive(true), dirsource.WithFileExtensions(reader.GetRegisteredExtensions()),
		}
		if job.ChunkSize > 0 {
			options = append(options, dirsource.WithChunkSize(job.ChunkSize))
		}
		if job.Overlap > 0 {
			options = append(options, dirsource.WithChunkOverlap(job.Overlap))
		}
		if f.extractor != nil {
			options = append(options, dirsource.WithExtractor(f.extractor))
		}
		return f.bound(dirsource.New([]string{path}, options...)), func() {}, nil
	case "", "file", "text":
		return f.uploadSource(job, metadata)
	default:
		return nil, nil, permanent(fmt.Errorf("unsupported knowledge source type %q", sourceType))
	}
}

func (f *SourceFactory) uploadSource(job storage.KnowledgeIngestJob, metadata map[string]any) (source.Source, func(), error) {
	if len(job.Data) == 0 || strings.TrimSpace(job.Filename) == "" {
		return nil, nil, permanent(errors.New("uploaded knowledge filename and data are required"))
	}
	extension := strings.ToLower(filepath.Ext(filepath.Base(job.Filename)))
	if extension == "" {
		extension = ".txt"
	}
	directory, err := os.MkdirTemp("", "trpc-knowledge-")
	if err != nil {
		return nil, nil, fmt.Errorf("create knowledge source workspace: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	path := filepath.Join(directory, "document"+extension)
	if err := os.WriteFile(path, job.Data, 0o600); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("write uploaded knowledge source: %w", err)
	}
	options := []filesource.Option{filesource.WithName(job.DocumentID), filesource.WithMetadata(metadata)}
	if job.ChunkSize > 0 {
		options = append(options, filesource.WithChunkSize(job.ChunkSize))
	}
	if job.Overlap > 0 {
		options = append(options, filesource.WithChunkOverlap(job.Overlap))
	}
	_, native := reader.GetReader(extension)
	// The framework currently registers legacy .doc on the DOCX reader, but
	// that reader parses OOXML ZIP content and cannot read real Word 97-2003
	// binary documents. Route .doc through the document extractor instead.
	if extension == ".doc" {
		native = false
	}
	if f.extractor != nil && !native {
		options = append(options, filesource.WithExtractor(f.extractor))
	}
	return f.bound(filesource.New([]string{path}, options...)), cleanup, nil
}

func (f *SourceFactory) validateRemoteURL(ctx context.Context, raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" {
		return permanent(fmt.Errorf("%w: remote sources require an HTTPS URL without credentials or fragment", ErrSourceRejected))
	}
	host := strings.ToLower(parsed.Hostname())
	allowedHost := false
	for allowed := range f.allowedHosts {
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			allowedHost = true
			break
		}
	}
	if !allowedHost {
		return permanent(fmt.Errorf("%w: host %q", ErrSourceRejected, host))
	}
	if err := netpolicy.ValidatePublicHTTPS(ctx, raw); err != nil {
		return permanent(fmt.Errorf("%w: %v", ErrSourceRejected, err))
	}
	return nil
}

func (f *SourceFactory) bound(delegate source.Source) source.Source {
	return &boundedSource{delegate: delegate, timeout: f.timeout, maxDocuments: f.maxDocuments, maxBytes: f.maxBytes}
}

type boundedSource struct {
	delegate     source.Source
	timeout      time.Duration
	maxDocuments int
	maxBytes     int64
}

func (s *boundedSource) ReadDocuments(ctx context.Context) ([]*document.Document, error) {
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	documents, err := s.delegate.ReadDocuments(ctx)
	if err != nil {
		return nil, err
	}
	if s.maxDocuments > 0 && len(documents) > s.maxDocuments {
		return nil, permanent(fmt.Errorf("%w: source produced %d documents; limit is %d", ErrSourceRejected, len(documents), s.maxDocuments))
	}
	var total int64
	for _, doc := range documents {
		if doc != nil {
			total += int64(len(doc.Content))
		}
		if s.maxBytes > 0 && total > s.maxBytes {
			return nil, permanent(fmt.Errorf("%w: extracted source exceeds %d bytes", ErrSourceRejected, s.maxBytes))
		}
	}
	return documents, nil
}

func (s *boundedSource) Name() string                { return s.delegate.Name() }
func (s *boundedSource) Type() string                { return s.delegate.Type() }
func (s *boundedSource) GetMetadata() map[string]any { return s.delegate.GetMetadata() }

func (f *SourceFactory) validateDirectory(raw string) (string, error) {
	path, err := filepath.Abs(filepath.Clean(strings.TrimSpace(raw)))
	if err != nil {
		return "", permanent(fmt.Errorf("%w: invalid directory", ErrSourceRejected))
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", permanent(fmt.Errorf("%w: resolve directory: %v", ErrSourceRejected, err))
	}
	for _, root := range f.allowedRoots {
		if path == root || strings.HasPrefix(path, root+string(filepath.Separator)) {
			return path, nil
		}
	}
	return "", permanent(fmt.Errorf("%w: directory %q", ErrSourceRejected, path))
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{error: err}
}

func isPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}
