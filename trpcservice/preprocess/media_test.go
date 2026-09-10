package preprocess_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	artifactmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
)

func TestMediaStagerPersistsOnlyScannedImmutableArtifact(t *testing.T) {
	content := []byte("safe document")
	fetcher := &mediaFetcherStub{contents: [][]byte{content, content}}
	store := artifactmemory.New()
	stager := preprocess.MediaStager{Fetcher: fetcher, Malware: scannerStub{result: preprocess.ScanResult{Verdict: preprocess.ScanClean, Version: "av-1"}},
		DLP: scannerStub{result: preprocess.ScanResult{Verdict: preprocess.ScanClean, Version: "dlp-1"}}, Artifacts: store}
	request := preprocess.MediaStageRequest{TenantID: "tenant-a", RequestID: "request-1", Channel: "feishu", Ordinal: 0,
		Media: channel.MediaRef{ID: "https://example.com/file", Kind: "file", ContentType: "text/plain", Size: int64(len(content))}}
	first, err := stager.Stage(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := stager.Stage(context.Background(), request)
	if err != nil || first != second || first.ArtifactRef == "" || first.MediaType != "text/plain" {
		t.Fatalf("first=%#v second=%#v err=%v", first, second, err)
	}
	record, err := store.GetArtifact(context.Background(), "tenant-a", first.ArtifactID)
	if err != nil || string(record.Content) != string(content) || record.MalwareScanVersion != "av-1" || record.DLPVersion != "dlp-1" {
		t.Fatalf("record=%#v err=%v", record, err)
	}
}

func TestMediaStagerFailsClosedOnSizeMIMEArchiveAndScanning(t *testing.T) {
	cases := []struct {
		name     string
		content  []byte
		media    channel.MediaRef
		download preprocess.MediaDownload
		malware  preprocess.ScanResult
		dlp      preprocess.ScanResult
	}{
		{name: "actual size", content: []byte("too large"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "text/plain"}, download: preprocess.MediaDownload{DeclaredSize: -1}, malware: clean("av"), dlp: clean("dlp")},
		{name: "declared mismatch", content: []byte("safe"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "text/plain", Size: 9}, download: preprocess.MediaDownload{DeclaredSize: 4}, malware: clean("av"), dlp: clean("dlp")},
		{name: "mime mismatch", content: []byte("safe"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "application/pdf"}, download: preprocess.MediaDownload{DeclaredSize: 4, ContentType: "application/pdf"}, malware: clean("av"), dlp: clean("dlp")},
		{name: "zip rejected", content: []byte("PK\x03\x04payload"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "application/zip"}, download: preprocess.MediaDownload{DeclaredSize: 11, ContentType: "application/zip"}, malware: clean("av"), dlp: clean("dlp")},
		{name: "encoded response", content: []byte("safe"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "text/plain"}, download: preprocess.MediaDownload{DeclaredSize: 4, ContentType: "text/plain", ContentEncoding: "gzip"}, malware: clean("av"), dlp: clean("dlp")},
		{name: "malware unknown", content: []byte("safe"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "text/plain"}, download: preprocess.MediaDownload{DeclaredSize: 4, ContentType: "text/plain"}, malware: preprocess.ScanResult{}, dlp: clean("dlp")},
		{name: "dlp unknown", content: []byte("safe"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "text/plain"}, download: preprocess.MediaDownload{DeclaredSize: 4, ContentType: "text/plain"}, malware: clean("av"), dlp: preprocess.ScanResult{}},
		{name: "malware rejected", content: []byte("safe"), media: channel.MediaRef{ID: "x", Kind: "file", ContentType: "text/plain"}, download: preprocess.MediaDownload{DeclaredSize: 4, ContentType: "text/plain"}, malware: preprocess.ScanResult{Verdict: preprocess.ScanRejected, Version: "av"}, dlp: clean("dlp")},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			download := test.download
			download.Body = io.NopCloser(bytes.NewReader(test.content))
			stager := preprocess.MediaStager{Fetcher: staticMediaFetcher{download: download}, Malware: scannerStub{result: test.malware},
				DLP: scannerStub{result: test.dlp}, Artifacts: artifactmemory.New(), MaxBytes: 4}
			_, err := stager.Stage(context.Background(), preprocess.MediaStageRequest{TenantID: "tenant", RequestID: "request",
				Channel: "wecom", Media: test.media})
			if err == nil {
				t.Fatal("unsafe media accepted")
			}
		})
	}
}

func TestMediaStagerDetectsImmutableSourceCollision(t *testing.T) {
	fetcher := &mediaFetcherStub{contents: [][]byte{[]byte("first"), []byte("other")}}
	stager := preprocess.MediaStager{Fetcher: fetcher, Malware: scannerStub{result: clean("av")}, DLP: scannerStub{result: clean("dlp")}, Artifacts: artifactmemory.New()}
	request := preprocess.MediaStageRequest{TenantID: "tenant", RequestID: "request", Channel: "feishu",
		Media: channel.MediaRef{ID: "https://example.com/file", Kind: "file", ContentType: "text/plain"}}
	if _, err := stager.Stage(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := stager.Stage(context.Background(), request); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision err=%v", err)
	}
}

func clean(version string) preprocess.ScanResult {
	return preprocess.ScanResult{Verdict: preprocess.ScanClean, Version: version}
}

type mediaFetcherStub struct {
	contents [][]byte
	index    int
}

func (f *mediaFetcherStub) Fetch(context.Context, preprocess.MediaFetchRequest) (preprocess.MediaDownload, error) {
	content := f.contents[f.index]
	f.index++
	return preprocess.MediaDownload{Body: io.NopCloser(bytes.NewReader(content)), ContentType: "text/plain", DeclaredSize: int64(len(content))}, nil
}

type staticMediaFetcher struct{ download preprocess.MediaDownload }

func (f staticMediaFetcher) Fetch(context.Context, preprocess.MediaFetchRequest) (preprocess.MediaDownload, error) {
	return f.download, nil
}

type scannerStub struct{ result preprocess.ScanResult }

func (s scannerStub) ScanMedia(context.Context, []byte, string) (preprocess.ScanResult, error) {
	return s.result, nil
}
func (s scannerStub) ScanMediaInput(context.Context, string, []byte, string) (preprocess.ScanResult, error) {
	return s.result, nil
}
