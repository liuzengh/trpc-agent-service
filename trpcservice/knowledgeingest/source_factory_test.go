package knowledgeingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/netpolicy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document/reader"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
	reposource "trpc.group/trpc-go/trpc-agent-go/knowledge/source/repo"
)

func createKnowledgeDOCX(t *testing.T, text string) []byte {
	t.Helper()
	libreoffice, err := exec.LookPath("libreoffice")
	if err != nil {
		t.Skip("libreoffice is required to generate a real DOCX fixture")
	}
	directory := t.TempDir()
	input := filepath.Join(directory, "fixture.txt")
	if err := os.WriteFile(input, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(libreoffice, "--headless", "--convert-to", "docx", "--outdir", directory, input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generate DOCX fixture: %v: %s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(directory, "fixture.docx"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSourceFactoryReadsEveryNativeUploadFormat(t *testing.T) {
	factory, err := NewSourceFactory(config.KnowledgeConfig{}, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		filename string
		data     []byte
		want     string
	}{
		{"guide.txt", []byte("Native TXT knowledge fixture"), "Native TXT"},
		{"guide.text", []byte("Native TEXT knowledge fixture"), "Native TEXT"},
		{"guide.md", []byte("# Native Markdown knowledge fixture"), "Native Markdown"},
		{"guide.markdown", []byte("# Native MARKDOWN knowledge fixture"), "Native MARKDOWN"},
		{"guide.json", []byte(`{"policy":"Native JSON knowledge fixture"}`), "Native JSON"},
		{"guide.csv", []byte("name,value\npolicy,Native CSV knowledge fixture\n"), "Native CSV"},
		{"guide.docx", createKnowledgeDOCX(t, "Native DOCX knowledge fixture"), "Native DOCX"},
		{"guide.go", []byte("package fixture\n\n// Native Go knowledge fixture\nfunc RefundAllowed() bool { return true }\n"), "Native Go"},
		{"guide.py", []byte("# Native Python knowledge fixture\ndef refund_allowed():\n    return True\n"), "refund_allowed"},
		{"guide.proto", []byte("syntax = \"proto3\";\n// Native Proto knowledge fixture\nmessage RefundRequest { string order_id = 1; }\n"), "RefundRequest"},
	}
	for _, test := range tests {
		t.Run(test.filename, func(t *testing.T) {
			src, cleanup, err := factory.Build(context.Background(), storage.KnowledgeIngestJob{
				DocumentID: strings.TrimSuffix(test.filename, filepath.Ext(test.filename)),
				Name:       test.filename, Filename: test.filename, Data: test.data,
			})
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			defer cleanup()
			documents, err := src.ReadDocuments(context.Background())
			if err != nil {
				t.Fatalf("ReadDocuments() error = %v", err)
			}
			combined := strings.Builder{}
			for _, document := range documents {
				if document != nil {
					combined.WriteString(document.Content)
					combined.WriteByte('\n')
				}
			}
			if !strings.Contains(combined.String(), test.want) {
				t.Fatalf("parsed content does not contain %q: %q", test.want, combined.String())
			}
		})
	}
}

func TestSourceFactoryUsesNativeReadersForUploadedContent(t *testing.T) {
	factory, err := NewSourceFactory(config.KnowledgeConfig{}, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, sourceType := range []string{"", "file", "text"} {
		t.Run("source_type="+sourceType, func(t *testing.T) {
			src, cleanup, err := factory.Build(context.Background(), storage.KnowledgeIngestJob{
				DocumentID: "guide", Name: "Guide", Filename: "guide.md", Data: []byte("# Refund\n\nSeven days."),
				ChunkSize: 20, Overlap: 5, Metadata: map[string]string{"source_type": sourceType},
			})
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}
			defer cleanup()
			documents, err := src.ReadDocuments(context.Background())
			if err != nil {
				t.Fatalf("ReadDocuments() error = %v", err)
			}
			if len(documents) == 0 || documents[0].Content == "" {
				t.Fatalf("native source documents = %#v", documents)
			}
		})
	}
}

func TestSourceFactoryDoesNotSendNativeMarkdownToDocling(t *testing.T) {
	factory, err := NewSourceFactory(config.KnowledgeConfig{}, "http://127.0.0.1:1", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := factory.Build(context.Background(), storage.KnowledgeIngestJob{
		DocumentID: "guide", Name: "Guide", Filename: "guide.md", ContentType: "text/markdown",
		Data: []byte("# Refund\n\nSeven days."), ChunkSize: 20, Overlap: 5,
	})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer cleanup()
	documents, err := src.ReadDocuments(context.Background())
	if err != nil {
		t.Fatalf("ReadDocuments() error = %v", err)
	}
	if len(documents) == 0 || !strings.Contains(documents[0].Content, "Refund") {
		t.Fatalf("native markdown documents = %#v", documents)
	}
}

func TestSourceFactoryExtractsChatDocumentWithConfiguredDocling(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/convert/file" {
			t.Fatalf("docling path = %q", request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"success","document":{"md_content":"# 维修单\n\n编号 A-17"}}`))
	}))
	defer server.Close()

	factory, err := NewSourceFactory(config.KnowledgeConfig{}, server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !factory.SupportsDocument("repair.pdf") || !factory.SupportsDocument("sheet.xlsx") || factory.SupportsDocument("archive.zip") {
		t.Fatal("unexpected document support set")
	}
	content, err := factory.ExtractDocument(context.Background(), "repair.pdf", []byte("fake-pdf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content, "编号 A-17") {
		t.Fatalf("extracted content = %q", content)
	}
}

func TestSourceFactoryEnforcesRemoteAndDirectoryPolicy(t *testing.T) {
	root := t.TempDir()
	allowed := filepath.Join(root, "allowed")
	if err := os.Mkdir(allowed, 0o700); err != nil {
		t.Fatal(err)
	}
	factory, err := NewSourceFactory(config.KnowledgeConfig{
		AllowedSourceHosts: []string{"example.com"}, AllowedSourceRoots: []string{allowed},
	}, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := factory.Build(context.Background(), storage.KnowledgeIngestJob{Metadata: map[string]string{
		"source_type": "url", "source_url": "http://example.com/a",
	}}); !errors.Is(err, ErrSourceRejected) {
		t.Fatalf("HTTP source error = %v, want ErrSourceRejected", err)
	}
	if _, _, err := factory.Build(context.Background(), storage.KnowledgeIngestJob{Metadata: map[string]string{
		"source_type": "url", "source_url": "https://evil.example/a",
	}}); !errors.Is(err, ErrSourceRejected) {
		t.Fatalf("unlisted host error = %v, want ErrSourceRejected", err)
	}
	if _, _, err := factory.Build(context.Background(), storage.KnowledgeIngestJob{DocumentID: "dir", Metadata: map[string]string{
		"source_type": "dir", "source_url": root,
	}}); !errors.Is(err, ErrSourceRejected) {
		t.Fatalf("outside directory error = %v, want ErrSourceRejected", err)
	}
	nested := filepath.Join(allowed, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(allowed, "faq.md"), []byte("# FAQ\n\nRefund within seven days."), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, "handler.go"), []byte("package support\n\nfunc RefundAllowed() bool { return true }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	src, cleanup, err := factory.Build(context.Background(), storage.KnowledgeIngestJob{
		DocumentID: "dir", ChunkSize: 64, Overlap: 8, Metadata: map[string]string{
			"source_type": "dir", "source_url": allowed,
		},
	})
	if err != nil {
		t.Fatalf("allowed directory Build() error = %v", err)
	}
	defer cleanup()
	documents, err := src.ReadDocuments(context.Background())
	if err != nil {
		t.Fatalf("allowed directory ReadDocuments() error = %v", err)
	}
	var content strings.Builder
	for _, document := range documents {
		if document != nil {
			content.WriteString(document.Content)
			content.WriteByte('\n')
		}
	}
	for _, want := range []string{"Refund within seven days", "RefundAllowed"} {
		if !strings.Contains(content.String(), want) {
			t.Fatalf("directory source content missing %q: %q", want, content.String())
		}
	}
}

func TestSourceFactoryUsesSharedPublicNetworkPolicy(t *testing.T) {
	if err := netpolicy.ValidatePublicHTTPSURL("http://example.com/private"); err == nil {
		t.Fatal("ValidatePublicHTTPSURL() error = nil, want non-HTTPS rejected")
	}
}

func TestFrameworkRepoSourceReadsRegisteredCodeReaders(t *testing.T) {
	repository := t.TempDir()
	files := map[string]string{
		"go.mod":        "module example.com/fixture\n\ngo 1.22\n",
		"main.go":       "package fixture\n\nfunc RefundAllowed() bool { return true }\n",
		"worker.py":     "def shipping_enabled():\n    return True\n",
		"service.proto": "syntax = \"proto3\";\nmessage RefundRequest { string order_id = 1; }\n",
		"README.md":     "# Fixture\n\nRefund and shipping knowledge.\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(repository, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.email", "fixture@example.com"}, {"config", "user.name", "Fixture"}, {"add", "."}, {"commit", "-qm", "fixture"}} {
		command := exec.Command("git", args...)
		command.Dir = repository
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}

	src := reposource.New(
		reposource.WithRepository(reposource.Repository{URL: repository}),
		reposource.WithName("fixture-repo"),
		reposource.WithFileExtensions(reader.GetRegisteredExtensions()),
	)
	documents, err := src.ReadDocuments(context.Background())
	if err != nil {
		t.Fatalf("framework Repo Source ReadDocuments() error = %v", err)
	}
	if len(documents) == 0 {
		t.Fatal("framework Repo Source returned no documents")
	}
	want := []string{"RefundAllowed", "shipping_enabled", "RefundRequest", "Refund and shipping knowledge"}
	combined := strings.Builder{}
	metadataSeen := false
	for _, document := range documents {
		if document == nil {
			continue
		}
		combined.WriteString(document.Content)
		combined.WriteByte('\n')
		if _, ok := document.Metadata[source.MetaRepoPath]; ok {
			metadataSeen = true
		}
	}
	for _, token := range want {
		if !strings.Contains(combined.String(), token) {
			t.Fatalf("framework Repo Source output does not contain %q", token)
		}
	}
	if !metadataSeen {
		t.Fatal("framework Repo Source lost repository path metadata")
	}
}
