package docsmcp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) (string, *Index) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "backend-adapters.md"), []byte("# Storage guide\n\nRedis Session is shared across workers.\nMemory uses PostgreSQL.\nAuthorization: Bearer secret-canary-12345678\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("TOP_SECRET_CANARY"), 0600); err != nil {
		t.Fatal(err)
	}
	index, err := LoadIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	return root, index
}
func TestSearchUsesOnlyCuratedSnapshot(t *testing.T) {
	root, index := fixture(t)
	result, err := index.Search(context.Background(), "Redis Session", 2)
	if err != nil || len(result.Matches) != 1 || result.Matches[0].Path != "docs/backend-adapters.md" || result.Matches[0].Line < 1 || !strings.Contains(result.Matches[0].Excerpt, "Redis Session") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if strings.Contains(result.Matches[0].Excerpt, "secret-canary") {
		t.Fatal("credential pattern not redacted")
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "backend-adapters.md"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	again, err := index.Search(context.Background(), "Redis Session", 2)
	if err != nil || len(again.Matches) != 1 || again.Snapshot != result.Snapshot {
		t.Fatal("request reread mutable files")
	}
	secret, err := index.Search(context.Background(), "TOP_SECRET_CANARY", 3)
	if err != nil || len(secret.Matches) != 0 {
		t.Fatal("private file indexed")
	}
	for _, query := range []string{"", strings.Repeat("a", 257), "a b c", "valid\nquery", "one two three four five six seven eight nine"} {
		if _, err := index.Search(context.Background(), query, 3); err == nil {
			t.Fatalf("invalid query accepted %q", query)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := index.Search(ctx, "Redis", 3); err == nil {
		t.Fatal("cancel ignored")
	}
}
func TestIndexRejectsSymlinksAndOversizedFiles(t *testing.T) {
	for _, kind := range []string{"link", "large", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "docs"), 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "docs", "architecture.md")
			switch kind {
			case "link":
				if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), path); err != nil {
					t.Fatal(err)
				}
			case "large":
				if err := os.WriteFile(path, []byte(strings.Repeat("x", maxDocumentBytes+1)), 0600); err != nil {
					t.Fatal(err)
				}
			case "invalid":
				if err := os.WriteFile(path, []byte{0xff}, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := LoadIndex(root); err == nil {
				t.Fatal("unsafe document accepted")
			}
		})
	}
}
