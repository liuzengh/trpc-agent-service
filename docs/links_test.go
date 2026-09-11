package docs

import (
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Check local Markdown file targets, not remote websites or generated heading
// anchors. This prevents the primary runbooks from pointing at missing files.
func TestLocalDocumentLinks(t *testing.T) {
	paths := []string{"../README.md"}
	if err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".md") {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	links := regexp.MustCompile(`\[[^\]]*\]\(([^\s)]+)\)`)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range links.FindAllStringSubmatch(string(data), -1) {
			u, err := url.Parse(match[1])
			if err != nil || u.Scheme != "" || u.Host != "" || u.Path == "" || strings.HasPrefix(u.Path, "/") {
				continue
			}
			target := filepath.Join(filepath.Dir(path), filepath.FromSlash(u.Path))
			if _, err := os.Stat(target); err != nil {
				t.Errorf("%s: missing local link %s", path, match[1])
			}
		}
	}
}

// The delivery README is a product entry point, not an assignment or build log.
func TestReadmeDeliveryBoundary(t *testing.T) {
	raw, err := os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	content := string(raw)
	for _, required := range []string{
		"tRPC-Agent-Go", "docs/architecture.md", "docs/sequence.md",
		"docs/data-model.md", "docs/data-consistency.md", "docs/backend-adapters.md",
		"docs/risks.md", "docs/operations-runbook.md", "compose.demo.yaml",
	} {
		if !strings.Contains(content, required) {
			t.Fatalf("README lost a delivery entry point: %s", required)
		}
	}
}

func TestDocumentationContainsNoDevelopmentHistory(t *testing.T) {
	paths, err := filepath.Glob("*.md")
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, "../README.md")
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, marker := range []string{"本轮", "本次修复", "验包报告", "封版", "本地提交", "Git HEAD", "workbuddy2api", "开发环境", "开发者日常", "联调记录", "回归入口", "regression.sh", "e2e-backup-restore.sh", "测试包", "测试条目"} {
			if strings.Contains(string(raw), marker) {
				t.Errorf("%s contains internal history: %s", path, marker)
			}
		}
	}
}
