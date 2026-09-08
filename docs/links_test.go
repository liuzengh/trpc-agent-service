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

// Keep the supplied implementation requirement when restoring README versions.
func TestReadmeRequirementBoundary(t *testing.T) {
	raw, err := os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	content := string(raw)
	if strings.Contains(content, "不要求实现完整系统") {
		t.Fatal("README restored an older, relaxed implementation requirement")
	}
	for _, required := range []string{
		"一份基于该设计的 GitHub 实现代码",
		"实现时不必严格按这个结构组织代码",
	} {
		if !strings.Contains(content, required) {
			t.Fatalf("README lost the supplied requirement: %s", required)
		}
	}
}
