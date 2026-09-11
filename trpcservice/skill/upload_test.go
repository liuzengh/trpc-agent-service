package skill

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const uploadedMarkdown = "---\nname: sample\ndescription: A bounded sample skill\n---\nRead the input and return a digest.\n"

func TestInstructionOnlySkillDoesNotEnableExecution(t *testing.T) {
	b, err := readUpload(Upload{Name: "sample", Version: "1", Markdown: uploadedMarkdown})
	if err != nil || b.descriptor.Executable {
		t.Fatal("instruction upload", err)
	}
	zipped, err := readUpload(Upload{Name: "sample", Version: "1", Archive: archiveFixture(t, []string{"SKILL.md"}, []string{uploadedMarkdown}, false)})
	if err != nil || zipped.descriptor.Checksum != b.descriptor.Checksum {
		t.Fatal("instruction ZIP identity", err)
	}
	r := &Registry{bundles: map[string]bundle{"sample@1": b}, grants: map[string]bool{grantKey("tenant", "sample", "1"): true}}
	raw, _ := json.Marshal(map[string]any{"skills": []Ref{b.descriptor.Ref}})
	if _, err = r.Validate("tenant", raw, []string{"skill_load"}); err != nil {
		t.Fatal("instruction loading rejected", err)
	}
	if _, err = r.Validate("tenant", raw, []string{"skill_load", "skill_run"}); err == nil {
		t.Fatal("instruction-only Skill obtained script permission")
	}
}

func archiveFixture(t *testing.T, names []string, values []string, symlink bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for i, name := range names {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.SetMode(0600)
		if symlink {
			header.SetMode(os.ModeSymlink | 0600)
		}
		f, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write([]byte(values[i])); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
func TestUploadedSkillMatchesDeploymentSnapshotWithoutExecution(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "must-not-exist")
	script := "#!/bin/sh\ntouch '" + marker + "'\n"
	pair, err := readUpload(Upload{Name: "sample", Version: "1", Markdown: uploadedMarkdown, Script: script})
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "sample/"} {
		zipped, err := readUpload(Upload{Name: "sample", Version: "1", Archive: archiveFixture(t, []string{prefix + "SKILL.md", prefix + "run.sh"}, []string{uploadedMarkdown, script}, false)})
		if err != nil || zipped.descriptor.Checksum != pair.descriptor.Checksum {
			t.Fatal("archive and text files differ", err)
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("upload executed script")
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sample"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"catalog.json": `[{"name":"sample","version":"1","directory":"sample"}]`, "sample/SKILL.md": uploadedMarkdown, "sample/run.sh": script} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := Load(root, `[{"tenant_id":"tenant","name":"sample","version":"1"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if registry.List("tenant")[0].Checksum != pair.descriptor.Checksum {
		t.Fatal("upload changed framework snapshot identity")
	}
}
func TestUploadRejectsUnsafeArchivesAndInvalidContent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input Upload
	}{
		{"traversal", Upload{Archive: archiveFixture(t, []string{"SKILL.md", "../run.sh"}, []string{uploadedMarkdown, "echo ok"}, false)}},
		{"absolute", Upload{Archive: archiveFixture(t, []string{"SKILL.md", "/run.sh"}, []string{uploadedMarkdown, "echo ok"}, false)}},
		{"symlink", Upload{Archive: archiveFixture(t, []string{"SKILL.md", "run.sh"}, []string{uploadedMarkdown, "target"}, true)}},
		{"extra", Upload{Archive: archiveFixture(t, []string{"SKILL.md", "run.sh", "payload.py"}, []string{uploadedMarkdown, "echo ok", "extra"}, false)}},
		{"duplicate", Upload{Archive: archiveFixture(t, []string{"SKILL.md", "run.sh", "run.sh"}, []string{uploadedMarkdown, "echo a", "echo b"}, false)}},
		{"mixed-root", Upload{Archive: archiveFixture(t, []string{"sample/SKILL.md", "run.sh"}, []string{uploadedMarkdown, "echo ok"}, false)}},
		{"oversize", Upload{Archive: archiveFixture(t, []string{"SKILL.md", "run.sh"}, []string{uploadedMarkdown, strings.Repeat("a", 65537)}, false)}},
		{"wrong-name", Upload{Markdown: strings.ReplaceAll(uploadedMarkdown, "sample", "other"), Script: "echo ok"}},
		{"nul", Upload{Markdown: uploadedMarkdown, Script: "echo\x00ok"}},
		{"missing", Upload{Script: "echo missing metadata"}},
		{"invalid-utf8", Upload{Markdown: uploadedMarkdown, Script: string([]byte{0xff})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.input.Name = "sample"
			tc.input.Version = "1"
			if _, err := readUpload(tc.input); err == nil {
				t.Fatal("unsafe upload accepted")
			}
		})
	}
}
