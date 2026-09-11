package skill

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	native "trpc.group/trpc-go/trpc-agent-go/skill"
)

var ErrUpload = errors.New("技能格式无效：需要有效名称、版本和 SKILL.md，可选 run.sh；每个文件最多 64 KiB")
var versionPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9.-]{0,31}$`)

type Upload struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Markdown string `json:"markdown"`
	Script   string `json:"script"`
	Archive  []byte `json:"archive,omitempty"`
}

func readUpload(input Upload) (bundle, error) {
	if !identifier.MatchString(input.Name) || !versionPattern.MatchString(input.Version) {
		return bundle{}, ErrUpload
	}
	md, script := []byte(input.Markdown), []byte(input.Script)
	if len(input.Archive) > 0 {
		if len(md) > 0 || len(script) > 0 || len(input.Archive) > 256<<10 {
			return bundle{}, ErrUpload
		}
		archive, err := zip.NewReader(bytes.NewReader(input.Archive), int64(len(input.Archive)))
		if err != nil || len(archive.File) > 3 {
			return bundle{}, ErrUpload
		}
		files := map[string][]byte{}
		prefix := ""
		prefixSet := false
		for _, f := range archive.File {
			if strings.Contains(f.Name, "\\") || path.Clean(f.Name) != strings.TrimSuffix(f.Name, "/") || path.IsAbs(f.Name) || strings.HasPrefix(f.Name, "../") || f.Mode()&os.ModeSymlink != 0 {
				return bundle{}, ErrUpload
			}
			if f.FileInfo().IsDir() {
				if f.Name != input.Name+"/" {
					return bundle{}, ErrUpload
				}
				continue
			}
			if !f.Mode().IsRegular() || f.UncompressedSize64 > 64<<10 {
				return bundle{}, ErrUpload
			}
			folder, base := path.Split(f.Name)
			if folder != "" && folder != input.Name+"/" {
				return bundle{}, ErrUpload
			}
			if prefixSet && folder != prefix {
				return bundle{}, ErrUpload
			}
			prefix, prefixSet = folder, true
			if base != "SKILL.md" && base != "run.sh" {
				return bundle{}, ErrUpload
			}
			if _, exists := files[base]; exists {
				return bundle{}, ErrUpload
			}
			reader, err := f.Open()
			if err != nil {
				return bundle{}, ErrUpload
			}
			value, readErr := io.ReadAll(io.LimitReader(reader, (64<<10)+1))
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || len(value) > 64<<10 {
				return bundle{}, ErrUpload
			}
			files[base] = value
		}
		md, script = files["SKILL.md"], files["run.sh"]
	}
	return parseBundle(input.Name, input.Version, md, script)
}

// Parsing never invokes the uploaded script. The native framework parser reads
// a private SKILL.md only; archive entries are never extracted to the filesystem.
func parseBundle(name, version string, md, script []byte) (bundle, error) {
	if !identifier.MatchString(name) || !versionPattern.MatchString(version) {
		return bundle{}, ErrUpload
	}
	for _, data := range [][]byte{md, script} {
		if len(data) > 64<<10 || !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
			return bundle{}, ErrUpload
		}
	}
	if len(bytes.TrimSpace(md)) == 0 || (len(script) > 0 && len(bytes.TrimSpace(script)) == 0) {
		return bundle{}, ErrUpload
	}
	if len(script) == 0 {
		script = []byte{}
	}
	tmp, err := os.MkdirTemp("", "trpc-skill-parse-")
	if err != nil {
		return bundle{}, ErrUpload
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	folder := filepath.Join(tmp, name)
	if os.Mkdir(folder, 0700) != nil || os.WriteFile(filepath.Join(folder, "SKILL.md"), md, 0600) != nil {
		return bundle{}, ErrUpload
	}
	repo, err := native.NewFSRepository(tmp)
	if err != nil {
		return bundle{}, ErrUpload
	}
	content, err := repo.Get(name)
	if err != nil || content.Summary.Name != name || strings.TrimSpace(content.Summary.Description) == "" || len(content.Summary.Description) > 2048 {
		return bundle{}, ErrUpload
	}
	encoded, _ := json.Marshal(struct {
		Name, Version    string
		Markdown, Script []byte
	}{name, version, md, script})
	digest := sha256.Sum256(encoded)
	descriptor := Descriptor{Ref: Ref{Name: name, Version: version, Checksum: hex.EncodeToString(digest[:])}, Description: content.Summary.Description, Source: "deployment", Executable: len(script) > 0}
	return bundle{descriptor: descriptor, content: *content, markdown: append([]byte(nil), md...), script: append([]byte(nil), script...)}, nil
}
