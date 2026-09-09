package recovery

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

// VerifyConfig is the operator input of one verification run.
type VerifyConfig struct {
	BackupDir     string
	MigrationsDir string // optional: cross-check against the trusted release source when non-empty
}

// VerifyResult reports the verified manifest plus the audited archive TOC.
type VerifyResult struct {
	Manifest  Manifest
	TOCTables []TableStat
}

// VerifyBackup validates a published backup end to end: directory hygiene,
// completion marker, manifest contract, file permissions, archive checksum,
// and a pg_restore TOC audit proving the archive contains TABLE DATA for
// exactly the expected release tables and nothing else (no DDL, no
// functions, no roles, no ACLs, no arbitrary SQL, no unknown objects).
func VerifyBackup(ctx context.Context, cfg VerifyConfig, runner ClientRunner) (VerifyResult, error) {
	result := VerifyResult{}
	if cfg.BackupDir == "" {
		return result, fmt.Errorf("%w: verify requires backup dir", ErrInvalidConfig)
	}
	manifest, err := readPublishedBackup(cfg.BackupDir)
	if err != nil {
		return result, err
	}
	digest, size, err := sha256File(filepath.Join(cfg.BackupDir, manifest.Archive.File))
	if err != nil {
		return result, err
	}
	if digest != manifest.Archive.SHA256 || size != manifest.Archive.SizeByte {
		return result, fmt.Errorf("%w: archive checksum mismatch", ErrIntegrityMismatch)
	}
	if cfg.MigrationsDir != "" {
		versions, err := pgstore.MigrationSet(os.DirFS(cfg.MigrationsDir))
		if err != nil {
			return result, fmt.Errorf("%w: migration source: %v", ErrInvalidConfig, err)
		}
		if len(versions) != len(manifest.Migrations) || MigrationDigest(entriesFromVersions(versions)) != manifest.MigrationDigest {
			return result, fmt.Errorf("%w: migration digest mismatch", ErrIntegrityMismatch)
		}
	}
	toc, err := auditArchiveTOC(ctx, runner, filepath.Join(cfg.BackupDir, manifest.Archive.File), manifest)
	if err != nil {
		return result, err
	}
	result.Manifest = manifest
	result.TOCTables = toc
	return result, nil
}

// readPublishedBackup loads and sanity-checks the manifest and the directory
// contract: no symlinks, regular files only, 0600 modes, marker present.
func readPublishedBackup(backupDir string) (Manifest, error) {
	var manifest Manifest
	info, err := os.Lstat(backupDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return manifest, fmt.Errorf("%w: backup dir missing or not a directory", ErrInvalidConfig)
	}
	if _, err := os.Lstat(filepath.Join(backupDir, BackupDirMarker)); err != nil {
		return manifest, fmt.Errorf("%w: completion marker missing", ErrIntegrityMismatch)
	}
	archivePath := filepath.Join(backupDir, archiveFileName)
	manifestPath := filepath.Join(backupDir, manifestFileName)
	for _, path := range []string{archivePath, manifestPath} {
		fileInfo, err := os.Lstat(path)
		if err != nil || !fileInfo.Mode().IsRegular() || fileInfo.Mode()&os.ModeSymlink != 0 {
			return manifest, fmt.Errorf("%w: backup file missing or not a regular file", ErrIntegrityMismatch)
		}
		if fileInfo.Mode().Perm() != 0o600 {
			return manifest, fmt.Errorf("%w: backup file mode is not 0600", ErrIntegrityMismatch)
		}
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return manifest, fmt.Errorf("%w: manifest read", ErrIntegrityMismatch)
	}
	if err := jsonUnmarshalStrict(raw, &manifest); err != nil {
		return manifest, fmt.Errorf("%w: manifest parse", ErrIntegrityMismatch)
	}
	if manifest.FormatVersion != manifestFormatVersion || manifest.ToolVersion != toolVersion {
		return manifest, fmt.Errorf("%w: manifest format unsupported", ErrIntegrityMismatch)
	}
	if !manifest.Completed || manifest.Archive.File != archiveFileName || !manifest.Archive.DataOnly {
		return manifest, fmt.Errorf("%w: manifest contract invalid", ErrIntegrityMismatch)
	}
	if manifest.Archive.Format != archiveFormat {
		return manifest, fmt.Errorf("%w: manifest archive format invalid", ErrIntegrityMismatch)
	}
	if len(manifest.Tables) != len(tenantTables) {
		return manifest, fmt.Errorf("%w: manifest table set incomplete", ErrIntegrityMismatch)
	}
	names := make([]string, 0, len(manifest.Tables))
	for _, stat := range manifest.Tables {
		names = append(names, stat.Name)
	}
	sort.Strings(names)
	expected := ArchiveTables()
	sort.Strings(expected)
	for i := range names {
		if names[i] != expected[i] {
			return manifest, fmt.Errorf("%w: manifest table set mismatch", ErrIntegrityMismatch)
		}
	}
	return manifest, nil
}

var (
	tocTableDataPattern   = regexp.MustCompile(`^\d+; \d+ \d+ TABLE DATA (\S+) (\S+)`)
	tocDataCommentPattern = regexp.MustCompile(`^-- Data for Name: (\S+); Type: TABLE DATA; Schema: (\S+);`)
)

// auditArchiveTOC runs `pg_restore --list` and enforces the archive content
// contract: every numbered TOC entry must be a TABLE DATA entry for an
// expected table, every TABLE DATA comment header must name the expected
// schema, and the set must equal the manifest exactly. Any DDL, FUNCTION,
// ROLE, ACL, SEQUENCE, BLOBS or unknown object entry fails closed.
func auditArchiveTOC(ctx context.Context, runner ClientRunner, archivePath string, manifest Manifest) ([]TableStat, error) {
	var stdout bytes.Buffer
	args := []string{"--list", archivePath}
	err := runner.Run(ctx, ToolPgRestore, args, nil, []Mount{{Host: filepath.Dir(archivePath), Container: filepath.Dir(archivePath), ReadOnly: true}}, &stdout)
	if err != nil {
		return nil, err
	}
	seen := map[string]int64{}
	commentSchemas := map[string]string{}
	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}
		if match := tocDataCommentPattern.FindStringSubmatch(line); match != nil {
			commentSchemas[match[1]] = match[2]
			continue
		}
		if strings.HasPrefix(line, "--") {
			continue
		}
		match := tocTableDataPattern.FindStringSubmatch(line)
		if match == nil {
			return nil, fmt.Errorf("%w: archive TOC contains a non data-only entry", ErrIntegrityMismatch)
		}
		schema, table := match[1], match[2]
		if schema != manifest.Schema {
			return nil, fmt.Errorf("%w: archive TOC schema mismatch", ErrIntegrityMismatch)
		}
		if !containsName(tenantTables, table) {
			return nil, fmt.Errorf("%w: archive TOC contains an unexpected table", ErrIntegrityMismatch)
		}
		if commentSchema, ok := commentSchemas[table]; ok && commentSchema != manifest.Schema {
			return nil, fmt.Errorf("%w: archive TOC schema mismatch", ErrIntegrityMismatch)
		}
		seen[table]++
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%w: TOC read", ErrIntegrityMismatch)
	}
	stats := make([]TableStat, 0, len(manifest.Tables))
	for _, stat := range manifest.Tables {
		count, ok := seen[stat.Name]
		if !ok {
			return nil, fmt.Errorf("%w: archive TOC missing expected table", ErrIntegrityMismatch)
		}
		if count != 1 {
			return nil, fmt.Errorf("%w: archive TOC duplicate table entry", ErrIntegrityMismatch)
		}
		delete(seen, stat.Name)
		stats = append(stats, stat)
	}
	if len(seen) != 0 {
		return nil, fmt.Errorf("%w: archive TOC unknown table entry", ErrIntegrityMismatch)
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })
	return stats, nil
}

func containsName(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

func entriesFromVersions(versions []pgstore.MigrationVersion) []MigrationEntry {
	entries := make([]MigrationEntry, 0, len(versions))
	for _, version := range versions {
		entries = append(entries, MigrationEntry{Version: version.Version, Name: version.Name, Checksum: version.Checksum})
	}
	return entries
}

// jsonUnmarshalStrict decodes JSON while rejecting unknown fields so a
// manifest from a different tool version cannot silently change the
// contract.
func jsonUnmarshalStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
