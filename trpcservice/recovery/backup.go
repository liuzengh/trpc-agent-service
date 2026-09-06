package recovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

// BackupDirMarker is the completion marker file: verify and restore refuse
// a backup directory without it, so an interrupted or failed backup is never
// accepted.
const BackupDirMarker = "COMPLETED"

const (
	archiveFileName  = "archive.pgdump"
	manifestFileName = "manifest.json"
	archiveFormat    = "custom"
)

// BackupConfig is the operator input of one backup run.
type BackupConfig struct {
	SourceURL     string
	OutputDir     string
	MigrationsDir string
	Timeout       time.Duration
	// afterSnapshot is a test seam invoked after the exported snapshot and
	// row counts but before pg_dump starts. It lets the integration suite
	// place concurrent transactions precisely between snapshot and dump.
	// Production callers leave it nil.
	afterSnapshot func(ctx context.Context) error
}

// BackupResult reports the published backup (paths only; no secrets).
type BackupResult struct {
	BackupDir     string
	ArchivePath   string
	ManifestPath  string
	ArchiveSHA256 string
	ArchiveSize   int64
	PostgresMajor int
	TableSum      int64
}

// RunBackup executes the full backup protocol:
//
//  1. fail-closed source preflight (role exemption, migration consistency);
//  2. one REPEATABLE READ snapshot: row counts and pg_dump archive come from
//     the same exported snapshot, so concurrent transactions land entirely
//     before or after it (no torn cross-table state);
//  3. data-only custom archive restricted to the expected release tables;
//  4. atomic publication: fsync archive, manifest, then the completion
//     marker; a failure at any point leaves no acceptable artifact.
func RunBackup(ctx context.Context, cfg BackupConfig, runner ClientRunner) (BackupResult, error) {
	if cfg.SourceURL == "" || cfg.OutputDir == "" || cfg.MigrationsDir == "" {
		return BackupResult{}, fmt.Errorf("%w: backup requires source dsn, output dir and migrations dir", ErrInvalidConfig)
	}
	params, err := parseDSN(cfg.SourceURL)
	if err != nil {
		return BackupResult{}, err
	}
	pool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: cfg.SourceURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		return BackupResult{}, fmt.Errorf("%w: source pool", ErrDependencyUnavailable)
	}
	defer pool.Close()

	// Preflight: the offline operator role must carry the FORCE-RLS
	// exemption and must never be the business runtime role.
	if err := requirePrivilegedRole(ctx, pool, "trpc_runtime"); err != nil {
		return BackupResult{}, err
	}
	major, err := readPostgresMajor(ctx, pool)
	if err != nil {
		return BackupResult{}, err
	}
	schema, err := readCatalogTables(ctx, pool)
	if err != nil {
		return BackupResult{}, err
	}
	migrations, err := verifySourceMigrations(ctx, pool, cfg.MigrationsDir)
	if err != nil {
		return BackupResult{}, err
	}
	fingerprint, err := readSchemaFingerprint(ctx, pool, schema)
	if err != nil {
		return BackupResult{}, err
	}

	backupDir, err := createBackupDir(cfg.OutputDir)
	if err != nil {
		return BackupResult{}, err
	}
	result := BackupResult{BackupDir: backupDir, PostgresMajor: major}

	pgpassEnv, pgpassCleanup, err := writePGPassFile(params)
	if err != nil {
		return result, err
	}
	defer pgpassCleanup()

	// The snapshot transaction stays open until pg_dump finished. REPEATABLE
	// READ plus the exported snapshot give pg_dump exactly the state the row
	// counts were computed from.
	snapTx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return result, classifyQueryError("snapshot begin", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = snapTx.Rollback(context.Background())
		}
	}()
	var snapshotID string
	if err := snapTx.QueryRow(ctx, `SELECT pg_export_snapshot()`).Scan(&snapshotID); err != nil {
		return result, classifyQueryError("snapshot export", err)
	}
	counts, err := readRowCounts(ctx, snapTx, schema, tenantTables)
	if err != nil {
		return result, err
	}
	if cfg.afterSnapshot != nil {
		if err := cfg.afterSnapshot(ctx); err != nil {
			return result, err
		}
	}

	archiveTmp := filepath.Join(backupDir, archiveFileName+".tmp")
	dumpArgs := []string{
		"--format=" + archiveFormat,
		"--data-only",
		"--snapshot=" + snapshotID,
		"--file=" + archiveTmp,
	}
	for _, table := range tenantTables {
		dumpArgs = append(dumpArgs, "--table="+schema+"."+table)
	}
	env := libpqEnv(params, pgpassEnv)
	if err := runner.Run(ctx, ToolPgDump, dumpArgs, env, []Mount{{Host: backupDir, Container: backupDir}}, io.Discard); err != nil {
		_ = os.Remove(archiveTmp)
		return result, err
	}
	if err := fsyncFile(archiveTmp, 0o600); err != nil {
		_ = os.Remove(archiveTmp)
		return result, fmt.Errorf("%w: archive fsync", ErrBackupFailed)
	}
	archivePath := filepath.Join(backupDir, archiveFileName)
	if err := os.Rename(archiveTmp, archivePath); err != nil {
		_ = os.Remove(archiveTmp)
		return result, fmt.Errorf("%w: archive publish", ErrBackupFailed)
	}
	digest, size, err := sha256File(archivePath)
	if err != nil {
		return result, fmt.Errorf("%w: archive digest", ErrBackupFailed)
	}
	result.ArchivePath = archivePath
	result.ArchiveSHA256 = digest
	result.ArchiveSize = size

	manifest := Manifest{
		FormatVersion:     manifestFormatVersion,
		ToolVersion:       toolVersion,
		CreatedAt:         time.Now().UTC(),
		PostgresMajor:     major,
		Schema:            schema,
		SchemaFingerprint: fingerprint,
		SourceIdentity:    sourceIdentityFingerprint(params.Host, params.Port, params.Database),
		Migrations:        migrations,
		MigrationDigest:   MigrationDigest(migrations),
		Tables:            countsToStats(counts),
		Archive: ArchiveInfo{
			File:     archiveFileName,
			SHA256:   digest,
			SizeByte: size,
			Format:   archiveFormat,
			DataOnly: true,
		},
		Completed: true,
	}
	manifestPath := filepath.Join(backupDir, manifestFileName)
	if err := writeAtomicFile(manifestPath, manifest); err != nil {
		return result, fmt.Errorf("%w: manifest publish", ErrBackupFailed)
	}
	result.ManifestPath = manifestPath
	if err := writeAtomicFile(filepath.Join(backupDir, BackupDirMarker), []byte("p2-02\n")); err != nil {
		return result, fmt.Errorf("%w: marker publish", ErrBackupFailed)
	}
	if err := fsyncDir(backupDir); err != nil {
		return result, fmt.Errorf("%w: backup dir fsync", ErrBackupFailed)
	}
	if err := snapTx.Commit(ctx); err != nil {
		committed = true
		return result, fmt.Errorf("%w: snapshot commit: %v", storage.ErrTransactionOutcomeUnknown, err)
	}
	committed = true
	for _, stat := range manifest.Tables {
		result.TableSum += stat.Rows
	}
	return result, nil
}

// verifySourceMigrations performs the fail-closed source consistency check:
// the database schema_migration directory must exactly equal the trusted
// migration source of the current release (same versions, names, checksums;
// no unknown version, no drift).
func verifySourceMigrations(ctx context.Context, pool pgxQuery, migrationsDir string) ([]MigrationEntry, error) {
	versions, err := pgstore.MigrationSet(os.DirFS(migrationsDir))
	if err != nil {
		return nil, fmt.Errorf("%w: migration source: %v", ErrInvalidConfig, err)
	}
	rows, err := pool.Query(ctx, `SELECT version, name, checksum FROM schema_migration ORDER BY version`)
	if err != nil {
		return nil, classifyQueryError("migration catalog read", err)
	}
	defer rows.Close()
	var applied []MigrationEntry
	for rows.Next() {
		var entry MigrationEntry
		if err := rows.Scan(&entry.Version, &entry.Name, &entry.Checksum); err != nil {
			return nil, classifyQueryError("migration catalog scan", err)
		}
		applied = append(applied, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, classifyQueryError("migration catalog iterate", err)
	}
	if len(applied) != len(versions) {
		return nil, fmt.Errorf("%w: migration catalog count drift applied=%d source=%d", ErrForbiddenState, len(applied), len(versions))
	}
	for i := range applied {
		if applied[i] != (MigrationEntry{Version: versions[i].Version, Name: versions[i].Name, Checksum: versions[i].Checksum}) {
			return nil, fmt.Errorf("%w: migration catalog mismatch at source index %d", ErrForbiddenState, i)
		}
	}
	return applied, nil
}

func readPostgresMajor(ctx context.Context, conn pgxQuery) (int, error) {
	var raw string
	if err := conn.QueryRow(ctx, `SHOW server_version_num`).Scan(&raw); err != nil {
		return 0, classifyQueryError("server version", err)
	}
	versionNum, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%w: server version unreadable", ErrForbiddenState)
	}
	return versionNum / 10000, nil
}

// createBackupDir creates a fresh 0700 output directory with a
// non-guessable, non-sensitive name. Symlinks and pre-existing directories
// are rejected.
func createBackupDir(outputDir string) (string, error) {
	info, err := os.Lstat(outputDir)
	switch {
	case err == nil && info.Mode()&os.ModeSymlink != 0:
		return "", fmt.Errorf("%w: output dir is a symlink", ErrInvalidConfig)
	case err == nil:
		if !info.IsDir() {
			return "", fmt.Errorf("%w: output dir is not a directory", ErrInvalidConfig)
		}
	case os.IsNotExist(err):
		if err := os.MkdirAll(outputDir, 0o700); err != nil {
			return "", fmt.Errorf("%w: output dir create", ErrInvalidConfig)
		}
	default:
		return "", fmt.Errorf("%w: output dir stat", ErrInvalidConfig)
	}
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("%w: backup id", ErrInvalidConfig)
	}
	name := "p202-backup-" + time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(random)
	dir := filepath.Join(outputDir, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", fmt.Errorf("%w: backup dir create", ErrInvalidConfig)
	}
	return dir, nil
}

func countsToStats(counts map[string]int64) []TableStat {
	stats := make([]TableStat, 0, len(counts))
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		stats = append(stats, TableStat{Name: name, Rows: counts[name]})
	}
	return stats
}

// writeAtomicFile writes a 0600 file via tmp+rename inside the same
// directory.
func writeAtomicFile(path string, payload any) error {
	var data []byte
	var err error
	switch typed := payload.(type) {
	case []byte:
		data = typed
	default:
		data, err = json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		data = append(data, '\n')
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := fsyncFile(tmp, 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func fsyncFile(path string, mode os.FileMode) error {
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func sha256File(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", 0, fmt.Errorf("%w: archive missing", ErrBackupFailed)
		}
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}
