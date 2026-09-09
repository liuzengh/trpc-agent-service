package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// manifestFormatVersion is bumped whenever the manifest contract changes.
const manifestFormatVersion = 1

// toolVersion identifies the P2-02 recovery tool release.
const toolVersion = "p2-02"

// Manifest is the non-sensitive contract of one backup. It must never
// contain DSNs, passwords, tenant identifiers, business identifiers, raw
// rows or data content: table names, row counts, file digests and migration
// checksums only.
type Manifest struct {
	FormatVersion     int              `json:"format_version"`
	ToolVersion       string           `json:"tool_version"`
	CreatedAt         time.Time        `json:"created_at"`
	PostgresMajor     int              `json:"postgres_major"`
	Schema            string           `json:"schema"`
	SchemaFingerprint string           `json:"schema_fingerprint"`
	SourceIdentity    string           `json:"source_identity"`
	Migrations        []MigrationEntry `json:"migrations"`
	MigrationDigest   string           `json:"migration_digest"`
	Tables            []TableStat      `json:"tables"`
	Archive           ArchiveInfo      `json:"archive"`
	Completed         bool             `json:"completed"`
}

// MigrationEntry mirrors one applied release migration.
type MigrationEntry struct {
	Version  int64  `json:"version"`
	Name     string `json:"name"`
	Checksum string `json:"checksum"`
}

// TableStat is one table's snapshot row count.
type TableStat struct {
	Name string `json:"name"`
	Rows int64  `json:"rows"`
}

// ArchiveInfo describes the published archive file.
type ArchiveInfo struct {
	File     string `json:"file"`
	SHA256   string `json:"sha256"`
	SizeByte int64  `json:"size_bytes"`
	Format   string `json:"format"`
	DataOnly bool   `json:"data_only"`
}

// MigrationDigest computes the deterministic digest over an ordered
// migration set. The algorithm is the recovery contract counterpart of the
// startup gate checksum (sha256 over up+0x00+down per file, now chained over
// the ordered version set).
func MigrationDigest(entries []MigrationEntry) string {
	hash := sha256.New()
	for _, entry := range entries {
		fmt.Fprintf(hash, "%d|%s|%s\n", entry.Version, entry.Name, entry.Checksum)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// sourceIdentityFingerprint hashes the connection coordinates (host, port,
// database) so restore can refuse a target identical to the backup source
// without ever storing or logging the coordinates themselves.
func sourceIdentityFingerprint(host, port, database string) string {
	hash := sha256.New()
	fmt.Fprintf(hash, "p202-source|%s|%s|%s", host, port, database)
	return hex.EncodeToString(hash.Sum(nil))
}
