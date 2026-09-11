// Package controlruntimev1 defines the authenticated owner export protocol.
package controlruntimev1

import (
	"encoding/json"
	"time"
)

const ManifestExportPath = "/internal/v1/runtime-manifests/export"
const MaxExportPageSize = 4
const MaxExportPageBytes = 5 * 1024 * 1024

// ExportPosition orders immutable events lexicographically. Null Upper denotes
// a confirmed empty initial export, not an uninitialized Worker projection.
type ExportPosition struct {
	CreatedAt time.Time `json:"created_at"`
	TenantID  string    `json:"tenant_id"`
	EventID   string    `json:"event_id"`
}
type ManifestExportPage struct {
	SchemaVersion string            `json:"schema_version"`
	SnapshotUpper *ExportPosition   `json:"snapshot_upper"`
	Events        []json.RawMessage `json:"events"`
	NextCursor    string            `json:"next_cursor"`
	Complete      bool              `json:"complete"`
}
