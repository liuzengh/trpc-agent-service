// Package migrations holds the platform's versioned SQL, embedded into the
// binary so a deployment artifact carries the schema it was built against.
//
// The files themselves are the source of truth for what runs: this package
// adds no per-file Go code, so adding a migration means adding a .sql file
// and nothing else. storage/mysql decides ordering and applies them.
package migrations

import "embed"

// FS holds every *.sql file in this directory.
//
//go:embed *.sql
var FS embed.FS
