// Package migrations embeds Control API database migrations into the service
// binary.
package migrations

import "embed"

// Files contains every versioned SQL migration shipped with Control API.
//
//go:embed *.sql
var Files embed.FS
