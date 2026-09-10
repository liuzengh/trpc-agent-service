// Package webui embeds the built development-console assets so the service
// remains a single deployable binary. It lives next to the Vite project so
// go:embed can reference dist directly.
package webui

import (
	"embed"
	"io/fs"
)

// files holds the production Vite build output. build.sh regenerates dist/
// from webui/src before compiling.
//
//go:embed all:dist
var files embed.FS

// FS returns the console asset tree rooted at dist.
func FS() fs.FS {
	sub, err := fs.Sub(files, "dist")
	if err != nil {
		panic(err) // unreachable: the embed directive guarantees dist exists.
	}
	return sub
}
