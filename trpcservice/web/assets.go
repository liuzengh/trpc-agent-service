package web

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed dist
var frontendFiles embed.FS

func frontendHandler() http.Handler {
	dist, err := fs.Sub(frontendFiles, "dist")
	if err != nil {
		panic(err)
	}
	files := http.FileServer(http.FS(dist))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}
		if _, err := fs.Stat(dist, path); err != nil {
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		files.ServeHTTP(w, r)
	})
}
