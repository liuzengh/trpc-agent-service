// Package web serves admin and chat pages for the Agent platform.
package web

import (
	"net/http"
	"os"
	"path/filepath"
)

// NewHandler serves the built React application and falls back to index.html
// for client-side routes.
func NewHandler(root string) http.Handler {
	if root == "" {
		root = "."
	}
	files := http.FileServer(http.Dir(root))
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		path := filepath.Join(root, filepath.Clean("/"+request.URL.Path))
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			files.ServeHTTP(writer, request)
			return
		}
		request.URL.Path = "/"
		files.ServeHTTP(writer, request)
	})
}
