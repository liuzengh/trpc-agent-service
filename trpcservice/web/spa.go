package web

import (
	"bytes"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

// NewConsoleSPAHandler serves the embedded development console at /console.
// Unknown non-asset paths fall back to index.html so client-side routing keeps
// working without server-side route registration.
func NewConsoleSPAHandler(files fs.FS) (http.Handler, error) {
	if files == nil {
		return nil, fmt.Errorf("console asset filesystem is required")
	}
	index, err := fs.ReadFile(files, "index.html")
	if err != nil {
		return nil, fmt.Errorf("read console index.html: %w", err)
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			writer.Header().Set("Allow", "GET, HEAD")
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(request.URL.Path, "/console/")
		if name == request.URL.Path || name == "" {
			name = "index.html"
		}
		if data, readErr := fs.ReadFile(files, name); readErr == nil {
			if info, statErr := fs.Stat(files, name); statErr == nil {
				http.ServeContent(writer, request, name, info.ModTime(), bytes.NewReader(data))
				return
			}
		}
		// Asset requests must fail loudly when the embedded production bundle is
		// stale or incomplete. Returning index.html here produces a misleading
		// 200 with the wrong MIME type and makes missing frontend packages much
		// harder to diagnose in browsers and deployment probes.
		if strings.HasPrefix(name, "assets/") || strings.HasPrefix(name, "brands/") {
			http.NotFound(writer, request)
			return
		}
		http.ServeContent(writer, request, "index.html", time.Time{}, bytes.NewReader(index))
	}), nil
}
