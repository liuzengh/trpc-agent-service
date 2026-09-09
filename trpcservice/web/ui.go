package web

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

// The chat page is this platform's own client. It is served by the same binary
// and talks to the same /v1/chat/completions as any other caller, with a chat
// credential the user types into it — there is no second protocol, no server
// state behind it, and nothing of the control plane in it.
//
// The icons under ui/assets/icons are vendored verbatim from the lucide-static
// package, version 1.41.0 (https://lucide.dev). Each file keeps its upstream
// licence comment, and ui/assets/icons/LICENSE.txt is that package's licence in
// full: ISC, with MIT for the icons derived from Feather.
//
//go:embed ui/index.html ui/assets/app.css ui/assets/app.js ui/assets/icons
var uiFiles embed.FS

const (
	// uiRoot is the embedded document root. Every served path is this prefix
	// plus a name that came out of the embedded tree, never out of a request.
	uiRoot = "ui"
	// uiAssetPrefix is the one subtree of that root a URL may name.
	uiAssetPrefix = "/assets/"
	// uiPageFile is what both page routes serve.
	uiPageFile = uiRoot + "/index.html"
)

// uiPagePaths are the exact URLs that answer with the chat page. Exact, because
// a document root that answered every unknown path with 200 and an HTML page
// would make "does this route exist" unanswerable — for a client, for a probe,
// and for the tests that assert the API surface is what it says it is.
var uiPagePaths = map[string]bool{
	"/":     true,
	"/chat": true,
}

// uiAsset is one embedded file, prepared once at start-up.
type uiAsset struct {
	body        []byte
	contentType string
	// cacheControl differs by kind: the page is never stored, because it is the
	// shell that names the current asset URLs and a stale one would load a UI
	// this binary no longer serves. Assets are revalidated instead of stored,
	// so a rebuilt binary wins immediately while an unchanged one still answers
	// 304 from the ETag below.
	cacheControl string
	etag         string
	// page marks the HTML document, which carries the framing and embedding
	// restrictions a page holding a chat credential should carry.
	page bool
}

// uiAssets is the whole servable surface, keyed by embedded name. It is built
// once because the content cannot change while the process runs, and because a
// map built from the embedded tree is what makes path handling safe: a request
// selects an entry or it selects nothing.
var uiAssets = loadUIAssets()

func loadUIAssets() map[string]uiAsset {
	assets := make(map[string]uiAsset)
	err := fs.WalkDir(uiFiles, uiRoot, func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		body, readErr := uiFiles.ReadFile(name)
		if readErr != nil {
			return readErr
		}
		contentType, typeErr := uiContentType(name)
		if typeErr != nil {
			return typeErr
		}
		digest := sha256.Sum256(body)
		isPage := name == uiPageFile
		cacheControl := "no-cache"
		if isPage {
			cacheControl = "no-store"
		}
		assets[name] = uiAsset{
			body:         body,
			contentType:  contentType,
			cacheControl: cacheControl,
			etag:         `"` + hex.EncodeToString(digest[:16]) + `"`,
			page:         isPage,
		}
		return nil
	})
	if err != nil {
		// The tree is compiled in, so this cannot depend on anything a request
		// or an operator does: reaching it means the binary was built with an
		// asset it has no content type for, which is a build defect.
		panic(fmt.Sprintf("web: embedded ui assets: %v", err))
	}
	if _, ok := assets[uiPageFile]; !ok {
		panic("web: embedded ui assets: " + uiPageFile + " is missing")
	}
	return assets
}

// uiContentType maps an embedded file to its type from a fixed table rather
// than from mime.TypeByExtension, whose answers come from the host: /etc/mime.types
// on one machine and the registry on another. What this binary serves should
// not depend on which machine it was started on, and an unknown extension is a
// build-time failure rather than a guess.
func uiContentType(name string) (string, error) {
	switch path.Ext(name) {
	case ".html":
		return "text/html; charset=utf-8", nil
	case ".css":
		return "text/css; charset=utf-8", nil
	case ".js":
		return "text/javascript; charset=utf-8", nil
	case ".svg":
		return "image/svg+xml", nil
	case ".txt":
		return "text/plain; charset=utf-8", nil
	default:
		return "", fmt.Errorf("no content type for %q", name)
	}
}

// handleUI serves the chat page and its assets.
//
// It is registered at "/" because the page lives at the site root, which also
// makes it the mux's catch-all — so everything it does not recognise has to end
// as a 404 here. The two page URLs are matched exactly and every other name is
// resolved by looking it up in the embedded tree, so a path this binary does
// not carry cannot be answered with something it does. Nothing is joined onto a
// filesystem path and nothing is cleaned here: a request either names an
// embedded file or it names nothing.
//
// The route is resolved before the method, because being the catch-all means a
// path this platform does not have must answer the way it did before there was
// a page here: 404, for any method. Only a name this binary does carry is in a
// position to say which methods it takes.
func handleUI(w http.ResponseWriter, r *http.Request) {
	asset, ok := uiAssets[uiAssetName(r.URL.Path)]
	if !ok {
		writeAPIError(w, http.StatusNotFound, "not_found", "route not found")
		return
	}
	// A page and its assets are read, never written to.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w, http.MethodGet, http.MethodHead)
		return
	}
	serveUIAsset(w, r, asset)
}

// uiAssetName translates a URL path into an embedded name, or into one that is
// not in the map. Only /assets/<name> is translated, so the rest of the
// embedded tree — including the page itself under its file name — is reachable
// through the page routes alone.
func uiAssetName(urlPath string) string {
	if uiPagePaths[urlPath] {
		return uiPageFile
	}
	if rest, ok := strings.CutPrefix(urlPath, uiAssetPrefix); ok {
		return uiRoot + uiAssetPrefix + rest
	}
	return ""
}

func serveUIAsset(w http.ResponseWriter, r *http.Request, asset uiAsset) {
	header := w.Header()
	header.Set("Content-Type", asset.contentType)
	// The type is declared, so it must not be guessed at: a browser that sniffs
	// an SVG or a licence file into something else is a browser executing an
	// asset this platform said was not executable.
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Cache-Control", asset.cacheControl)
	header.Set("ETag", asset.etag)
	if asset.page {
		// The page holds a chat credential in memory for as long as it is open.
		// It has no reason to be framed by another site, to be a form's target,
		// or to load or reach anything that did not come from this binary, and
		// saying so is what keeps an injected string from becoming a way to send
		// that credential somewhere else.
		header.Set("Content-Security-Policy", strings.Join([]string{
			"default-src 'none'",
			"script-src 'self'",
			"style-src 'self'",
			"img-src 'self' data:",
			"connect-src 'self'",
			"base-uri 'none'",
			"form-action 'self'",
			"frame-ancestors 'none'",
		}, "; "))
		header.Set("Referrer-Policy", "no-referrer")
		header.Set("X-Frame-Options", "DENY")
	}
	// ServeContent for the conditional request and HEAD handling, with no
	// modification time: the content is compiled in, so the ETag above is the
	// only version this binary can honestly claim.
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(asset.body))
}
