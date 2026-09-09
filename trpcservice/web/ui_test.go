package web

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The page is served at the two names it claims and at no others. The negative
// half is the point: handleUI is registered at "/", so if it answered anything
// it did not recognise with the page, every unknown path on this platform would
// become a 200 and "does this route exist" would stop having an answer.
func TestUIServesTheChatPageOnExactRoutesOnly(t *testing.T) {
	platform := newPlatformTestServer(t)

	for _, path := range []string{"/", "/chat"} {
		page := requireStatus(t, platform.handler, http.MethodGet, path, "", nil, http.StatusOK)
		require.Equal(t, "text/html; charset=utf-8", page.Header().Get("Content-Type"))
		require.Contains(t, page.Body.String(), "<!DOCTYPE html>")
		// The chat UI itself, not a placeholder: the composer, the script that
		// drives it and the product name it carries.
		require.Contains(t, page.Body.String(), `id="composer"`)
		require.Contains(t, page.Body.String(), `src="/assets/app.js"`)
		require.Contains(t, page.Body.String(), "tRPC Agent")
	}

	// Neighbouring and near-miss names are 404s, not the page.
	for _, path := range []string{
		"/chat/", "/chat/x", "/index.html", "/ui", "/ui/index.html", "/assets",
		"/assets/", "/assets/app.js.map", "/assets/icons", "/favicon.ico", "/nope",
	} {
		missing := requireStatus(t, platform.handler, http.MethodGet, path, "", nil, http.StatusNotFound)
		require.Contains(t, missing.Body.String(), `"code":"not_found"`, path)
		require.NotContains(t, missing.Body.String(), "<!DOCTYPE html>", path)
	}

	// The method never turns an unknown path into a known one. Serving the page
	// from the root made this handler the mux's catch-all, and a path this
	// platform does not have has to answer the way it did before that: 404, not
	// a list of the methods some other route would have taken.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		for _, path := range []string{"/nope", "/assets/nope.js", "/chat/x"} {
			missing := requireStatus(t, platform.handler, method, path, "", nil, http.StatusNotFound)
			require.Contains(t, missing.Body.String(), `"code":"not_found"`, method+" "+path)
			require.Empty(t, missing.Header().Get("Allow"), method+" "+path)
		}
	}
}

// Every asset the page references is served, with a declared type and no
// invitation to guess at it.
func TestUIServesTheReferencedAssets(t *testing.T) {
	platform := newPlatformTestServer(t)

	page := requireStatus(t, platform.handler, http.MethodGet, "/", "", nil, http.StatusOK)
	body := page.Body.String()
	require.Equal(t, "no-store", page.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", page.Header().Get("X-Content-Type-Options"))
	// A page that holds a chat credential in memory says where it may load from
	// and who may frame it.
	require.Contains(t, page.Header().Get("Content-Security-Policy"), "default-src 'none'")
	require.Contains(t, page.Header().Get("Content-Security-Policy"), "connect-src 'self'")
	require.Equal(t, "DENY", page.Header().Get("X-Frame-Options"))

	assets := map[string]string{
		"/assets/app.css":              "text/css; charset=utf-8",
		"/assets/app.js":               "text/javascript; charset=utf-8",
		"/assets/icons/send.svg":       "image/svg+xml",
		"/assets/icons/square.svg":     "image/svg+xml",
		"/assets/icons/square-pen.svg": "image/svg+xml",
		"/assets/icons/settings.svg":   "image/svg+xml",
		"/assets/icons/panel-left.svg": "image/svg+xml",
		"/assets/icons/x.svg":          "image/svg+xml",
		"/assets/icons/copy.svg":       "image/svg+xml",
		"/assets/icons/check.svg":      "image/svg+xml",
		"/assets/icons/LICENSE.txt":    "text/plain; charset=utf-8",
	}
	for path, contentType := range assets {
		response := requireStatus(t, platform.handler, http.MethodGet, path, "", nil, http.StatusOK)
		require.Equal(t, contentType, response.Header().Get("Content-Type"), path)
		require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"), path)
		require.NotEmpty(t, response.Body.String(), path)
		// Only the document carries the page policy; an icon has no use for it.
		require.Empty(t, response.Header().Get("Content-Security-Policy"), path)
	}

	// The vendored icons keep their licence with them, and the page's own
	// stylesheet is what asks for them.
	license := requireStatus(
		t, platform.handler, http.MethodGet, "/assets/icons/LICENSE.txt", "", nil, http.StatusOK,
	)
	require.Contains(t, license.Body.String(), "ISC License")
	styles := requireStatus(t, platform.handler, http.MethodGet, "/assets/app.css", "", nil, http.StatusOK)
	for _, icon := range []string{"send", "square", "square-pen", "settings", "panel-left", "x", "copy", "check"} {
		require.Contains(t, styles.Body.String(), "icons/"+icon+".svg")
	}
	require.Contains(t, body, `href="/assets/app.css"`)
}

// The page is read, not written to, and a stale copy of it must not survive a
// rebuild. Assets revalidate rather than being stored, which is what makes the
// ETag worth publishing.
func TestUIAssetMethodsAndRevalidation(t *testing.T) {
	platform := newPlatformTestServer(t)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		refused := requireStatus(
			t, platform.handler, method, "/", "", nil, http.StatusMethodNotAllowed,
		)
		require.Equal(t, "GET, HEAD", refused.Header().Get("Allow"))
	}
	head := requireStatus(t, platform.handler, http.MethodHead, "/", "", nil, http.StatusOK)
	require.Equal(t, "text/html; charset=utf-8", head.Header().Get("Content-Type"))

	script := requireStatus(t, platform.handler, http.MethodGet, "/assets/app.js", "", nil, http.StatusOK)
	require.Equal(t, "no-cache", script.Header().Get("Cache-Control"))
	etag := script.Header().Get("ETag")
	require.NotEmpty(t, etag)
	requireStatus(
		t, platform.handler, http.MethodGet, "/assets/app.js", "",
		map[string]string{"If-None-Match": etag}, http.StatusNotModified,
	)
}

// Adding a document root must not have moved the API, and the page must not
// have become a way into the control plane.
func TestUIDoesNotDisturbTheAPIOrTheControlPlane(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	// The chat endpoint still answers on its own terms, including the CORS
	// headers a browser needs to read a refusal.
	answer := requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"deterministic-echo","stream":true,"messages":[{"role":"user","content":"hello"}]
	}`, chatHeaders(keyTenantA, appAssistant), http.StatusOK)
	require.Equal(t, "text/event-stream", answer.Header().Get("Content-Type"))
	require.Contains(t, answer.Body.String(), "data: [DONE]")
	require.NotEmpty(t, answer.Header().Get(HeaderSessionID))
	require.NotEmpty(t, answer.Header().Get(HeaderRequestID))
	require.Equal(t, "*", answer.Header().Get("Access-Control-Allow-Origin"))

	// GET on the chat route is still a 405 rather than the page, and the health
	// probe still answers JSON.
	wrongMethod := requireStatus(
		t, platform.handler, http.MethodGet, chatPath, "", nil, http.StatusMethodNotAllowed,
	)
	require.Equal(t, "POST, OPTIONS", wrongMethod.Header().Get("Allow"))
	health := requireStatus(t, platform.handler, http.MethodGet, "/healthz", "", nil, http.StatusOK)
	require.JSONEq(t, `{"status":"ok"}`, health.Body.String())

	// The admin boundary still runs before the router, so no admin path — real,
	// unknown, or spelled to look like an asset — is answered by the UI.
	for _, path := range []string{
		adminPathPrefix, "/admin/v1/tenants", "/admin/assets/app.js", "/admin/nope",
	} {
		refused := requireStatus(t, platform.handler, http.MethodGet, path, "", nil, http.StatusUnauthorized)
		require.NotContains(t, refused.Body.String(), "<!DOCTYPE html>", path)
	}
	// And the page never carries the CORS grant the chat endpoint publishes:
	// it is same-origin, and a cross-origin reader of this HTML is not a client
	// this platform has.
	page := requireStatus(t, platform.handler, http.MethodGet, "/", "", nil, http.StatusOK)
	require.Empty(t, page.Header().Get("Access-Control-Allow-Origin"))
}

// Nothing outside the embedded tree is reachable, and nothing inside it is
// reachable under a name the page does not use.
func TestUIServesOnlyEmbeddedAssets(t *testing.T) {
	platform := newPlatformTestServer(t)

	for _, path := range []string{
		"/assets/../ui/index.html",
		"/assets/icons/../app.js",
		"/assets/./app.js",
		"/assets//app.js",
	} {
		// http.ServeMux cleans the path before dispatch, so these are the
		// router's redirect to the cleaned form; what matters is that none of
		// them is served as a file. Following the redirect is somebody else's
		// request, and it lands on the cleaned name or on a 404.
		response := serve(platform.handler, http.MethodGet, path, "", nil)
		require.Contains(
			t,
			[]int{http.StatusMovedPermanently, http.StatusNotFound},
			response.Code,
			path,
		)
		require.NotContains(t, response.Body.String(), "'use strict'", path)
	}

	// The tree is loaded once at start-up and is exactly what the page needs.
	require.Len(t, uiAssets, 12)
	for name := range uiAssets {
		require.True(t, strings.HasPrefix(name, uiRoot+"/"), name)
	}
}
