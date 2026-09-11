package admin

import (
	"net/http"
	"strings"
	"testing"
)

// The operator surfaces added with the tool ledger (approved plan, P3:
// "Admin 增加 DLQ/unknown 查询处置"). What a test can assert here without
// staging a whole blocked session: the routes exist, are admin-only, answer
// with real shapes on an empty plane, and refuse malformed dispositions
// *before* touching the ledger. The unblock logic itself is covered by the
// execution package's real-MySQL tests; re-proving it through HTTP would be
// testing the same code twice.
func TestToolLedgerOperatorSurfaces(t *testing.T) {
	s, _, token := setupControlPlaneService(t)
	h := s.Handler()

	t.Run("listing requires a bearer token", func(t *testing.T) {
		if got := doRaw(t, h, http.MethodGet, "/admin/v2/tool-calls", "", ""); got.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated tool-calls = %d", got.Code)
		}
	})

	t.Run("empty ledger answers with an empty list, not a 500", func(t *testing.T) {
		got := doAs(t, h, http.MethodGet, "/admin/v2/tool-calls?status=unknown", "", token)
		if got.Code != http.StatusOK || got.Body != "[]\n" {
			t.Fatalf("tool-calls = %d %q", got.Code, got.Body)
		}
		blocked := doAs(t, h, http.MethodGet, "/admin/v2/blocked-sessions", "", token)
		if blocked.Code != http.StatusOK || blocked.Body != "[]\n" {
			t.Fatalf("blocked-sessions = %d %q", blocked.Code, blocked.Body)
		}
		dead := doAs(t, h, http.MethodGet, "/admin/v2/dead-letters", "", token)
		if dead.Code != http.StatusOK ||
			!strings.Contains(dead.Body, `"reply_outbox":[]`) ||
			!strings.Contains(dead.Body, `"outbox_events":[]`) {
			t.Fatalf("dead-letters = %d %q", dead.Code, dead.Body)
		}
	})

	t.Run("bad filters are refused", func(t *testing.T) {
		if got := doAs(t, h, http.MethodGet, "/admin/v2/tool-calls?session_pk=abc", "", token); got.Code != http.StatusBadRequest {
			t.Fatalf("bad session_pk = %d", got.Code)
		}
		if got := doAs(t, h, http.MethodGet, "/admin/v2/tool-calls?limit=nan", "", token); got.Code != http.StatusBadRequest {
			t.Fatalf("bad limit = %d", got.Code)
		}
	})

	t.Run("disposition is validated before the ledger is touched", func(t *testing.T) {
		got := doAs(t, h, http.MethodPost, "/admin/v2/tool-calls/exec-1:1:1/resolve", `{"resolution":"maybe"}`, token)
		if got.Code != http.StatusBadRequest {
			t.Fatalf("bogus resolution = %d %q", got.Code, got.Body)
		}
		unknown := doAs(t, h, http.MethodPost, "/admin/v2/tool-calls/no-such-call/resolve", `{"resolution":"cancelled"}`, token)
		if unknown.Code != http.StatusNotFound {
			t.Fatalf("unknown call = %d %q", unknown.Code, unknown.Body)
		}
	})

	t.Run("dead-letter requeue is validated", func(t *testing.T) {
		if got := doAs(t, h, http.MethodPost, "/admin/v2/dead-letters/requeue", `{"kind":"nope","id":1}`, token); got.Code != http.StatusBadRequest {
			t.Fatalf("bogus kind = %d", got.Code)
		}
		if got := doAs(t, h, http.MethodPost, "/admin/v2/dead-letters/requeue", `{"kind":"reply_outbox","id":999}`, token); got.Code != http.StatusNotFound {
			t.Fatalf("missing dead letter = %d", got.Code)
		}
		if got := doAs(t, h, http.MethodPost, "/admin/v2/dead-letters/requeue", ``, token); got.Code != http.StatusBadRequest {
			t.Fatalf("empty body = %d", got.Code)
		}
	})

	t.Run("wrong method is refused", func(t *testing.T) {
		if got := doAs(t, h, http.MethodDelete, "/admin/v2/tool-calls", "", token); got.Code != http.StatusMethodNotAllowed {
			t.Fatalf("DELETE tool-calls = %d", got.Code)
		}
	})
}
