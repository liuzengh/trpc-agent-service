package web

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	HeaderTenantID        = "X-Tenant-ID"
	HeaderAgentAppID      = "X-Agent-App-ID"
	HeaderAgentRevisionID = "X-Agent-Revision-ID"
	HeaderSessionID       = "X-Session-ID"
	HeaderAuthorization   = "Authorization"
	HeaderRetryAfter      = "Retry-After"
	// HeaderRequestID publishes the id this platform gave the run. It is only
	// ever written, never read: a request id a client could choose is a request
	// id one tenant can use to label another's run.
	HeaderRequestID = "X-Request-ID"
)

// busyRetryAfter is what a 409 tells the client to wait. It is short because
// session_busy is usually one impatient client with two tabs open, not a queue:
// the other run is expected to finish in seconds, and there is no waiting list
// to join in the meantime.
const busyRetryAfter = 2 * time.Second

// bearerScheme is matched case-insensitively, as RFC 7235 requires.
const bearerScheme = "bearer"

// resourceIDSyntax restates the shared resource id constraint in client terms.
const resourceIDSyntax = "1-128 characters of [A-Za-z0-9._-] starting with a letter or digit"

// PlatformServer exposes the control plane and routes authenticated chat
// traffic to the revision each session is pinned to.
//
// The two authenticators are separate fields of separate types, so a chat
// credential cannot reach an admin route and an admin credential cannot reach a
// chat one. That isolation is a property of the type system here, not of a
// comparison somewhere in a handler.
type PlatformServer struct {
	repository tenant.Repository
	// profiles is the control plane's storage-profile half. It is a second
	// repository rather than a method on the first because a profile is not a
	// tenant resource in the way an app is: the data plane resolves one through
	// the same value, as a ProfileSource, and the two halves are stored and
	// tested as one contract.
	profiles storagebundle.ProfileRepository
	runs     *sessionrun.Service
	chat     identity.Authenticator
	admin    identity.AdminAuthenticator
	// capabilities answers both questions this API asks about what a tenant may
	// reach: whether a revision may run, and whether one secret reference may be
	// named. They are one value on purpose — a process that answered them from
	// two tables could entitle a credential through the gap between them.
	capabilities security.CapabilityAuthorizer
	handler      http.Handler
}

// NewPlatformServer builds the server. Every dependency is a parameter,
// including the run service: it owns the run lease, the revision pin and the
// Runtime lease, and this package holds no second copy of that sequence. HTTP
// is one entry into a run, not the definition of one, and the IM channels that
// come next enter through the same service.
//
// Nothing has a permissive default and nothing may be nil. A control plane that
// started with no way to authenticate, or with no opinion on which revisions
// may run, is a control plane whose safety depends on nobody finding it; a
// process that started with no run service would be coordinating nothing, and
// one with no profile repository would have an admin route that stores storage
// arrangements nowhere.
func NewPlatformServer(
	repository tenant.Repository,
	profiles storagebundle.ProfileRepository,
	runs *sessionrun.Service,
	chatAuthenticator identity.Authenticator,
	adminAuthenticator identity.AdminAuthenticator,
	capabilityAuthorizer security.CapabilityAuthorizer,
) (*PlatformServer, error) {
	if repository == nil {
		return nil, fmt.Errorf("web: tenant repository is required")
	}
	if profiles == nil {
		return nil, fmt.Errorf("web: backend profile repository is required")
	}
	if runs == nil {
		return nil, fmt.Errorf("web: run service is required")
	}
	if chatAuthenticator == nil {
		return nil, fmt.Errorf("web: chat authenticator is required")
	}
	if adminAuthenticator == nil {
		return nil, fmt.Errorf("web: admin authenticator is required")
	}
	if capabilityAuthorizer == nil {
		return nil, fmt.Errorf("web: revision authorizer is required")
	}
	server := &PlatformServer{
		repository:   repository,
		profiles:     profiles,
		runs:         runs,
		chat:         chatAuthenticator,
		admin:        adminAuthenticator,
		capabilities: capabilityAuthorizer,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/v1/chat/completions", server.handleChatCompletions)
	// The chat page and its assets. It is registered last and at "/" because it
	// is also the catch-all: handleUI answers the two page URLs and the embedded
	// asset names, and every other path with a 404. See handleUI.
	mux.HandleFunc("/", handleUI)
	// The admin subtree is deliberately not registered on the mux: it is taken
	// before the mux runs at all. See adminFirst.
	server.handler = adminFirst(mux, server.handleAdmin)
	return server, nil
}

// adminFirst puts the control-plane trust boundary in front of the router.
//
// The whole /admin subtree is one handler, not just the paths that exist. Left
// to a router, every other admin path would be answered by the router itself —
// before anything has authenticated — so "does this admin route exist" would be
// a question any unauthenticated caller could ask.
//
// http.ServeMux cannot be that router, because it answers about /admin before
// it dispatches: it cleans the request path first, and a path that changes
// under cleaning gets a 301 to the cleaned form no matter what is registered.
// /admin//v1/tenants, /admin/./v1/tenants and /admin/v1/tenants/../secrets are
// each a redirect written for a caller holding no credential. So the prefix is
// matched here, on the raw URL.Path, and handleAdmin runs before the mux ever
// sees the request.
//
// handleAdmin then routes on that same raw path by exact comparison, which is
// what makes this safe rather than merely quiet: no traversal is resolved on
// the way in, so a path that spells a real route oddly is not that route. It
// authenticates, and is then a 404 like any other name that is not there.
func adminFirst(mux http.Handler, admin http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAdminPath(r.URL.Path) {
			admin(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// isAdminPath reports whether path addresses the control plane. The bare
// prefix is included: /admin is answered, never redirected to /admin/.
func isAdminPath(path string) bool {
	return path == adminPathPrefix || strings.HasPrefix(path, adminPathPrefix+"/")
}

func (s *PlatformServer) Handler() http.Handler {
	return s.handler
}

// handleChatCompletions derives the whole run scope from the credential. The
// request body and the routing headers can only narrow that scope or be
// rejected; they can never widen it.
func (s *PlatformServer) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	// Every chat response carries these, including the ones that fail before
	// routing: without them a browser cannot read the documented JSON error of a
	// 401, 403, 400 or 409 answer, only an opaque CORS failure.
	writeChatCORSHeaders(w)
	if r.Method == http.MethodOptions {
		writeChatPreflight(w)
		return
	}
	// The platform's own id for this run, minted here and never read from the
	// request: a client that could choose it could label another tenant's run,
	// or send an empty one and label nothing. It is published immediately so
	// that every answer below carries it — a caller reporting a 401, a 409 or a
	// 500 has something to quote, which is the whole point of a correlation id.
	requestID := uuid.NewString()
	w.Header().Set(HeaderRequestID, requestID)
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost, http.MethodOptions)
		return
	}
	caller, ok := s.authenticateChat(w, r)
	if !ok {
		return
	}
	// X-Tenant-ID is an assertion a client may make about the credential it
	// used. It never selects the tenant, so a mismatch is a refusal.
	if asserted := r.Header.Get(HeaderTenantID); asserted != "" && asserted != caller.TenantID {
		writeAPIError(
			w,
			http.StatusForbidden,
			"forbidden",
			HeaderTenantID+" does not match the authenticated tenant",
		)
		return
	}
	appID := r.Header.Get(HeaderAgentAppID)
	if err := tenant.ValidateResourceID("app id", appID); err != nil {
		writeAPIError(
			w,
			http.StatusBadRequest,
			"missing_route",
			HeaderAgentAppID+" is required and must be "+resourceIDSyntax,
		)
		return
	}
	if !caller.AllowsApp(appID) {
		writeAPIError(
			w,
			http.StatusForbidden,
			"forbidden",
			"this credential may not call the requested agent app",
		)
		return
	}
	sessionID, ok := chatSessionID(w, r)
	if !ok {
		return
	}
	// Everything the run needs, and nothing that is HTTP's: the service takes
	// the lease, reads or adopts the pin, leases the Runtime and attaches the
	// trusted scope, in that order, and gives back a handle that undoes exactly
	// those in reverse.
	started, err := s.runs.Start(r.Context(), sessionrun.Request{
		RequestID:    requestID,
		TenantID:     caller.TenantID,
		AppID:        appID,
		PrincipalID:  caller.PrincipalID,
		SessionID:    sessionID,
		RevisionHint: strings.TrimSpace(r.Header.Get(HeaderAgentRevisionID)),
	})
	if err != nil {
		writeRunRefusal(w, sessionID, err)
		return
	}
	// ServeHTTP returning is the boundary — that is what "the run is over"
	// means here, and Close is what decides whether the lease is given back or
	// left to its TTL.
	defer started.Close()

	handler, err := started.OpenAIHandler()
	if err != nil {
		writeDomainError(w, err)
		return
	}
	// The adapter owns the status line from here on, so every response header
	// has to be in place first: a client that let the platform mint a session
	// id still needs it back from a 400 or a 500 answer. The fence is not among
	// them: it is an observation handle, it admits nothing, and publishing it
	// would invite a client to treat it as a guarantee it is not.
	writeChatResponseHeaders(w, sessionID, started.Scope().RevisionID)
	// Keep the adapter's own view of the session consistent with the pinned
	// one. contextRunner ignores this header either way.
	r.Header.Set(HeaderSessionID, sessionID)
	handler.ServeHTTP(w, r.WithContext(started.Context()))
}

// writeRunRefusal maps a run that never started onto this endpoint's contract.
//
// The run service reports what happened; the status code, the retry hint and
// the wording are HTTP's, and they stay here. The three that are not ordinary
// domain errors:
//
//   - busy is 409 with a Retry-After, because the platform is healthy and the
//     same request will work shortly;
//   - coordination that could not answer is 503 with no retry hint, because
//     nobody knows when it comes back and suggesting two seconds would turn an
//     outage into a stampede. A cancelled request context arrives here too and
//     shares this answer: there is no status for "the caller stopped
//     listening", and no caller left to read one;
//   - a pin conflict is 409, and names the revision the session is actually
//     pinned to, because that is the one fact that lets the client fix it.
func writeRunRefusal(w http.ResponseWriter, sessionID string, err error) {
	var pinConflict *sessionrun.PinConflictError
	switch {
	case errors.Is(err, sessionlease.ErrSessionBusy):
		w.Header().Set(HeaderRetryAfter, strconv.Itoa(int(busyRetryAfter.Seconds())))
		writeAPIError(w, http.StatusConflict, "session_busy", fmt.Sprintf(
			"session %q is already being run; retry when that run finishes",
			sessionID,
		))
	case errors.Is(err, sessionrun.ErrCoordinationUnavailable):
		writeAPIError(
			w,
			http.StatusServiceUnavailable,
			"coordination_unavailable",
			"run coordination is unavailable; this request was not started",
		)
	case errors.As(err, &pinConflict):
		writeAPIError(w, http.StatusConflict, "pin_conflict", fmt.Sprintf(
			"session %q is pinned to revision %q",
			pinConflict.SessionID,
			pinConflict.PinnedRevisionID,
		))
	default:
		writeDomainError(w, err)
	}
}

func (s *PlatformServer) authenticateChat(
	w http.ResponseWriter,
	r *http.Request,
) (identity.Identity, bool) {
	token, ok := bearerToken(r.Header.Get(HeaderAuthorization))
	if !ok {
		writeUnauthenticated(w, "a Bearer credential is required")
		return identity.Identity{}, false
	}
	caller, err := s.chat.Authenticate(r.Context(), token)
	if err != nil {
		if errors.Is(err, identity.ErrForbidden) {
			writeAPIError(w, http.StatusForbidden, "forbidden", "this credential is not allowed")
			return identity.Identity{}, false
		}
		writeUnauthenticated(w, "the credential is not valid")
		return identity.Identity{}, false
	}
	if err := caller.Validate(); err != nil {
		writeUnauthenticated(w, "the credential is not valid")
		return identity.Identity{}, false
	}
	return caller, true
}

// chatSessionID returns the client session id, or a new one the platform owns.
// A generated id is echoed back in every response so the caller can continue
// the same conversation.
func chatSessionID(w http.ResponseWriter, r *http.Request) (string, bool) {
	sessionID := strings.TrimSpace(r.Header.Get(HeaderSessionID))
	if sessionID == "" {
		return uuid.NewString(), true
	}
	if err := tenant.ValidateResourceID("session id", sessionID); err != nil {
		writeAPIError(
			w,
			http.StatusBadRequest,
			"invalid_session_id",
			HeaderSessionID+" must be "+resourceIDSyntax,
		)
		return "", false
	}
	return sessionID, true
}

func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(strings.TrimSpace(header), " ")
	if !found || !strings.EqualFold(scheme, bearerScheme) {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}

func writeUnauthenticated(w http.ResponseWriter, message string) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeAPIError(w, http.StatusUnauthorized, "unauthenticated", message)
}

// writeChatCORSHeaders publishes what every chat response shares, preflight and
// actual alike. It runs before authentication, so the browser of a caller that
// was refused can still read why.
func writeChatCORSHeaders(w http.ResponseWriter) {
	header := w.Header()
	header.Set("Access-Control-Allow-Origin", "*")
	// Retry-After is exposed because it is the actionable part of a 409: a
	// browser client that cannot read it has been told "busy" with no idea when
	// to come back, and will either give up or retry immediately. X-Request-ID
	// is exposed for the same reason in the other direction: it is the only part
	// of a 500 a browser client can usefully quote back to an operator.
	header.Set("Access-Control-Expose-Headers", strings.Join([]string{
		HeaderSessionID,
		HeaderAgentRevisionID,
		HeaderRetryAfter,
		HeaderRequestID,
	}, ", "))
}

// writeChatResponseHeaders publishes the run scope. It stays at the point where
// the session and the revision are actually known: a request refused earlier has
// no revision to name, and would be pinned to nothing.
func writeChatResponseHeaders(w http.ResponseWriter, sessionID string, revisionID string) {
	header := w.Header()
	header.Set(HeaderSessionID, sessionID)
	header.Set(HeaderAgentRevisionID, revisionID)
}

// writeChatPreflight stays unauthenticated: a browser cannot attach the
// credential to a preflight request.
func writeChatPreflight(w http.ResponseWriter) {
	header := w.Header()
	header.Set("Access-Control-Allow-Methods", http.MethodPost)
	header.Set("Access-Control-Allow-Headers", strings.Join([]string{
		"Content-Type",
		HeaderAuthorization,
		HeaderTenantID,
		HeaderAgentAppID,
		HeaderAgentRevisionID,
		HeaderSessionID,
	}, ", "))
	w.WriteHeader(http.StatusNoContent)
}
