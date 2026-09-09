package web

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// This file is the attack surface of the control plane, written as tests.
//
// Every case here is something a caller with a real credential could try, so
// none of them is about a malformed request: they are about a well-formed one
// aimed somewhere it does not belong. The properties being defended are that
// authentication happens before anything else, that a tenant admin cannot see
// past its own tenant — not even far enough to make this process ask the
// database — and that a refusal never becomes an oracle.

// notFoundBody is the one not-found answer the Admin API gives. A cross-tenant
// refusal must be byte-identical to it: if the two ever differ by a word, the
// difference is a way to enumerate another tenant's resources.
const notFoundBody = `{"error":{"code":"not_found","message":"resource not found"}}`

// failingRepository fails the test if anything reaches it.
//
// Asserting the status code alone would not prove what these tests claim.
// A 404 is what a cross-tenant read must return, but it is also what a real
// repository returns for a resource that is not there — so a handler that
// queried the database on another tenant's behalf and then relayed its
// not-found would pass a status assertion while doing the exact thing the
// short-circuit exists to prevent.
type failingRepository struct {
	t *testing.T
}

func (r *failingRepository) fail(method string) {
	r.t.Helper()
	r.t.Errorf("the repository was reached: %s", method)
}

func (r *failingRepository) CreateTenant(
	context.Context, tenant.Tenant,
) (tenant.Tenant, error) {
	r.fail("CreateTenant")
	return tenant.Tenant{}, tenant.ErrNotFound
}

func (r *failingRepository) GetTenant(context.Context, string) (tenant.Tenant, error) {
	r.fail("GetTenant")
	return tenant.Tenant{}, tenant.ErrNotFound
}

func (r *failingRepository) CreateAgentApp(
	context.Context, tenant.TenantContext, tenant.AgentApp,
) (tenant.AgentApp, error) {
	r.fail("CreateAgentApp")
	return tenant.AgentApp{}, tenant.ErrNotFound
}

func (r *failingRepository) GetAgentApp(
	context.Context, tenant.TenantContext, string,
) (tenant.AgentApp, error) {
	r.fail("GetAgentApp")
	return tenant.AgentApp{}, tenant.ErrNotFound
}

func (r *failingRepository) CreateRevision(
	context.Context, tenant.TenantContext, tenant.AgentRevision,
) (tenant.AgentRevision, error) {
	r.fail("CreateRevision")
	return tenant.AgentRevision{}, tenant.ErrNotFound
}

func (r *failingRepository) GetRevision(
	context.Context, tenant.TenantContext, string, string,
) (tenant.AgentRevision, error) {
	r.fail("GetRevision")
	return tenant.AgentRevision{}, tenant.ErrNotFound
}

func (r *failingRepository) PublishRevision(
	context.Context, tenant.TenantContext, string, string,
) (tenant.AgentApp, tenant.AgentRevision, error) {
	r.fail("PublishRevision")
	return tenant.AgentApp{}, tenant.AgentRevision{}, tenant.ErrNotFound
}

func (r *failingRepository) ResolveRevision(
	context.Context, tenant.TenantContext, string, string,
) (tenant.AgentRevision, error) {
	r.fail("ResolveRevision")
	return tenant.AgentRevision{}, tenant.ErrNotFound
}

// tenant-a's admin holds a real, valid credential. Every request below is
// correctly formed and correctly authenticated; the only thing wrong with it is
// the tenant in the path.
func TestAdminTenantAdminCannotReachAnotherTenant(t *testing.T) {
	// The repository fails the test if it is called, so "the short-circuit came
	// first" is observed rather than inferred from a status code.
	platform := newPlatformTestServerWith(t, platformTestOptions{
		repository: &failingRepository{t: t},
	})

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{
			name:   "read the tenant",
			method: http.MethodGet,
			path:   "/admin/v1/tenants/tenant-b",
		},
		{
			name:   "read an app",
			method: http.MethodGet,
			path:   "/admin/v1/tenants/tenant-b/apps/assistant",
		},
		{
			name:   "read a revision",
			method: http.MethodGet,
			path:   "/admin/v1/tenants/tenant-b/apps/assistant/revisions/revision-1",
		},
		{
			name:   "create an app",
			method: http.MethodPost,
			path:   "/admin/v1/tenants/tenant-b/apps",
			body:   `{"id":"stolen","name":"Stolen"}`,
		},
		{
			name:   "create a revision",
			method: http.MethodPost,
			path:   "/admin/v1/tenants/tenant-b/apps/assistant/revisions",
			body: `{"id":"revision-9","revision_no":9,"config":{
				"agent_name":"x","instruction":"x",
				"model":{"provider":"deterministic","name":"echo-v1"}}}`,
		},
		{
			name:   "publish a revision",
			method: http.MethodPost,
			path:   "/admin/v1/tenants/tenant-b/apps/assistant/revisions/revision-1/publish",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, tc.method, tc.path, tc.body,
				adminHeaders(adminKeyTenantA), http.StatusNotFound,
			)
			require.JSONEq(t, notFoundBody, response.Body.String())
			// Byte-identical, not merely equivalent JSON: two answers that differ
			// in whitespace or field order are two answers.
			require.Equal(t, notFoundBody+"\n", response.Body.String())
		})
	}
}

// The other half of the same claim: the refusal above is indistinguishable from
// what the platform says about a resource that genuinely is not there. Both
// bodies are produced here, from the same server, and compared.
func TestAdminCrossTenantRefusalMatchesARealNotFound(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	// tenant-a's admin asking for something tenant-a really does not have.
	real := requireStatus(
		t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/does-not-exist",
		"", adminHeaders(adminKeyTenantA), http.StatusNotFound,
	)
	// tenant-b's admin asking for something that does exist, under tenant-a.
	refused := requireStatus(
		t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1",
		"", adminHeaders(adminKeyTenantB), http.StatusNotFound,
	)

	require.Equal(t, real.Body.String(), refused.Body.String())
	require.Equal(t, real.Code, refused.Code)
	require.Equal(t, real.Header().Get("Content-Type"), refused.Header().Get("Content-Type"))
}

// A tenant admin administers its own tenant fully. The isolation above is worth
// nothing if it were achieved by refusing everybody.
func TestAdminTenantAdminAdministersItsOwnTenant(t *testing.T) {
	platform := newPlatformTestServer(t)
	// The tenant itself is created by the platform admin: that is the one
	// control-plane operation a tenant admin does not have.
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		`{"id":"tenant-a","slug":"tenant-a","name":"Tenant A"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)

	headers := adminHeaders(adminKeyTenantA)
	requireStatus(t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a", "", headers, http.StatusOK)
	requireStatus(t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps",
		`{"id":"assistant","name":"Assistant"}`, headers, http.StatusCreated)
	requireStatus(t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a/apps/assistant", "", headers, http.StatusOK)
	requireStatus(t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions", `{
			"id":"revision-1","revision_no":1,"config":{
				"agent_name":"test-agent","instruction":"Answer.",
				"model":{"provider":"deterministic","name":"echo-v1"}}
		}`, headers, http.StatusCreated)
	requireStatus(t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1",
		"", headers, http.StatusOK)
	requireStatus(t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish",
		"", headers, http.StatusOK)
}

// Creating a tenant is the one operation that belongs to no tenant, so it
// belongs to the role that belongs to no tenant.
//
// This is a 403 and not the 404 the cross-tenant cases get, and the difference
// is deliberate: nothing is being disclosed. A tenant admin already knows the
// tenant collection exists — it is the parent of its own tenant — so hiding the
// route would conceal nothing while making a permissions error look like a typo.
func TestAdminTenantCreationIsPlatformAdminOnly(t *testing.T) {
	platform := newPlatformTestServer(t)
	body := `{"id":"tenant-c","slug":"tenant-c","name":"Tenant C"}`

	refused := requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		body, adminHeaders(adminKeyTenantA), http.StatusForbidden)
	require.JSONEq(t, `{
		"error":{"code":"forbidden","message":"this credential is not allowed"}
	}`, refused.Body.String())

	// And the tenant was not created on the way to being refused.
	requireStatus(t, platform.handler, http.MethodGet, "/admin/v1/tenants/tenant-c",
		"", adminHeaders(adminKeyPlatform), http.StatusNotFound)

	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		body, adminHeaders(adminKeyPlatform), http.StatusCreated)
}

// Authentication happens before routing, so an unauthenticated caller learns
// nothing about the shape of this API: which paths exist, which methods they
// take, or whether a tenant is there. Every one of these is the same 401.
func TestAdminAuthenticatesBeforeRouting(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	for _, tc := range []struct {
		name    string
		method  string
		path    string
		body    string
		headers map[string]string
	}{
		{
			name:   "a real route, no credential",
			method: http.MethodPost,
			path:   "/admin/v1/tenants",
			body:   `{"id":"tenant-x","slug":"tenant-x","name":"X"}`,
		},
		{
			name:   "a real resource that exists",
			method: http.MethodGet,
			path:   "/admin/v1/tenants/tenant-a",
		},
		{
			name:   "a tenant that does not exist",
			method: http.MethodGet,
			path:   "/admin/v1/tenants/tenant-absent",
		},
		{
			name:   "an unknown route",
			method: http.MethodGet,
			path:   "/admin/v1/secrets",
		},
		{
			name:   "the admin prefix itself",
			method: http.MethodGet,
			path:   "/admin",
		},
		{
			name:   "a path below the prefix",
			method: http.MethodGet,
			path:   "/admin/",
		},
		{
			name:   "a deep unknown path",
			method: http.MethodDelete,
			path:   "/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/rollback",
		},
		{
			name:   "the wrong method on a real route",
			method: http.MethodGet,
			path:   "/admin/v1/tenants",
		},
		{
			name:   "a method nothing takes",
			method: http.MethodPut,
			path:   "/admin/v1/tenants/tenant-a",
		},
		{
			name:   "a preflight",
			method: http.MethodOptions,
			path:   "/admin/v1/tenants",
		},
		{
			name:    "a malformed authorization header",
			method:  http.MethodGet,
			path:    "/admin/v1/tenants/tenant-a",
			headers: map[string]string{HeaderAuthorization: adminKeyPlatform},
		},
		{
			name:    "a basic credential",
			method:  http.MethodGet,
			path:    "/admin/v1/tenants/tenant-a",
			headers: map[string]string{HeaderAuthorization: "Basic " + adminKeyPlatform},
		},
		{
			name:    "an unknown bearer token",
			method:  http.MethodGet,
			path:    "/admin/v1/tenants/tenant-a",
			headers: adminHeaders(adminKeyUnknown),
		},
		{
			name:   "a chat credential",
			method: http.MethodGet,
			path:   "/admin/v1/tenants/tenant-a",
			// The purposes do not cross. A chat key is a real credential of this
			// process, and it must be worth exactly nothing here.
			headers: adminHeaders(keyTenantA),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, tc.method, tc.path, tc.body, tc.headers,
				http.StatusUnauthorized,
			)
			// A 401 without this header is a 401 a client cannot act on.
			require.Equal(t, "Bearer", response.Header().Get("WWW-Authenticate"))
			// No Allow header: the method is not information this caller is owed.
			require.Empty(t, response.Header().Values("Allow"))
			requireNoCORS(t, response.Header())
		})
	}

	// The converse of the chat case above: an admin credential is worth nothing
	// on the data plane.
	requireStatus(t, platform.handler, http.MethodPost, chatPath, `{
		"model":"ignored","messages":[{"role":"user","content":"hello"}]
	}`, map[string]string{
		HeaderAuthorization: "Bearer " + adminKeyPlatform,
		HeaderAgentAppID:    appAssistant,
	}, http.StatusUnauthorized)

	// And /healthz stays public, because a probe cannot hold a credential.
	requireStatus(t, platform.handler, http.MethodGet, "/healthz", "", nil, http.StatusOK)
}

// oddAdminPaths are paths in the control-plane subtree that change under path
// cleaning. Handed to http.ServeMux they are each answered with a 301 to the
// cleaned form — a response written before any handler runs, and so before
// anything has authenticated. They are the reason the subtree is matched ahead
// of the router.
var oddAdminPaths = []string{
	"/admin//v1/tenants",
	"/admin/./v1/tenants",
	"/admin/v1/tenants/../secrets",
	"/admin/v1/tenants/tenant-a/../tenant-b",
	"/admin/../healthz",
	"/admin/v1/tenants/../../healthz",
	"/admin//",
	"/admin/.",
}

// A path that only looks like a route is still the control plane, and the
// control plane authenticates first.
//
// Without the subtree being taken ahead of the router, every path here was a
// 301 with a Location naming the cleaned path — the router answering a question
// about /admin to a caller holding no credential. The claim being tested is not
// that the redirect was dangerous in itself but that the boundary was not first:
// once the router may answer, what it answers is no longer this package's
// decision.
func TestAdminOddPathsAreRefusedBeforeTheRouterAnswers(t *testing.T) {
	platform := newPlatformTestServer(t)

	// The answer a real admin route gives the same caller. Every odd path has to
	// match it byte for byte: an unauthenticated caller must not be able to tell
	// the two apart, which is the whole of what "authentication before routing"
	// buys.
	reference := requireStatus(
		t, platform.handler, http.MethodGet, "/admin/v1/tenants/tenant-a", "", nil,
		http.StatusUnauthorized,
	)

	for _, path := range oddAdminPaths {
		t.Run(path, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, http.MethodGet, path, "", nil,
				http.StatusUnauthorized,
			)
			require.Equal(t, reference.Body.String(), response.Body.String())
			require.Equal(t, "Bearer", response.Header().Get("WWW-Authenticate"))
			// Nothing to follow: a Location here would be the router's answer
			// wearing this one's status.
			require.Empty(t, response.Header().Values("Location"))
			require.Empty(t, response.Header().Values("Allow"))
			requireNoCORS(t, response.Header())
		})
	}
}

// The same paths with a real credential are answered, never redirected.
//
// They are 404s, and that is the point rather than an accident: handleAdmin
// compares the raw path exactly and resolves no traversal, so a path that spells
// a real route oddly is not that route. A 301 here would instead be the platform
// telling a client that /admin/v1/tenants/../secrets means /admin/v1/secrets,
// which is a normalization this boundary must never perform on a caller's behalf.
func TestAdminOddPathsAreNotRedirectedForAValidCredential(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	for _, path := range oddAdminPaths {
		for _, credential := range []struct {
			name string
			key  string
		}{
			{name: "platform admin", key: adminKeyPlatform},
			{name: "tenant admin", key: adminKeyTenantA},
		} {
			t.Run(credential.name+" "+path, func(t *testing.T) {
				response := requireStatus(
					t, platform.handler, http.MethodGet, path, "",
					adminHeaders(credential.key), http.StatusNotFound,
				)
				require.Empty(t, response.Header().Values("Location"))
				requireNoCORS(t, response.Header())
			})
		}
	}

	// And the real route is unaffected: taking the subtree early changed which
	// paths reach handleAdmin, not what it does with the ones that always did.
	requireStatus(t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a", "", adminHeaders(adminKeyPlatform), http.StatusOK)
}

// Traffic that is not the control plane still goes through the router, path
// cleaning and all.
//
// A path outside /admin that cleans into it — /foo/../admin/v1/tenants — is
// still the router's 301, and that is left alone deliberately. The redirect is
// syntactic: http.ServeMux issues it whenever cleaning changes the path, for
// paths it has never heard of just as readily, so it discloses nothing about
// what exists. The client follows it to /admin/v1/tenants and meets the
// boundary there, which is where the answer about /admin belongs.
func TestNonAdminTrafficStillRoutesThroughTheMux(t *testing.T) {
	platform := newPlatformTestServer(t)

	requireStatus(t, platform.handler, http.MethodGet, "/healthz", "", nil, http.StatusOK)

	redirected := requireStatus(
		t, platform.handler, http.MethodGet, "/foo/../admin/v1/tenants", "", nil,
		http.StatusMovedPermanently,
	)
	// It points at the boundary rather than through it: following this lands on
	// the 401 above, and no admin handler ran to produce the redirect.
	require.Equal(t, "/admin/v1/tenants", redirected.Header().Get("Location"))
	requireStatus(
		t, platform.handler, http.MethodGet, redirected.Header().Get("Location"), "", nil,
		http.StatusUnauthorized,
	)

	// An unknown path outside the subtree is the router's 404, not a 401: this
	// platform does not claim paths it does not serve.
	requireStatus(t, platform.handler, http.MethodGet, "/nope", "", nil, http.StatusNotFound)
}

// Why identity refuses a key that cannot be carried, demonstrated rather than
// asserted.
//
// The rule lives in the identity package, where keys are configured; the reason
// lives here, where they arrive. Two different things go wrong and both end in
// a credential that authenticates nobody, so a process holding one starts
// cleanly, reports the credential it was given, and then answers 401 to the
// operator who holds it.
//
// A key with whitespace at an end is sent and arrives as a different string:
// the client trims the header value on the way out and bearerToken trims it
// again on the way in, and the digest that is finally looked up is not the
// digest of the configured key. A key holding a control character is not sent
// at all — net/http will not write the header — so it never reaches a
// comparison in the first place.
func TestKeysThatCannotBeCarriedCouldNeverAuthenticate(t *testing.T) {
	// The whole server is the platform's own header parsing: what bearerToken
	// makes of the Authorization header is the only string an authenticator ever
	// gets to compare against its key set.
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearerToken(r.Header.Get(HeaderAuthorization))
			if !ok {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, token)
		}))
	defer server.Close()

	presented := func(t *testing.T, key string) (string, error) {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		request.Header.Set(HeaderAuthorization, "Bearer "+key)
		response, err := server.Client().Do(request)
		if err != nil {
			return "", err
		}
		defer func() { require.NoError(t, response.Body.Close()) }()
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		return string(body), nil
	}

	// The control: an ordinary key arrives exactly as configured, so everything
	// below is a fact about those keys and not about this harness.
	t.Run("an ordinary key", func(t *testing.T) {
		token, err := presented(t, adminKeyPlatform)
		require.NoError(t, err)
		require.Equal(t, adminKeyPlatform, token)
	})

	// Sent, and not what was configured by the time it is compared.
	for name, key := range map[string]string{
		"nothing but spaces": strings.Repeat(" ", 40),
		"nothing but tabs":   strings.Repeat("\t", 40),
		"a leading space":    " " + adminKeyPlatform,
		"a trailing space":   adminKeyPlatform + " ",
		"a leading tab":      "\t" + adminKeyPlatform,
		"a trailing tab":     adminKeyPlatform + "\t",
	} {
		t.Run(name, func(t *testing.T) {
			token, err := presented(t, key)
			require.NoError(t, err)
			// An empty token is the request that carried no usable credential at
			// all; a non-empty one is the trimmed key, which matches no digest.
			require.NotEqual(t, key, token)
		})
	}

	// Not sent at all.
	for name, key := range map[string]string{
		"an embedded newline": adminKeyPlatform + "\nx",
		"an embedded return":  adminKeyPlatform + "\rx",
		"an embedded NUL":     adminKeyPlatform + "\x00x",
		"an embedded DEL":     adminKeyPlatform + "\x7fx",
		"a vertical tab":      adminKeyPlatform + "\vx",
		"a form feed":         adminKeyPlatform + "\fx",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := presented(t, key)
			// Named rather than left as any error: a bare require.Error would also
			// be satisfied by a closed port or a crashed handler, and the claim
			// being made is the specific one that the client would not write the
			// header. If a future Go sanitizes instead of refusing, this fails and
			// the reasoning gets re-checked, which is the point.
			require.ErrorContains(t, err, "invalid header field value")
		})
	}

	// And the configuration that would hold any of them is refused before the
	// process starts, which is the whole point of knowing the above.
	for _, key := range []string{
		strings.Repeat(" ", 40),
		" " + adminKeyPlatform,
		adminKeyPlatform + "\n",
	} {
		_, err := identity.NewStaticAdminAPIKeyAuthenticator(
			map[string]identity.AdminIdentity{key: {
				Role:        identity.RolePlatformAdmin,
				PrincipalID: principalPlatform,
			}})
		require.ErrorContains(t, err, "cannot be sent as a Bearer credential")
	}
}

// No admin response carries a CORS header, on any path, for any outcome.
//
// This is what makes the control plane unreachable from a browser page on
// another origin: without Access-Control-Allow-Origin the page cannot read a
// response, and without a preflight to succeed against it cannot send a
// non-simple request at all. There is no OPTIONS branch under /admin for the
// same reason.
func TestAdminNeverPublishesCORSHeaders(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")

	for _, tc := range []struct {
		name    string
		method  string
		path    string
		body    string
		headers map[string]string
	}{
		{
			name: "unauthenticated", method: http.MethodGet,
			path: "/admin/v1/tenants/tenant-a",
		},
		{
			name: "preflight", method: http.MethodOptions, path: "/admin/v1/tenants",
			headers: map[string]string{
				"Origin":                        "https://evil.example",
				"Access-Control-Request-Method": http.MethodPost,
			},
		},
		{
			name: "authenticated preflight", method: http.MethodOptions,
			path: "/admin/v1/tenants", headers: adminHeaders(adminKeyPlatform),
		},
		{
			name: "successful read", method: http.MethodGet,
			path: "/admin/v1/tenants/tenant-a", headers: adminHeaders(adminKeyPlatform),
		},
		{
			name: "successful write", method: http.MethodPost, path: "/admin/v1/tenants",
			body:    `{"id":"tenant-z","slug":"tenant-z","name":"Z"}`,
			headers: adminHeaders(adminKeyPlatform),
		},
		{
			name: "cross-tenant refusal", method: http.MethodGet,
			path: "/admin/v1/tenants/tenant-a", headers: adminHeaders(adminKeyTenantB),
		},
		{
			name: "unknown route", method: http.MethodGet, path: "/admin/v1/nope",
			headers: adminHeaders(adminKeyPlatform),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := cloneHeaders(tc.headers)
			headers["Origin"] = "https://evil.example"
			response := serve(platform.handler, tc.method, tc.path, tc.body, headers)
			requireNoCORS(t, response.Header())
		})
	}

	// An OPTIONS request is not special-cased: with a valid credential it is
	// simply a method these routes do not take.
	response := requireStatus(
		t, platform.handler, http.MethodOptions, "/admin/v1/tenants", "",
		adminHeaders(adminKeyPlatform), http.StatusMethodNotAllowed,
	)
	require.Equal(t, http.MethodPost, response.Header().Get("Allow"))
}

// An admin write must declare application/json, which puts it outside the set
// of requests a browser sends without asking first. The three media types below
// are exactly that set.
func TestAdminWritesRequireJSONContentType(t *testing.T) {
	platform := newPlatformTestServer(t)
	body := `{"id":"tenant-c","slug":"tenant-c","name":"Tenant C"}`

	for _, contentType := range []string{
		"",
		"text/plain",
		"text/plain;charset=UTF-8",
		"application/x-www-form-urlencoded",
		"multipart/form-data; boundary=x",
		"application/json-patch+json",
		"application/jsonx",
	} {
		t.Run(contentType, func(t *testing.T) {
			headers := map[string]string{
				HeaderAuthorization: "Bearer " + adminKeyPlatform,
				"Content-Type":      contentType,
			}
			response := requireStatus(
				t, platform.handler, http.MethodPost, "/admin/v1/tenants", body, headers,
				http.StatusUnsupportedMediaType,
			)
			require.Contains(t, response.Body.String(), "unsupported_media_type")
		})
	}

	// A charset parameter is fine: the media type is what matters, and so is
	// case-insensitivity, both of which are HTTP's rules rather than ours.
	for index, contentType := range []string{
		"application/json",
		"application/json; charset=utf-8",
		"Application/JSON",
	} {
		requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
			fmt.Sprintf(`{"id":"t-%d","slug":"t-%d","name":"T"}`, index, index),
			map[string]string{
				HeaderAuthorization: "Bearer " + adminKeyPlatform,
				"Content-Type":      contentType,
			}, http.StatusCreated)
	}

	// A read needs no content type: there is no body to declare, and a GET is a
	// simple request no matter what this API does.
	seedTenantAppRevision(t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	requireStatus(t, platform.handler, http.MethodGet, "/admin/v1/tenants/tenant-a", "",
		map[string]string{HeaderAuthorization: "Bearer " + adminKeyPlatform}, http.StatusOK)
}

// Authorship is the credential's, not the body's.
//
// The field is absent from the request type rather than accepted and
// overwritten, so a body that still carries it is refused outright. A client
// sending created_by believes something about the record it is creating, and
// silently storing something else would leave that belief in place.
func TestAdminRevisionAuthorshipComesFromTheCredential(t *testing.T) {
	platform := newPlatformTestServer(t)
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		`{"id":"tenant-a","slug":"tenant-a","name":"Tenant A"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants/tenant-a/apps",
		`{"id":"assistant","name":"Assistant"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)

	const config = `"config":{"agent_name":"test-agent","instruction":"Answer.",
		"model":{"provider":"deterministic","name":"echo-v1"}}`

	rejected := requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		`{"id":"revision-1","revision_no":1,"created_by":"somebody-else",`+config+`}`,
		adminHeaders(adminKeyPlatform), http.StatusBadRequest,
	)
	require.Contains(t, rejected.Body.String(), "invalid_json")

	// The same body without the field is accepted, and the stored author is the
	// principal of the credential that sent it.
	requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		`{"id":"revision-1","revision_no":1,`+config+`}`,
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
	stored, err := platform.repository.GetRevision(
		context.Background(), tenant.TenantContext{TenantID: "tenant-a"},
		"assistant", "revision-1",
	)
	require.NoError(t, err)
	require.Equal(t, principalAdminA, stored.CreatedBy)

	// A different credential authors a different revision, so the value tracks
	// the caller rather than being a constant that happens to match.
	requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		`{"id":"revision-2","revision_no":2,`+config+`}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated,
	)
	stored, err = platform.repository.GetRevision(
		context.Background(), tenant.TenantContext{TenantID: "tenant-a"},
		"assistant", "revision-2",
	)
	require.NoError(t, err)
	require.Equal(t, principalPlatform, stored.CreatedBy)
}

// Entitlement is checked at create, before the write, and the refusal is the
// same answer whatever the reference happens to be.
//
// The four cases below differ in the two ways a caller could hope to learn
// something: whether the environment variable exists, and whether the policy is
// one this binary has. All four must be indistinguishable, or this endpoint
// becomes a probe for the process environment and the tool registry of a
// platform the caller does not administer.
func TestAdminRefusesUnentitledRevisionsIdentically(t *testing.T) {
	t.Setenv("TEST_PRESENT_MODEL_KEY", "a-value-that-must-not-be-reachable")

	platform := newPlatformTestServerWith(t, platformTestOptions{
		// tenant-a is entitled to exactly one secret reference and to no policy
		// at all; nothing in the table below names it.
		revisions: mustEntitle(t, security.Grant{
			TenantID:   "tenant-a",
			SecretRefs: []string{"env:TEST_ENTITLED_MODEL_KEY"},
		}),
	})
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		`{"id":"tenant-a","slug":"tenant-a","name":"Tenant A"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants/tenant-a/apps",
		`{"id":"assistant","name":"Assistant"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)

	bodies := map[string]string{
		"a secret whose variable is set": `"model":{"provider":"deterministic",
			"name":"echo-v1","secret_ref":"env:TEST_PRESENT_MODEL_KEY"}`,
		"a secret whose variable is not set": `"model":{"provider":"deterministic",
			"name":"echo-v1","secret_ref":"env:TEST_ABSENT_MODEL_KEY"}`,
		"a policy this binary has": `"model":{"provider":"deterministic","name":"echo-v1"},
			"policy_refs":["` + tool.PolicySafeTools + `"]`,
		"a policy that does not exist": `"model":{"provider":"deterministic","name":"echo-v1"},
			"policy_refs":["builtin.no-such-policy"]`,
		"a secret that is not even a reference": `"model":{"provider":"deterministic",
			"name":"echo-v1","secret_ref":"not-a-reference"}`,
	}

	var answers []string
	index := 0
	for name, fragment := range bodies {
		index++
		t.Run(name, func(t *testing.T) {
			body := fmt.Sprintf(
				`{"id":"revision-%d","revision_no":%d,"config":{
					"agent_name":"test-agent","instruction":"Answer.",%s}}`,
				index, index, fragment)
			response := requireStatus(
				t, platform.handler, http.MethodPost,
				"/admin/v1/tenants/tenant-a/apps/assistant/revisions", body,
				adminHeaders(adminKeyTenantA), http.StatusForbidden,
			)
			// The same literal the profile gate is checked against, so the two
			// halves of the control plane cannot answer this differently.
			require.JSONEq(t, notEntitledBody, response.Body.String())
			// Nothing about the rejected reference reaches the caller.
			require.NotContains(t, response.Body.String(), "TEST_")
			require.NotContains(t, response.Body.String(), "policy")
			answers = append(answers, response.Body.String())
		})
	}
	// Byte-identical to each other, not merely all 403.
	for _, answer := range answers {
		require.Equal(t, answers[0], answer)
	}

	// And nothing was written: an un-entitled revision must not exist as a draft
	// either, or the refusal would only be a refusal to publish.
	for i := 1; i <= len(bodies); i++ {
		requireStatus(t, platform.handler, http.MethodGet,
			fmt.Sprintf("/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-%d", i),
			"", adminHeaders(adminKeyTenantA), http.StatusNotFound)
	}

	// The entitled reference is accepted, so the refusals above are about the
	// grant and not about the fields. Note what this means for the third case:
	// builtin.safe-tools is a policy this binary really has, and it is refused
	// here for exactly the same reason an invented one is — the tenant does not
	// hold it. That is the property. The registry is never consulted, so it
	// cannot be enumerated.
	requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions", `{
			"id":"revision-ok","revision_no":100,"config":{
				"agent_name":"test-agent","instruction":"Answer.",
				"model":{"provider":"deterministic","name":"echo-v1",
					"secret_ref":"env:TEST_ENTITLED_MODEL_KEY"}}}`,
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
}

// Entitlement is checked again at publish, on the stored revision rather than on
// anything the request says.
//
// The check cannot be skipped by publishing something that was entitled when it
// was created: the grant is process configuration, the revision is data, and the
// two are compared every time the revision is about to become servable.
func TestAdminPublishRechecksEntitlement(t *testing.T) {
	entitled := mustEntitle(t, security.Grant{
		TenantID:   "tenant-a",
		PolicyRefs: []string{tool.PolicySafeTools},
	})
	platform := newPlatformTestServerWith(t, platformTestOptions{revisions: entitled})
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		`{"id":"tenant-a","slug":"tenant-a","name":"Tenant A"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants/tenant-a/apps",
		`{"id":"assistant","name":"Assistant"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	requireStatus(t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions", `{
			"id":"revision-1","revision_no":1,"config":{
				"agent_name":"test-agent","instruction":"Answer.",
				"policy_refs":["`+tool.PolicySafeTools+`"],
				"model":{"provider":"deterministic","name":"echo-v1"}}}`,
		adminHeaders(adminKeyTenantA), http.StatusCreated)

	// Same stored revision, a platform whose configuration no longer entitles
	// it. This is the restart-with-a-changed-manifest case, and publish has to
	// refuse rather than trust that create checked.
	stored, err := platform.repository.GetRevision(
		context.Background(), tenant.TenantContext{TenantID: "tenant-a"},
		"assistant", "revision-1")
	require.NoError(t, err)
	require.ErrorIs(t,
		security.DenyCapabilities().AuthorizeRevision("tenant-a", stored.Config),
		security.ErrNotEntitled)

	restarted := newPlatformTestServerWith(t, platformTestOptions{
		revisions: security.DenyCapabilities(),
	})
	requireStatus(t, restarted.handler, http.MethodPost, "/admin/v1/tenants",
		`{"id":"tenant-a","slug":"tenant-a","name":"Tenant A"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	requireStatus(t, restarted.handler, http.MethodPost, "/admin/v1/tenants/tenant-a/apps",
		`{"id":"assistant","name":"Assistant"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	// Written straight to the repository, because the Admin API would refuse to
	// create it — which is the point being isolated: publish does its own check.
	_, err = restarted.repository.CreateRevision(
		context.Background(), tenant.TenantContext{TenantID: "tenant-a"}, stored)
	require.NoError(t, err)

	refused := requireStatus(t, restarted.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish",
		"", adminHeaders(adminKeyTenantA), http.StatusForbidden)
	require.Contains(t, refused.Body.String(), "not_entitled")

	// Under the original entitlement the same publish succeeds.
	requireStatus(t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish",
		"", adminHeaders(adminKeyTenantA), http.StatusOK)
}

// Entitlement does not replace tool authorization. A tenant entitled to a policy
// can still write a revision the registry refuses, and that refusal is a
// different, explicit answer — by then nothing is being disclosed to someone who
// should not know it.
func TestAdminPublishStillValidatesTheToolRegistry(t *testing.T) {
	platform := newPlatformTestServerWith(t, platformTestOptions{
		revisions: mustEntitle(t, security.Grant{
			TenantID:   "tenant-a",
			PolicyRefs: []string{tool.PolicySafeTools},
		}),
	})
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants",
		`{"id":"tenant-a","slug":"tenant-a","name":"Tenant A"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	requireStatus(t, platform.handler, http.MethodPost, "/admin/v1/tenants/tenant-a/apps",
		`{"id":"assistant","name":"Assistant"}`,
		adminHeaders(adminKeyPlatform), http.StatusCreated)
	// An unknown tool, under a policy the tenant genuinely holds.
	requireStatus(t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions", `{
			"id":"revision-1","revision_no":1,"config":{
				"agent_name":"test-agent","instruction":"Answer.",
				"tool_refs":["builtin_nonexistent"],
				"policy_refs":["`+tool.PolicySafeTools+`"],
				"model":{"provider":"deterministic","name":"echo-v1"}}}`,
		adminHeaders(adminKeyTenantA), http.StatusCreated)

	refused := requireStatus(t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish",
		"", adminHeaders(adminKeyTenantA), http.StatusBadRequest)
	require.Contains(t, refused.Body.String(), "invalid_revision")
}

// requireNoCORS asserts that a response carries nothing a browser could use to
// let another origin read it.
func requireNoCORS(t *testing.T, header http.Header) {
	t.Helper()
	for name := range header {
		require.NotContains(t, name, "Access-Control",
			"an admin response carried a CORS header")
	}
}

// mustEntitle builds an entitlement table through the same constructor the
// process uses, so a fixture cannot express a grant the manifest could not.
func mustEntitle(t *testing.T, grants ...security.Grant) security.CapabilityAuthorizer {
	t.Helper()
	entitlements, err := security.NewEntitlements(grants...)
	require.NoError(t, err)
	return entitlements
}
