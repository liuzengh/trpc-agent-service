package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// This file is the backend-profile half of the control plane's attack surface.
//
// A backend profile is the one control-plane object that names a credential
// outside a revision, so the properties it has to hold are the ones
// admin_test.go establishes for revisions — authentication before routing,
// tenant scope before the repository, a refusal that is never an oracle — plus
// one of its own: the entitlement is checked before anything is stored, and
// again at every gate a revision naming the profile passes through.

const (
	profileTenantA = "/admin/v1/tenants/tenant-a/backend-profiles"
	profileTenantB = "/admin/v1/tenants/tenant-b/backend-profiles"
	tenantADSNRef  = "env:TENANT_A_SESSION_DSN"

	postgresProfileBody = `{"id":%q,"session":{"backend":"postgres","postgres":{"dsn_ref":%q}}}`
	inMemoryProfileBody = `{"id":%q,"session":{"backend":"inmemory"}}`
)

// notEntitledBody is the one answer an un-entitled caller gets, whichever gate
// refused it and whichever object it named. Like unavailableRevisionBody it is a
// literal rather than something built from the handler: the point of the answer
// is that it never varies, and a change to the wording has to be made here too.
//
// It lives in this file and is used by admin_test.go as well, so the revision
// gates and the profile gate are pinned to one literal rather than to two that
// could drift apart — see TestAdminNotEntitledSaysNothingAboutWhatWasRefused for
// what the drift would cost.
const notEntitledBody = `{"error":{
	"code":"not_entitled",
	"message":"this tenant is not entitled to a capability the configuration references"
}}`

// failingProfiles fails the test if anything reaches it.
//
// It is the profile-shaped twin of failingRepository, and it exists for the same
// reason: a 404 is what a cross-tenant read must answer, but it is also what a
// working repository answers for a profile that is simply not there, so a status
// assertion alone cannot tell a short-circuit from a query made on another
// tenant's behalf.
type failingProfiles struct {
	t *testing.T
}

func (p *failingProfiles) fail(method string) {
	p.t.Helper()
	p.t.Errorf("the profile repository was reached: %s", method)
}

func (p *failingProfiles) CreateProfile(
	context.Context, tenant.TenantContext, storagebundle.Profile, string,
) (storagebundle.ProfileRecord, error) {
	p.fail("CreateProfile")
	return storagebundle.ProfileRecord{}, storagebundle.ErrProfileNotFound
}

func (p *failingProfiles) GetProfile(
	context.Context, tenant.TenantContext, string,
) (storagebundle.ProfileRecord, error) {
	p.fail("GetProfile")
	return storagebundle.ProfileRecord{}, storagebundle.ErrProfileNotFound
}

func (p *failingProfiles) ListProfiles(
	context.Context, tenant.TenantContext,
) ([]storagebundle.ProfileRecord, error) {
	p.fail("ListProfiles")
	return nil, storagebundle.ErrProfileNotFound
}

func (p *failingProfiles) ResolveProfile(
	context.Context, tenant.TenantContext, string,
) (storagebundle.Profile, error) {
	p.fail("ResolveProfile")
	return storagebundle.Profile{}, storagebundle.ErrProfileNotFound
}

// stubProfiles answers every call with one chosen error.
//
// It is how the mapping table below reaches branches a working repository cannot
// be talked into: a row that no longer matches its own fingerprint, a pool that
// is down. Those are the failures whose mapping matters most, because each of
// them decides whether a driver's own words end up in an HTTP body.
type stubProfiles struct {
	err error
}

func (p stubProfiles) CreateProfile(
	context.Context, tenant.TenantContext, storagebundle.Profile, string,
) (storagebundle.ProfileRecord, error) {
	return storagebundle.ProfileRecord{}, p.err
}

func (p stubProfiles) GetProfile(
	context.Context, tenant.TenantContext, string,
) (storagebundle.ProfileRecord, error) {
	return storagebundle.ProfileRecord{}, p.err
}

func (p stubProfiles) ListProfiles(
	context.Context, tenant.TenantContext,
) ([]storagebundle.ProfileRecord, error) {
	return nil, p.err
}

func (p stubProfiles) ResolveProfile(
	context.Context, tenant.TenantContext, string,
) (storagebundle.Profile, error) {
	return storagebundle.Profile{}, p.err
}

// A tenant admin creates, reads and lists its own profiles, and what comes back
// is what was stored — including the three fields the request could not state.
func TestAdminBackendProfileRoundTrip(t *testing.T) {
	platform := newPlatformTestServerWith(t, platformTestOptions{
		revisions: mustEntitle(t, security.Grant{
			TenantID:   "tenant-a",
			SecretRefs: []string{tenantADSNRef},
		}),
	})
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	created := requireStatus(
		t, platform.handler, http.MethodPost, profileTenantA,
		fmt.Sprintf(postgresProfileBody, "primary", tenantADSNRef),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
	require.Equal(
		t, "/admin/v1/tenants/tenant-a/backend-profiles/primary",
		created.Header().Get("Location"))

	record := decodeProfile(t, created)
	require.Equal(t, "tenant-a", record.TenantID, "the tenant is the path's")
	require.Equal(t, "primary", record.ID)
	require.Equal(t, tenantADSNRef, record.Session.Postgres.DSNRef)
	// Authorship is the credential's principal, not anything the body said — the
	// body could not say it, and this is the value that proves it did not have to.
	require.Equal(t, principalAdminA, record.CreatedBy)
	require.False(t, record.CreatedAt.IsZero())
	// Derived from the content, and it is the content's own fingerprint rather
	// than whatever happened to be stored beside it.
	fingerprint, err := record.Profile.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, fingerprint, record.Fingerprint)

	fetched := requireStatus(
		t, platform.handler, http.MethodGet, profileTenantA+"/primary", "",
		adminHeaders(adminKeyTenantA), http.StatusOK,
	)
	require.Equal(t, record, decodeProfile(t, fetched))

	// A second profile, so the list order is a real ordering rather than one
	// element that cannot be out of place.
	requireStatus(
		t, platform.handler, http.MethodPost, profileTenantA,
		fmt.Sprintf(inMemoryProfileBody, "ephemeral"),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
	listed := decodeProfileList(t, requireStatus(
		t, platform.handler, http.MethodGet, profileTenantA, "",
		adminHeaders(adminKeyTenantA), http.StatusOK,
	))
	require.Len(t, listed.Profiles, 2)
	require.Equal(t, "ephemeral", listed.Profiles[0].ID)
	require.Equal(t, "primary", listed.Profiles[1].ID)
}

// The id is the version: a profile is never replaced, and a second create under
// the same id is refused even when the content is identical.
func TestAdminBackendProfileIsImmutable(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	body := fmt.Sprintf(inMemoryProfileBody, "primary")
	requireStatus(t, platform.handler, http.MethodPost, profileTenantA, body,
		adminHeaders(adminKeyTenantA), http.StatusCreated)
	repeated := requireStatus(t, platform.handler, http.MethodPost, profileTenantA, body,
		adminHeaders(adminKeyTenantA), http.StatusConflict)
	require.Contains(t, repeated.Body.String(), "already_exists")

	// And there is no route that would edit or remove one. Each is a 405 rather
	// than a 404, which is the honest answer: the resource is there, these are
	// not things that may be done to it.
	for _, method := range []string{http.MethodPut, http.MethodPatch, http.MethodDelete} {
		refused := requireStatus(
			t, platform.handler, method, profileTenantA+"/primary", "",
			adminHeaders(adminKeyTenantA), http.StatusMethodNotAllowed,
		)
		require.Equal(t, http.MethodGet, refused.Header().Get("Allow"))
	}
	refused := requireStatus(
		t, platform.handler, http.MethodDelete, profileTenantA, "",
		adminHeaders(adminKeyTenantA), http.StatusMethodNotAllowed,
	)
	require.Equal(t, "GET, POST", refused.Header().Get("Allow"))
}

// Provenance is not something a request may state.
//
// Every field below is stored, and none of them is in
// createBackendProfileRequest — so a body carrying one is refused outright
// rather than accepted and overwritten. A client that sends created_by believes
// something about the record it is creating, and silently storing something else
// would leave that belief in place.
func TestAdminBackendProfileRejectsFieldsItWouldOverwrite(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	for name, body := range map[string]string{
		"created_by":  `{"id":"p1","created_by":"someone-else","session":{"backend":"inmemory"}}`,
		"created_at":  `{"id":"p1","created_at":"2020-01-01T00:00:00Z","session":{"backend":"inmemory"}}`,
		"fingerprint": `{"id":"p1","fingerprint":"deadbeef","session":{"backend":"inmemory"}}`,
		// The tenant is the path's. A body that names one has an opinion about
		// scope, and the scope was already decided by the credential.
		"tenant_id": `{"id":"p1","tenant_id":"tenant-b","session":{"backend":"inmemory"}}`,
		"unknown":   `{"id":"p1","session":{"backend":"inmemory"},"nonsense":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			refused := requireStatus(
				t, platform.handler, http.MethodPost, profileTenantA, body,
				adminHeaders(adminKeyTenantA), http.StatusBadRequest,
			)
			require.Contains(t, refused.Body.String(), "invalid_json")
		})
	}

	// One object per body, so a second one smuggled after the first cannot be the
	// thing that gets stored.
	refused := requireStatus(
		t, platform.handler, http.MethodPost, profileTenantA,
		`{"id":"p1","session":{"backend":"inmemory"}}{"id":"p2","session":{"backend":"inmemory"}}`,
		adminHeaders(adminKeyTenantA), http.StatusBadRequest,
	)
	require.Contains(t, refused.Body.String(), "request body must contain one JSON object")

	// None of the above stored anything.
	listed := decodeProfileList(t, requireStatus(
		t, platform.handler, http.MethodGet, profileTenantA, "",
		adminHeaders(adminKeyTenantA), http.StatusOK,
	))
	require.Empty(t, listed.Profiles)
}

// A tenant admin reaching into another tenant's profiles is refused before the
// repository is asked anything at all.
func TestAdminBackendProfileTenantIsolation(t *testing.T) {
	platform := newPlatformTestServerWith(t, platformTestOptions{
		repository: &failingRepository{t: t},
		profiles:   &failingProfiles{t: t},
	})

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "list", method: http.MethodGet, path: profileTenantB},
		{name: "read", method: http.MethodGet, path: profileTenantB + "/primary"},
		{
			name:   "create",
			method: http.MethodPost,
			path:   profileTenantB,
			body:   fmt.Sprintf(inMemoryProfileBody, "stolen"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, tc.method, tc.path, tc.body,
				adminHeaders(adminKeyTenantA), http.StatusNotFound,
			)
			// Byte-identical to every other not-found this API gives, so the
			// refusal cannot be told apart from a profile that is simply absent.
			require.Equal(t, notFoundBody+"\n", response.Body.String())
		})
	}
}

// Authentication happens before routing here too, so an unauthenticated caller
// learns nothing about whether these routes exist.
func TestAdminBackendProfileAuthenticatesBeforeRouting(t *testing.T) {
	platform := newPlatformTestServerWith(t, platformTestOptions{
		repository: &failingRepository{t: t},
		profiles:   &failingProfiles{t: t},
	})

	for _, tc := range []struct {
		name    string
		method  string
		path    string
		body    string
		headers map[string]string
	}{
		{
			name: "no credential", method: http.MethodGet, path: profileTenantA,
			headers: map[string]string{"Content-Type": adminMediaTypeJSON},
		},
		{
			name: "an unknown credential", method: http.MethodGet, path: profileTenantA + "/primary",
			headers: adminHeaders(adminKeyUnknown),
		},
		// A chat key is a real credential for this very tenant, and it is not an
		// admin one. This is the case that would fail if the profile routes were
		// mounted anywhere but behind the admin authenticator.
		{
			name: "a chat credential", method: http.MethodPost, path: profileTenantA,
			body: fmt.Sprintf(inMemoryProfileBody, "p1"), headers: adminHeaders(keyTenantA),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := requireStatus(
				t, platform.handler, tc.method, tc.path, tc.body,
				tc.headers, http.StatusUnauthorized,
			)
			requireNoCORS(t, response.Header())
		})
	}

	// And a write still has to declare JSON, which is what keeps it outside the
	// set of requests a browser sends without preflighting.
	requireStatus(
		t, platform.handler, http.MethodPost, profileTenantA,
		fmt.Sprintf(inMemoryProfileBody, "p1"),
		map[string]string{
			HeaderAuthorization: "Bearer " + adminKeyTenantA,
			"Content-Type":      "text/plain",
		},
		http.StatusUnsupportedMediaType,
	)
}

// The credentials a profile names go through the same entitlement table, by the
// same exact string, that a model key goes through — and they are checked before
// anything is stored.
//
// Without this a backend profile would be a second, unentitled channel into the
// process environment: one tenant naming another tenant's DSN variable and
// getting a working connection out of it, with no revision check anywhere in the
// path that would see it happen.
func TestAdminBackendProfileRefusesUnentitledSecretRefs(t *testing.T) {
	// Set, so the refusals below cannot be explained away by the variable's
	// absence — and so the answers being identical means something.
	t.Setenv("TENANT_A_PRESENT_DSN", "a-value-that-must-not-be-reachable")

	platform := newPlatformTestServerWith(t, platformTestOptions{
		// Entitled to one reference, and none of the bodies below names it.
		revisions: mustEntitle(t, security.Grant{
			TenantID:   "tenant-a",
			SecretRefs: []string{tenantADSNRef},
		}),
	})
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	answers := map[string]string{}
	for name, body := range map[string]string{
		"a variable that is set": fmt.Sprintf(
			postgresProfileBody, "p1", "env:TENANT_A_PRESENT_DSN"),
		"a variable that is not set": fmt.Sprintf(
			postgresProfileBody, "p2", "env:TENANT_A_ABSENT_DSN"),
		"another tenant's variable": fmt.Sprintf(
			postgresProfileBody, "p3", "env:TENANT_B_SESSION_DSN"),
		"a redis url": `{"id":"p4","session":{"backend":"redis",` +
			`"redis":{"url_ref":"env:TENANT_A_PRESENT_REDIS_URL"}}}`,
	} {
		refused := requireStatus(
			t, platform.handler, http.MethodPost, profileTenantA, body,
			adminHeaders(adminKeyTenantA), http.StatusForbidden,
		)
		require.JSONEq(t, notEntitledBody, refused.Body.String(), name)
		// Nothing about the rejected reference reaches the caller, so this
		// endpoint cannot be turned into a probe for the process environment.
		require.NotContains(t, refused.Body.String(), "TENANT_", name)
		answers[name] = refused.Body.String()
	}

	// The set-variable case and the absent-variable case are the same bytes. If
	// they were not, this endpoint would answer "does this process hold that
	// credential" for any name a tenant cared to try.
	var first string
	for name, answer := range answers {
		if first == "" {
			first = answer
			continue
		}
		require.Equal(t, first, answer, "%s is distinguishable from the others", name)
	}

	// Nothing was written: an un-entitled profile must not exist as a draft
	// either, or the refusal would only be a refusal to use it.
	listed := decodeProfileList(t, requireStatus(
		t, platform.handler, http.MethodGet, profileTenantA, "",
		adminHeaders(adminKeyTenantA), http.StatusOK,
	))
	require.Empty(t, listed.Profiles)

	// The entitled reference is accepted, so the refusals above are about the
	// grant and not about the shape of the request.
	requireStatus(
		t, platform.handler, http.MethodPost, profileTenantA,
		fmt.Sprintf(postgresProfileBody, "primary", tenantADSNRef),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
}

// The refusal describes neither what was refused nor what kind of object asked.
//
// Two things are being pinned. The first is the old one: no reference, no
// variable name, no policy name reaches the caller. The second is what sharing
// one writer across the revision gates and the profile gate now costs if it is
// got wrong — a message that says "the revision references" is a claim about the
// request, and on POST backend-profiles it is a false one. There is no revision
// in that request, and an answer that says there is tells a tenant admin their
// profile was rejected on behalf of something they did not send.
//
// So the three refusals below are asserted byte-identical rather than merely all
// 403 with the same code. Identical is the property: a caller must not be able
// to tell which gate refused, and the operator reading the message must not be
// sent looking for a revision that never existed.
func TestAdminNotEntitledSaysNothingAboutWhatWasRefused(t *testing.T) {
	// Set, so a refusal cannot be explained away by the variable's absence.
	t.Setenv("TENANT_A_PRESENT_DSN", "a-value-that-must-not-be-reachable")

	// Entitled to nothing at all, so every gate below refuses for the same
	// reason and any difference in the answers is the wording.
	platform := newPlatformTestServerWith(t, platformTestOptions{
		revisions: security.DenyCapabilities(),
	})
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	revisions := "/admin/v1/tenants/tenant-a/apps/assistant/revisions"
	answers := map[string]string{}
	for name, request := range map[string]struct {
		path string
		body string
	}{
		"a profile naming a dsn": {
			path: profileTenantA,
			body: fmt.Sprintf(postgresProfileBody, "p1", "env:TENANT_A_PRESENT_DSN"),
		},
		"a revision naming a model key": {
			path: revisions,
			body: `{"id":"revision-1","revision_no":1,"config":{
				"agent_name":"test-agent","instruction":"Answer.",
				"model":{"provider":"deterministic","name":"echo-v1",
					"secret_ref":"env:TENANT_A_PRESENT_DSN"}}}`,
		},
		"a revision naming a policy": {
			path: revisions,
			body: `{"id":"revision-2","revision_no":2,"config":{
				"agent_name":"test-agent","instruction":"Answer.",
				"model":{"provider":"deterministic","name":"echo-v1"},
				"policy_refs":["` + tool.PolicySafeTools + `"]}}`,
		},
	} {
		refused := requireStatus(
			t, platform.handler, http.MethodPost, request.path, request.body,
			adminHeaders(adminKeyTenantA), http.StatusForbidden,
		)
		require.JSONEq(t, notEntitledBody, refused.Body.String(), name)
		answers[name] = refused.Body.String()

		// Not the reference, and not the kind of object either. "revision" is
		// the one that regressed: it was in the shared message while only
		// revisions could reach it.
		for _, absent := range []string{
			"revision", "profile", "policy", "secret", "TENANT_", "env:",
		} {
			require.NotContains(t, refused.Body.String(), absent, "%s leaks %q", name, absent)
		}
	}

	var first, firstName string
	for name, answer := range answers {
		if firstName == "" {
			first, firstName = answer, name
			continue
		}
		require.Equal(t, first, answer, "%s answers differently from %s", name, firstName)
	}
}

// A profile whose shape could never be built is refused with the reason, and the
// reason is the caller's own submission read back to them.
//
// The shape check runs before the entitlement one, so these are the messages
// that reach a caller holding no grant at all — which is why each of them has to
// be a statement about a field and a rule rather than about a value.
func TestAdminBackendProfileRefusesMalformedProfiles(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	for name, tc := range map[string]struct {
		body string
		want string
	}{
		"no backend": {
			body: `{"id":"p1","session":{}}`,
			want: "session backend is required",
		},
		"an unknown backend": {
			body: `{"id":"p1","session":{"backend":"cassandra"}}`,
			want: "unknown session backend",
		},
		"settings for a backend it does not name": {
			body: `{"id":"p1","session":{"backend":"inmemory",` +
				`"postgres":{"dsn_ref":"env:TENANT_A_SESSION_DSN"}}}`,
			want: "takes no settings",
		},
		"a backend without the settings it needs": {
			body: `{"id":"p1","session":{"backend":"postgres"}}`,
			want: "requires postgres settings",
		},
		"a dsn where its name belonged": {
			body: `{"id":"p1","session":{"backend":"postgres",` +
				`"postgres":{"dsn_ref":"postgres://user:hunter2@db.internal:5432/sessions"}}}`,
			want: "dsn_ref",
		},
		"a malformed id": {
			body: `{"id":"../tenant-b","session":{"backend":"inmemory"}}`,
			want: "backend profile id",
		},
		// Both namespaces are individually fine and cannot be used together:
		// upstream spends them in one index name and PostgreSQL truncates it.
		// The caller has to hear that now, because the alternative is a 201 for
		// a profile that is immutable, undeletable and unbuildable.
		"namespaces that cannot both fit": {
			body: `{"id":"p1","session":{"backend":"postgres","postgres":{` +
				`"dsn_ref":"env:TENANT_A_SESSION_DSN",` +
				`"schema":"tenant_a_sessions_production","table_prefix":"conversations"}}}`,
			want: "too long together",
		},
	} {
		t.Run(name, func(t *testing.T) {
			refused := requireStatus(
				t, platform.handler, http.MethodPost, profileTenantA, tc.body,
				adminHeaders(adminKeyTenantA), http.StatusBadRequest,
			)
			require.Contains(t, refused.Body.String(), "invalid_argument")
			require.Contains(t, refused.Body.String(), tc.want)
			// The rejected value never comes back. The connection-string case is
			// why: the likeliest way to get an invalid reference here is a
			// credential pasted where its name belonged.
			require.NotContains(t, refused.Body.String(), "hunter2")
			require.NotContains(t, refused.Body.String(), "../tenant-b")
		})
	}
}

// writeProfileError is a separate mapper from writeDomainError, and this is the
// table that says what it maps to.
//
// The two legitimately disagree about the same sentinels. To a chat client an
// absent profile means a published revision points at storage this process
// cannot give it — a 409 that names nothing. To an admin asking its own tenant's
// collection for an id, the same sentinel is an ordinary missing resource, and
// the honest answer is the 404 a missing tenant or app already gets. Sharing one
// mapper would force one of the two to be wrong.
func TestWriteProfileErrorMapping(t *testing.T) {
	for name, tc := range map[string]struct {
		err        error
		status     int
		wantCode   string
		wantAbsent string
	}{
		"missing": {
			err:      fmt.Errorf("%w: %q", storagebundle.ErrProfileNotFound, "p1"),
			status:   http.StatusNotFound,
			wantCode: "not_found",
		},
		"malformed": {
			err:      fmt.Errorf("%w: session backend is required", storagebundle.ErrInvalidProfile),
			status:   http.StatusBadRequest,
			wantCode: "invalid_argument",
		},
		"the budget is spent": {
			err: fmt.Errorf("%w: tenant %q already owns 32",
				storagebundle.ErrProfileLimit, "tenant-a"),
			status:   http.StatusConflict,
			wantCode: "profile_limit",
		},
		"not entitled": {
			err:      security.ErrNotEntitled,
			status:   http.StatusForbidden,
			wantCode: "not_entitled",
		},
		"a bad argument": {
			err:      fmt.Errorf("%w: tenant id is required", tenant.ErrInvalidArgument),
			status:   http.StatusBadRequest,
			wantCode: "invalid_argument",
		},
		"another tenant's": {
			err:      fmt.Errorf("%w: belongs to another tenant", tenant.ErrTenantScope),
			status:   http.StatusNotFound,
			wantCode: "not_found",
		},
		"a tenant that is not there": {
			err:      fmt.Errorf("%w: tenant %q", tenant.ErrNotFound, "tenant-a"),
			status:   http.StatusNotFound,
			wantCode: "not_found",
		},
		"already taken": {
			err:      fmt.Errorf("%w: backend profile %q", tenant.ErrAlreadyExists, "p1"),
			status:   http.StatusConflict,
			wantCode: "already_exists",
		},
		"a tenant that is switched off": {
			err:      fmt.Errorf("%w: tenant %q", tenant.ErrTenantInactive, "tenant-a"),
			status:   http.StatusForbidden,
			wantCode: "tenant_inactive",
		},
		// A row that no longer matches its own fingerprint is this platform's
		// fault, not the caller's: the request was well formed and no edit to it
		// would help. It is a 500, and it says nothing else — which is also what
		// keeps it from reporting on stored data the caller cannot see.
		"a damaged row": {
			err: fmt.Errorf(
				"%w: stored backend profile %q of tenant %q does not match its recorded fingerprint",
				tenant.ErrConfigIntegrity, "p1", "tenant-a"),
			status:     http.StatusInternalServerError,
			wantCode:   "internal_error",
			wantAbsent: "fingerprint",
		},
		// A driver failure is the same answer, for the stronger reason: its
		// message carries SQL text and column values with it.
		"a storage failure": {
			err: errors.New(
				`ERROR: relation "backend_profiles" does not exist (SQLSTATE 42P01)`),
			status:     http.StatusInternalServerError,
			wantCode:   "internal_error",
			wantAbsent: "SQLSTATE",
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Through the handler rather than by calling the mapper directly, so
			// what is pinned is the answer a caller gets from a route that exists.
			platform := newPlatformTestServerWith(t, platformTestOptions{
				profiles: stubProfiles{err: tc.err},
			})
			response := requireStatus(
				t, platform.handler, http.MethodGet, profileTenantA+"/p1", "",
				adminHeaders(adminKeyTenantA), tc.status,
			)
			require.Contains(t, response.Body.String(), tc.wantCode)
			if tc.wantAbsent != "" {
				require.NotContains(t, response.Body.String(), tc.wantAbsent)
			}
		})
	}
}

// A revision may only name a profile that is there, and the refusal lands where
// the revision is written.
//
// Finding this out at publish time, or worse at the first chat request, is
// finding it out at the point where an operator can least tell whether the
// platform or the configuration is wrong.
func TestAdminRevisionRefusesAnAbsentBackendProfile(t *testing.T) {
	platform := newPlatformTestServer(t)
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	refused := requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		revisionBody(t, "revision-1", 1, "no-such-profile"),
		adminHeaders(adminKeyTenantA), http.StatusBadRequest,
	)
	require.Contains(t, refused.Body.String(), "invalid_argument")
	// Naming the id back discloses nothing: the caller supplied it, and the
	// lookup was scoped to the caller's own tenant.
	require.Contains(t, refused.Body.String(), "no-such-profile")

	// And nothing was written, so this is a refusal to store rather than a
	// refusal to publish something already there.
	requireStatus(
		t, platform.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1", "",
		adminHeaders(adminKeyTenantA), http.StatusNotFound,
	)
}

// One tenant's profile id is not another tenant's, and a revision that reaches
// for one is refused exactly as if the id had been invented.
func TestAdminRevisionCannotNameAnotherTenantsBackendProfile(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	profiles := newTestProfiles(t, repository)
	platform := newPlatformTestServerWith(t, platformTestOptions{
		repository: repository,
		profiles:   profiles,
	})
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)
	seedTenantAndApp(t, platform.handler, "tenant-b", appAssistant)

	// tenant-b's profile, created through tenant-b's own credential.
	requireStatus(
		t, platform.handler, http.MethodPost, profileTenantB,
		fmt.Sprintf(inMemoryProfileBody, "shared-name"),
		adminHeaders(adminKeyTenantB), http.StatusCreated,
	)

	refused := requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		revisionBody(t, "revision-1", 1, "shared-name"),
		adminHeaders(adminKeyTenantA), http.StatusBadRequest,
	)
	require.Contains(t, refused.Body.String(), "is not a backend profile of this tenant")

	// tenant-a creating its own profile under the same id is a different profile,
	// and the same revision is then accepted — which is what makes the refusal
	// above about ownership rather than about the id.
	requireStatus(
		t, platform.handler, http.MethodPost, profileTenantA,
		fmt.Sprintf(inMemoryProfileBody, "shared-name"),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
	requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		revisionBody(t, "revision-1", 1, "shared-name"),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
}

// The credentials a profile names are checked again at every gate a revision
// passes through, not trusted from the moment the profile was stored.
//
// This is the withdrawn-grant case, and it is the reason create time cannot be
// the only check. The profile is data and outlives any one process; the grant is
// this process's configuration. A profile created while a grant existed must not
// keep working as a way into a credential the grant no longer covers, and the
// only place that can be enforced is each gate the profile is reached through.
func TestAdminRechecksBackendProfileEntitlementAtEveryGate(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	profiles := newTestProfiles(t, repository)
	entitled := newPlatformTestServerWith(t, platformTestOptions{
		repository: repository,
		profiles:   profiles,
		revisions: mustEntitle(t, security.Grant{
			TenantID:   "tenant-a",
			SecretRefs: []string{tenantADSNRef},
		}),
	})
	seedTenantAndApp(t, entitled.handler, "tenant-a", appAssistant)
	requireStatus(
		t, entitled.handler, http.MethodPost, profileTenantA,
		fmt.Sprintf(postgresProfileBody, "primary", tenantADSNRef),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
	requireStatus(
		t, entitled.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		revisionBody(t, "revision-1", 1, "primary"),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)

	// The same control plane, read by a process whose manifest no longer entitles
	// that reference. A restart with a changed manifest is the only way a grant
	// actually goes away, so it is how the withdrawal is staged.
	restarted := newPlatformTestServerWith(t, platformTestOptions{
		repository: repository,
		profiles:   profiles,
		revisions:  security.DenyCapabilities(),
	})

	// Publish refuses the stored revision, and it is the profile gate that
	// refuses it: the revision names no secret and no policy of its own, so
	// AuthorizeRevision passes it.
	refusedPublish := requireStatus(
		t, restarted.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish", "",
		adminHeaders(adminKeyTenantA), http.StatusForbidden,
	)
	require.JSONEq(t, notEntitledBody, refusedPublish.Body.String())

	// Creating a new revision that names it is refused the same way, so a draft
	// cannot be written against a profile that is no longer usable.
	refusedCreate := requireStatus(
		t, restarted.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions",
		revisionBody(t, "revision-2", 2, "primary"),
		adminHeaders(adminKeyTenantA), http.StatusForbidden,
	)
	require.JSONEq(t, notEntitledBody, refusedCreate.Body.String())
	requireStatus(
		t, restarted.handler, http.MethodGet,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-2", "",
		adminHeaders(adminKeyTenantA), http.StatusNotFound,
	)

	// Reading the profile itself is still allowed. The grant decides what may be
	// resolved, not what a tenant may see of its own control plane, and losing
	// this would leave an operator unable to find out why the publish was
	// refused. It discloses nothing either: the profile is the caller's own, and
	// the caller wrote it.
	requireStatus(
		t, restarted.handler, http.MethodGet, profileTenantA+"/primary", "",
		adminHeaders(adminKeyTenantA), http.StatusOK,
	)

	// Under the original configuration the same publish succeeds, so what is
	// under test is the grant and not the wiring.
	requireStatus(
		t, entitled.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish", "",
		adminHeaders(adminKeyTenantA), http.StatusOK,
	)
}

// Publish refuses a revision whose profile is not there, and calls it a fault in
// the revision rather than a missing resource.
//
// The distinction matters to whoever reads it: a 404 here would say "there is no
// such revision", which is false and sends an operator looking in the wrong
// place.
func TestAdminPublishRefusesARevisionWhoseProfileIsAbsent(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	profiles := newTestProfiles(t, repository)
	platform := newPlatformTestServerWith(t, platformTestOptions{
		repository: repository,
		profiles:   profiles,
	})
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	// Written straight to the repository, because the Admin API would refuse to
	// create it — which is the point being isolated: publish makes its own check
	// rather than trusting that create made one. A revision can reach this state
	// for real, by being written while the profile existed and read back after a
	// restore that did not carry it.
	_, err := repository.CreateRevision(
		context.Background(),
		tenant.TenantContext{TenantID: "tenant-a"},
		tenant.AgentRevision{
			ID: "revision-1", TenantID: "tenant-a", AgentAppID: appAssistant, RevisionNo: 1,
			CreatedBy: principalAdminA,
			Config: tenant.RevisionConfig{
				AgentName:        "test-agent",
				Instruction:      "Answer through the deterministic model.",
				Model:            tenant.ModelConfig{Provider: "deterministic", Name: "echo-v1"},
				BackendProfileID: "vanished",
			},
		},
	)
	require.NoError(t, err)

	refused := requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish", "",
		adminHeaders(adminKeyTenantA), http.StatusBadRequest,
	)
	require.Contains(t, refused.Body.String(), "invalid_revision")
	require.Contains(t, refused.Body.String(), "vanished")

	// Creating the profile it names makes the same publish succeed, so the
	// refusal was about the reference and not about the revision.
	requireStatus(
		t, platform.handler, http.MethodPost, profileTenantA,
		fmt.Sprintf(inMemoryProfileBody, "vanished"),
		adminHeaders(adminKeyTenantA), http.StatusCreated,
	)
	requireStatus(
		t, platform.handler, http.MethodPost,
		"/admin/v1/tenants/tenant-a/apps/assistant/revisions/revision-1/publish", "",
		adminHeaders(adminKeyTenantA), http.StatusOK,
	)
}

// A revision that names no profile reaches neither gate.
//
// The empty reference means this process's default store, so there is nothing to
// look up — and the profile repository here fails the test if it is asked. That
// is the compatibility claim these two gates had to preserve: every revision
// written before backend profiles existed still creates and publishes without
// touching profile storage at all.
func TestAdminRevisionWithoutABackendProfileNeverAsksForOne(t *testing.T) {
	platform := newPlatformTestServerWith(t, platformTestOptions{
		profiles: &failingProfiles{t: t},
	})
	seedTenantAndApp(t, platform.handler, "tenant-a", appAssistant)

	createRevisionThroughAPI(
		t, platform.handler, "tenant-a", appAssistant, "revision-1", 1, "echo-v1")
	publishRevisionThroughAPI(t, platform.handler, "tenant-a", appAssistant, "revision-1")
}

func decodeProfile(t *testing.T, response *httptest.ResponseRecorder) storagebundle.ProfileRecord {
	t.Helper()
	var record storagebundle.ProfileRecord
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &record))
	return record
}

func decodeProfileList(
	t *testing.T,
	response *httptest.ResponseRecorder,
) listBackendProfilesResponse {
	t.Helper()
	var listed listBackendProfilesResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &listed))
	return listed
}
