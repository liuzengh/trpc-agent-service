package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

const maxAdminRequestBytes = 1 << 20

type createTenantRequest struct {
	ID          string             `json:"id"`
	Slug        string             `json:"slug"`
	Name        string             `json:"name"`
	Status      tenant.Status      `json:"status,omitempty"`
	Quota       tenant.Quota       `json:"quota,omitempty"`
	AuditPolicy tenant.AuditPolicy `json:"audit_policy,omitempty"`
}

type createAgentAppRequest struct {
	ID     string           `json:"id"`
	Name   string           `json:"name"`
	Status tenant.AppStatus `json:"status,omitempty"`
}

// createRevisionRequest carries no created_by. Authorship is not something a
// request may state: it is the principal of the credential that made the
// request, and the field is absent rather than accepted-and-overwritten so that
// a body still carrying it is refused by DisallowUnknownFields instead of
// quietly meaning something other than what it says.
type createRevisionRequest struct {
	ID         string                `json:"id"`
	RevisionNo uint64                `json:"revision_no"`
	Config     tenant.RevisionConfig `json:"config"`
}

// createBackendProfileRequest is the whole of what a caller may say about a
// backend profile: the id it is filed under, and the storage it describes.
//
// The tenant is absent because it is the path, and the rest of what a stored
// profile carries — the fingerprint, the author, the creation time — is
// provenance rather than content. A body that states one of those is refused by
// DisallowUnknownFields, for the reason createRevisionRequest documents: a
// field that is accepted and then overwritten is a field that means something
// other than what it says.
type createBackendProfileRequest struct {
	ID      string                    `json:"id"`
	Session storagebundle.SessionSpec `json:"session"`
}

// listBackendProfilesResponse wraps the list in an object rather than answering
// with a bare JSON array, so a later page cursor or count can be added without
// changing the shape of every existing client's parse.
type listBackendProfilesResponse struct {
	Profiles []storagebundle.ProfileRecord `json:"profiles"`
}

type publishRevisionResponse struct {
	App      tenant.AgentApp      `json:"app"`
	Revision tenant.AgentRevision `json:"revision"`
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// adminPathPrefix is the control-plane subtree. Everything at or below it is
// the Admin API — see adminFirst, which is what makes that true of the paths
// that only look like they are below it.
const adminPathPrefix = "/admin"

// adminTenantsPrefix is the only admin route family that exists. Everything
// else under /admin reaches handleAdmin too, and is answered — after
// authentication — as a route that is not there.
const adminTenantsPrefix = adminPathPrefix + "/v1/tenants"

// adminMediaTypeJSON is what an admin write request must declare.
const adminMediaTypeJSON = "application/json"

// backendProfilesSegment is the collection a tenant's storage profiles live
// under. It is a sibling of "apps" rather than a child of one: a profile is a
// tenant's storage arrangement, and several apps' revisions may name the same
// one.
const backendProfilesSegment = "backend-profiles"

// handleAdmin is the control-plane trust boundary.
//
// Authentication comes first, before the path is looked at and before the
// method is judged. That ordering is the point: a caller who has not presented
// a valid admin credential gets the same 401 from a real route, an unknown
// route and a wrong method alike, so this API answers no questions about its
// own shape. Routing after authentication also means no handler below can be
// reached by a caller this function has not already identified.
//
// No response from here carries an Access-Control-Allow header — not on
// success, not on failure, and there is no preflight branch. A browser page on
// another origin therefore cannot read an admin response, and cannot send an
// admin write at all: requireAdminContentType keeps every write outside the
// "simple request" set that browsers send without asking first, and the
// preflight they must send instead is refused by the absence of those headers.
// An OPTIONS request is not special-cased anywhere; it authenticates like
// everything else and is then answered as a method these routes do not take.
func (s *PlatformServer) handleAdmin(w http.ResponseWriter, r *http.Request) {
	admin, ok := s.authenticateAdmin(w, r)
	if !ok {
		return
	}
	if !requireAdminContentType(w, r) {
		return
	}
	path := r.URL.Path
	if path != adminTenantsPrefix && !strings.HasPrefix(path, adminTenantsPrefix+"/") {
		writeAPIError(w, http.StatusNotFound, "not_found", "admin route not found")
		return
	}
	trimmed := strings.Trim(strings.TrimPrefix(path, adminTenantsPrefix), "/")
	var segments []string
	if trimmed != "" {
		segments = strings.Split(trimmed, "/")
	}
	// Tenant scope is decided once, here, for every route that names a tenant —
	// and before the route is dispatched, so no handler can reach the
	// repository on behalf of a tenant the caller does not administer.
	if len(segments) > 0 && !s.allowsAdminTenant(w, admin, segments[0]) {
		return
	}

	switch {
	case len(segments) == 0:
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		// Creating a tenant is the one control-plane operation that belongs to
		// no tenant, so it belongs to the role that belongs to no tenant.
		if !admin.IsPlatformAdmin() {
			writeForbidden(w)
			return
		}
		s.createTenant(w, r)
	case len(segments) == 1:
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.getTenant(w, r, segments[0])
	case len(segments) == 2 && segments[1] == "apps":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.createAgentApp(w, r, segments[0])
	case len(segments) == 3 && segments[1] == "apps":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.getAgentApp(w, r, segments[0], segments[2])
	case len(segments) == 2 && segments[1] == backendProfilesSegment:
		switch r.Method {
		case http.MethodPost:
			s.createBackendProfile(w, r, admin, segments[0])
		case http.MethodGet:
			s.listBackendProfiles(w, r, segments[0])
		default:
			methodNotAllowed(w, http.MethodGet, http.MethodPost)
		}
	case len(segments) == 3 && segments[1] == backendProfilesSegment:
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.getBackendProfile(w, r, segments[0], segments[2])
	case len(segments) == 4 && segments[1] == "apps" && segments[3] == "revisions":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.createRevision(w, r, admin, segments[0], segments[2])
	case len(segments) == 5 && segments[1] == "apps" && segments[3] == "revisions":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.getRevision(w, r, segments[0], segments[2], segments[4])
	case len(segments) == 6 && segments[1] == "apps" && segments[3] == "revisions" && segments[5] == "publish":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.publishRevision(w, r, segments[0], segments[2], segments[4])
	default:
		writeAPIError(w, http.StatusNotFound, "not_found", "admin route not found")
	}
}

// authenticateAdmin resolves the control-plane caller, or answers 401.
//
// Every failure is the same answer. A malformed header, an unknown key, a chat
// key presented here, an expired context: all of them are "the credential is
// not valid", because the differences between them are only useful to someone
// who does not have one.
func (s *PlatformServer) authenticateAdmin(
	w http.ResponseWriter,
	r *http.Request,
) (identity.AdminIdentity, bool) {
	token, ok := bearerToken(r.Header.Get(HeaderAuthorization))
	if !ok {
		writeUnauthenticated(w, "a Bearer credential is required")
		return identity.AdminIdentity{}, false
	}
	admin, err := s.admin.AuthenticateAdmin(r.Context(), token)
	if err != nil {
		writeUnauthenticated(w, "the credential is not valid")
		return identity.AdminIdentity{}, false
	}
	if err := admin.Validate(); err != nil {
		writeUnauthenticated(w, "the credential is not valid")
		return identity.AdminIdentity{}, false
	}
	return admin, true
}

// allowsAdminTenant reports whether admin may act on tenantID, and answers the
// caller when it may not.
//
// A platform admin passes, and a malformed id is left to the repository to
// reject as a bad request: that is not a disclosure to a caller who administers
// every tenant anyway. A tenant admin is compared exactly against its own
// tenant, and a mismatch is answered right here — before any repository call —
// with a body byte-identical to a real not-found. Both halves matter. A
// different body, or a different status, would turn this API into an oracle for
// which tenants exist; reaching the repository first would let one tenant's
// admin make this process query the database on another tenant's behalf, which
// is a load and an audit trail it has no business creating.
func (s *PlatformServer) allowsAdminTenant(
	w http.ResponseWriter,
	admin identity.AdminIdentity,
	tenantID string,
) bool {
	if admin.IsPlatformAdmin() || admin.AllowsTenant(tenantID) {
		return true
	}
	writeNotFound(w)
	return false
}

// requireAdminContentType refuses an admin write that does not declare JSON.
//
// This is not about parsing — decodeAdminJSON rejects a body that is not JSON
// anyway, and publish sends no body at all. It is about what a browser may send
// without asking permission first. A POST whose Content-Type is one of three
// form-ish values is a CORS "simple request": a page on any origin can send it
// with whatever credentials the browser holds, and never needs to read the
// response for the write to have happened. Requiring application/json puts
// every admin write outside that set, so the browser must preflight — and the
// preflight has nothing to succeed against, because no admin response carries a
// single Access-Control-Allow header.
func requireAdminContentType(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, adminMediaTypeJSON) {
		writeAPIError(
			w,
			http.StatusUnsupportedMediaType,
			"unsupported_media_type",
			"admin requests must send Content-Type: "+adminMediaTypeJSON,
		)
		return false
	}
	return true
}

func (s *PlatformServer) createTenant(w http.ResponseWriter, r *http.Request) {
	var request createTenantRequest
	if !decodeAdminJSON(w, r, &request) {
		return
	}
	created, err := s.repository.CreateTenant(r.Context(), tenant.Tenant{
		ID: request.ID, Slug: request.Slug, Name: request.Name,
		Status: request.Status, Quota: request.Quota, AuditPolicy: request.AuditPolicy,
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.Header().Set("Location", "/admin/v1/tenants/"+created.ID)
	writeJSON(w, http.StatusCreated, created)
}

func (s *PlatformServer) getTenant(w http.ResponseWriter, r *http.Request, tenantID string) {
	item, err := s.repository.GetTenant(r.Context(), tenantID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *PlatformServer) createAgentApp(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
) {
	var request createAgentAppRequest
	if !decodeAdminJSON(w, r, &request) {
		return
	}
	created, err := s.repository.CreateAgentApp(
		r.Context(),
		tenant.TenantContext{TenantID: tenantID},
		tenant.AgentApp{
			ID: request.ID, TenantID: tenantID, Name: request.Name, Status: request.Status,
		},
	)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/admin/v1/tenants/%s/apps/%s", tenantID, created.ID))
	writeJSON(w, http.StatusCreated, created)
}

func (s *PlatformServer) getAgentApp(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
	appID string,
) {
	item, err := s.repository.GetAgentApp(
		r.Context(), tenant.TenantContext{TenantID: tenantID}, appID,
	)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

// createBackendProfile stores one immutable storage arrangement for a tenant.
//
// The order of the three steps is the whole security argument, and it is the
// same order the Factory uses when it later builds against this profile:
//
//  1. Validate the shape. A profile that could never be built is refused with
//     the reason, which is the caller's own submission read back to them.
//  2. Authorize every credential it names, against the same entitlement table
//     and by the same exact string a model key goes through. Without this a
//     tenant could name another tenant's DSN variable and get a working
//     connection out of it — a second, unentitled channel to the process
//     environment that no revision check would ever see.
//  3. Only then store it.
//
// Nothing here reads an environment variable and nothing connects. This handler
// cannot tell whether the reference it just authorized names a variable that is
// set, which is exactly the property that keeps it from being a probe for the
// process environment.
func (s *PlatformServer) createBackendProfile(
	w http.ResponseWriter,
	r *http.Request,
	admin identity.AdminIdentity,
	tenantID string,
) {
	var request createBackendProfileRequest
	if !decodeAdminJSON(w, r, &request) {
		return
	}
	// The tenant is the path's, never the body's: there is no field to disagree
	// with the scope allowsAdminTenant already decided.
	profile := storagebundle.Profile{
		TenantID: tenantID,
		ID:       request.ID,
		Session:  request.Session,
	}
	if err := profile.Validate(); err != nil {
		writeProfileError(w, err)
		return
	}
	if err := s.authorizeProfileSecrets(tenantID, profile); err != nil {
		writeNotEntitled(w)
		return
	}
	created, err := s.profiles.CreateProfile(
		r.Context(),
		tenant.TenantContext{TenantID: tenantID},
		profile,
		// Authorship is the authenticated principal, never a request field.
		admin.PrincipalID,
	)
	if err != nil {
		writeProfileError(w, err)
		return
	}
	w.Header().Set("Location", fmt.Sprintf(
		"/admin/v1/tenants/%s/%s/%s", tenantID, backendProfilesSegment, created.ID))
	writeJSON(w, http.StatusCreated, created)
}

func (s *PlatformServer) listBackendProfiles(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
) {
	records, err := s.profiles.ListProfiles(r.Context(), tenant.TenantContext{TenantID: tenantID})
	if err != nil {
		writeProfileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, listBackendProfilesResponse{Profiles: records})
}

func (s *PlatformServer) getBackendProfile(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
	profileID string,
) {
	record, err := s.profiles.GetProfile(
		r.Context(), tenant.TenantContext{TenantID: tenantID}, profileID)
	if err != nil {
		writeProfileError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

// authorizeProfileSecrets checks every credential a profile names against the
// process entitlement table.
//
// It is one loop in one place because three call sites ask the same question —
// creating a profile, creating a revision that names one, publishing one — and
// a fourth backend added to Profile.SecretRefs has to reach all three at once.
//
// The error is security.ErrNotEntitled and says nothing about which reference
// was refused, so a caller cannot tell a variable this process has from one it
// does not.
func (s *PlatformServer) authorizeProfileSecrets(
	tenantID string,
	profile storagebundle.Profile,
) error {
	for _, ref := range profile.SecretRefs() {
		if err := s.capabilities.AuthorizeSecretRef(tenantID, ref); err != nil {
			return err
		}
	}
	return nil
}

// checkBackendProfileReference resolves the profile a revision config names and
// checks that this tenant may use it.
//
// Both halves are refusals a revision has earned. The profile has to exist in
// this tenant — a reference to one that is not there is a revision that can
// never be served, and finding that out at publish time, or worse at the first
// chat request, is finding it out at the point where an operator can least tell
// whether the platform or the configuration is wrong. And the credentials it
// names have to be entitled to this tenant a second time, at this second gate,
// because a profile created while a grant existed must not keep working as a
// way into a credential the grant no longer covers.
//
// An empty reference means this process's default store and needs neither.
//
// It returns the error instead of writing it: the two call sites owe their
// callers different words for the same fault, because one is refusing a field
// of the request in front of it and the other is refusing a revision that was
// stored some time ago.
func (s *PlatformServer) checkBackendProfileReference(
	ctx context.Context,
	tenantID string,
	config tenant.RevisionConfig,
) error {
	if config.BackendProfileID == "" {
		return nil
	}
	// Scoped to the caller's own tenant, so this can only ever confirm the
	// existence of a profile the caller administers.
	record, err := s.profiles.GetProfile(
		ctx, tenant.TenantContext{TenantID: tenantID}, config.BackendProfileID)
	if err != nil {
		return err
	}
	return s.authorizeProfileSecrets(tenantID, record.Profile)
}

// createRevision stores a draft revision, authored by the caller.
//
// The entitlement check happens here, before the write, so a revision naming a
// capability this tenant does not hold never reaches the database. Checking only
// at publish would let a draft accumulate refs that look accepted and then fail
// much later, at the point where an operator is least able to tell whether the
// configuration or the platform is wrong.
//
// A backend_profile_id is checked immediately after, and in that order: the
// revision's own capabilities decide whether this caller may store anything at
// all, and only a caller who has cleared that gets an answer about which
// profiles its tenant owns.
func (s *PlatformServer) createRevision(
	w http.ResponseWriter,
	r *http.Request,
	admin identity.AdminIdentity,
	tenantID string,
	appID string,
) {
	var request createRevisionRequest
	if !decodeAdminJSON(w, r, &request) {
		return
	}
	if err := s.capabilities.AuthorizeRevision(tenantID, request.Config); err != nil {
		writeNotEntitled(w)
		return
	}
	if err := s.checkBackendProfileReference(r.Context(), tenantID, request.Config); err != nil {
		if errors.Is(err, security.ErrNotEntitled) {
			writeNotEntitled(w)
			return
		}
		if errors.Is(err, storagebundle.ErrProfileNotFound) {
			// The request named it, so this is a 400 about a field the caller
			// controls — and it is not a disclosure, because the lookup was
			// scoped to the caller's own tenant.
			writeAPIError(w, http.StatusBadRequest, "invalid_argument", fmt.Sprintf(
				"backend_profile_id %q is not a backend profile of this tenant",
				request.Config.BackendProfileID))
			return
		}
		writeProfileError(w, err)
		return
	}
	created, err := s.repository.CreateRevision(
		r.Context(),
		tenant.TenantContext{TenantID: tenantID},
		tenant.AgentRevision{
			ID: request.ID, TenantID: tenantID, AgentAppID: appID,
			RevisionNo: request.RevisionNo, Config: request.Config,
			// Authorship is the authenticated principal, never a request field.
			CreatedBy: admin.PrincipalID,
		},
	)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	w.Header().Set(
		"Location",
		fmt.Sprintf("/admin/v1/tenants/%s/apps/%s/revisions/%s", tenantID, appID, created.ID),
	)
	writeJSON(w, http.StatusCreated, created)
}

func (s *PlatformServer) getRevision(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
	appID string,
	revisionID string,
) {
	item, err := s.repository.GetRevision(
		r.Context(), tenant.TenantContext{TenantID: tenantID}, appID, revisionID,
	)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

// publishRevision makes a revision servable, after checking that it may be.
//
// Publishing is the moment a configuration stops being a draft and starts being
// something this platform will run, so both checks that decide whether it can
// run happen here, on the stored revision rather than on anything the request
// says. The revision is immutable, so reading it first is not a race: what is
// read is what will be published, and what a Runtime will later build from.
//
// The order is entitlement, then tool registry, then storage — the same order
// Runtime uses. Entitlement is about what this tenant is allowed to reach at
// all, and it answers identically whether or not the ref names anything real;
// the registry and storage checks are about whether a revision this tenant may
// run is internally coherent, and they can afford to say what is wrong because
// by then nothing is being disclosed to someone who should not know it.
//
// Storage is checked again here rather than trusted from create time. A profile
// is looked up by an id that was legal when the draft was written, and between
// then and now the grant behind its credentials may have been withdrawn — a
// revision that named a profile while it was entitled must not become a way to
// keep using it afterwards.
func (s *PlatformServer) publishRevision(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
	appID string,
	revisionID string,
) {
	scope := tenant.TenantContext{TenantID: tenantID}
	revision, err := s.repository.GetRevision(r.Context(), scope, appID, revisionID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	if err := s.capabilities.AuthorizeRevision(tenantID, revision.Config); err != nil {
		writeNotEntitled(w)
		return
	}
	if _, err := tool.Builtin().Resolve(
		revision.Config.ToolRefs, revision.Config.PolicyRefs,
	); err != nil {
		// Distinct from not_entitled on purpose: this caller is entitled to every
		// ref it named, so the remaining fault is in the revision itself, and
		// saying so is the difference between a fixable error and a mystery.
		writeAPIError(
			w, http.StatusBadRequest, "invalid_revision", "revision cannot be served: "+err.Error())
		return
	}
	if err := s.checkBackendProfileReference(r.Context(), tenantID, revision.Config); err != nil {
		if errors.Is(err, security.ErrNotEntitled) {
			writeNotEntitled(w)
			return
		}
		if errors.Is(err, storagebundle.ErrProfileNotFound) {
			// Same code as the tool-registry refusal above, because it is the
			// same fault: a revision this tenant may run, naming something this
			// platform cannot give it. The id is the caller's own and the lookup
			// was scoped to the caller's own tenant, so naming it discloses
			// nothing.
			writeAPIError(w, http.StatusBadRequest, "invalid_revision", fmt.Sprintf(
				"revision cannot be served: backend profile %q is not a backend profile of this tenant",
				revision.Config.BackendProfileID))
			return
		}
		writeProfileError(w, err)
		return
	}
	app, published, err := s.repository.PublishRevision(r.Context(), scope, appID, revisionID)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, publishRevisionResponse{App: app, Revision: published})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func decodeAdminJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "invalid request body")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return false
	}
	return true
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, identity.ErrUnauthenticated):
		writeUnauthenticated(w, "the credential is not valid")
	case errors.Is(err, identity.ErrForbidden):
		writeForbidden(w)
	// Storage routing, and it comes before the invalid_argument case rather than
	// near the other revision cases below. That is load-bearing, not tidiness:
	// storagebundle.Profile.Validate wraps both ErrInvalidProfile and
	// tenant.ErrInvalidArgument, so a profile this process cannot build would
	// otherwise leave here as a 400 carrying err.Error() — which is the profile
	// id, the tenant id and whichever internal reason the Factory gave.
	//
	// None of that is the caller's. The chat client named no profile; a revision
	// did, and which storage a revision runs on is platform configuration. So all
	// six collapse into the same "this revision is not available" a client
	// already gets for an unpublished or un-entitled one, with no wording that
	// says which. It is 409 rather than 500 because the platform is working: this
	// revision is not servable here, and retrying it unchanged will not help.
	case errors.Is(err, storagebundle.ErrProfileNotFound),
		errors.Is(err, storagebundle.ErrProfileChanged),
		errors.Is(err, storagebundle.ErrInvalidProfile),
		errors.Is(err, storagebundle.ErrUnsupportedBackend),
		errors.Is(err, storagebundle.ErrPinsNotDurable),
		errors.Is(err, storagebundle.ErrNotSharedAcrossWorkers):
		writeAPIError(
			w, http.StatusConflict, "revision_unavailable", "revision is not available")
	// A Router and a RuntimeResolver that are shutting down are one answer,
	// because to a caller they are one fact: this process is going away. 503 with
	// no retry hint — the next request will reach a different process or none,
	// and this one cannot say which.
	//
	// storagebundle.ErrIncompleteBundle is deliberately absent from both lists.
	// A Factory that returned a Bundle with nothing in it is a defect in this
	// platform's own code, and the 500 it falls through to is what says so.
	case errors.Is(err, storagebundle.ErrRouterClosed),
		errors.Is(err, platformagent.ErrResolverClosed):
		writeAPIError(
			w, http.StatusServiceUnavailable, "runtime_unavailable", "runtime is unavailable")
	case errors.Is(err, tenant.ErrInvalidArgument):
		writeAPIError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, tenant.ErrTenantScope), errors.Is(err, tenant.ErrNotFound):
		// Same writer as the tenant-scope short-circuit above, so a refused
		// cross-tenant read and a genuinely missing resource stay identical.
		writeNotFound(w)
	case errors.Is(err, tenant.ErrAlreadyExists):
		writeAPIError(w, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, tenant.ErrTenantInactive):
		writeAPIError(w, http.StatusForbidden, "tenant_inactive", err.Error())
	case errors.Is(err, tenant.ErrNoPublishedRevision), errors.Is(err, tenant.ErrRevisionNotPublished):
		writeAPIError(w, http.StatusConflict, "revision_unavailable", err.Error())
	case errors.Is(err, security.ErrNotEntitled), errors.Is(err, tenant.ErrConfigIntegrity):
		// A revision that reaches a Runtime build un-entitled, or whose stored
		// bytes no longer match their digest, is a revision this platform will not
		// serve — and the reason is not the caller's to learn. Both collapse into
		// the same "this revision is unavailable" that a client already gets for
		// an unpublished one, with no wording that says which of the three it was.
		writeAPIError(
			w, http.StatusConflict, "revision_unavailable", "revision is not available")
	default:
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// writeProfileError maps a control-plane profile failure, and is deliberately
// not writeDomainError.
//
// The two disagree about the same sentinels, and each is right for its own
// caller. To a chat client, storagebundle.ErrProfileNotFound means a published
// revision points at storage this process cannot give it: the client named no
// profile, it can do nothing about the one that is missing, and telling it which
// is missing would leak another tenant's control-plane configuration — so
// writeDomainError collapses it into a 409 "revision is not available". To an
// admin asking this tenant's own profile collection for an id, the same sentinel
// means exactly what a missing resource always means, and the honest answer is
// the same 404 a missing tenant or app gets.
//
// tenant.ErrConfigIntegrity splits the same way and for the same reason. Here it
// is a 500: the request was well formed, no edit to it would help, and a stored
// row that no longer matches its own fingerprint is this platform's fault to fix.
//
// Sharing one mapper would force one of the two answers to be wrong. Keeping
// them apart also keeps the profile-shaped sentinels out of the chat path, where
// a new one added to this list must not silently start changing what a chat
// client is told.
func writeProfileError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storagebundle.ErrProfileNotFound):
		writeNotFound(w)
	case errors.Is(err, storagebundle.ErrInvalidProfile):
		// The message is the caller's own submission read back to them. It
		// carries no credential: Profile.Validate refuses a malformed secret
		// reference without echoing it, and sessionbackend.Config.Validate names
		// the field and the rule rather than the value.
		writeAPIError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, storagebundle.ErrProfileLimit):
		// Its own code, because it is the one refusal here that no change to the
		// request can fix and no retry will clear: profiles are immutable and
		// there is no delete, so the budget is spent for good.
		writeAPIError(w, http.StatusConflict, "profile_limit", err.Error())
	case errors.Is(err, security.ErrNotEntitled):
		writeNotEntitled(w)
	case errors.Is(err, tenant.ErrInvalidArgument):
		writeAPIError(w, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, tenant.ErrTenantScope), errors.Is(err, tenant.ErrNotFound):
		// Same writer as the tenant-scope short-circuit in handleAdmin, so a
		// refused cross-tenant read and a genuinely missing tenant stay identical.
		writeNotFound(w)
	case errors.Is(err, tenant.ErrAlreadyExists):
		// The id is the version, so this is the ordinary answer to "publish new
		// storage", not an edge case: a profile is never replaced, a different
		// arrangement is a different id.
		writeAPIError(w, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, tenant.ErrTenantInactive):
		writeAPIError(w, http.StatusForbidden, "tenant_inactive", err.Error())
	default:
		// tenant.ErrConfigIntegrity lands here, and so does every storage
		// failure. Neither is the caller's, and neither message is theirs to
		// read: a driver error spliced into a body would carry SQL text and
		// column values with it.
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

// writeNotFound is the one not-found answer this package gives.
//
// A tenant admin reaching for another tenant gets exactly this, and so does a
// caller asking for a tenant that was never created — same status, same code,
// same message, byte for byte. It is a single function rather than two identical
// literals so that they cannot drift apart later: the moment the two answers
// differ by a word, the difference between them is a way to enumerate tenants.
func writeNotFound(w http.ResponseWriter) {
	writeAPIError(w, http.StatusNotFound, "not_found", "resource not found")
}

// writeForbidden answers a caller who is known, and known not to be allowed.
//
// This is distinct from writeNotFound on purpose. It appears only where the
// existence of the thing is not a secret from this caller — a tenant admin
// cannot create tenants, and knows it already — never where the answer would
// reveal whether some other tenant's resource exists.
func writeForbidden(w http.ResponseWriter) {
	writeAPIError(w, http.StatusForbidden, "forbidden", "this credential is not allowed")
}

// writeNotEntitled refuses a configuration that names a capability its tenant
// does not hold.
//
// The wording is fixed and says nothing about which ref was rejected. That is
// the whole design: a caller must get the same answer for a secret_ref whose
// environment variable exists and one whose does not, for a policy the registry
// knows and one it has never heard of. Any difference between those cases would
// make this endpoint a probe for the process environment and the tool registry
// of a platform the caller does not administer.
//
// It says "configuration" rather than "revision" because every gate that
// refuses shares it, and not all of them are looking at a revision: creating a
// backend profile is refused here too, and a message naming a revision would be
// describing a request that has none. Keeping it one function is what keeps
// them identical — the moment a gate words its refusal differently, the
// difference is what a caller reads the answer for.
func writeNotEntitled(w http.ResponseWriter) {
	writeAPIError(
		w,
		http.StatusForbidden,
		"not_entitled",
		"this tenant is not entitled to a capability the configuration references",
	)
}

func writeAPIError(w http.ResponseWriter, status int, code string, message string) {
	response := apiError{}
	response.Error.Code = code
	response.Error.Message = message
	writeJSON(w, status, response)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}
