package storagebundle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ProfileSource resolves a profile id within one tenant.
//
// Two rules bind every implementation:
//
//   - A profile that does not exist and a profile that belongs to another
//     tenant are both ErrProfileNotFound, and they are not distinguishable.
//     Telling them apart would answer "does tenant B have a profile called
//     p1", which is not a question tenant A may ask.
//   - The same (tenant, id) returns the same content forever. The id is the
//     version; a source that mutates content under a stable id breaks the
//     contract every Bundle built from it depends on, and Router reports that
//     as ErrProfileChanged rather than following it.
type ProfileSource interface {
	ResolveProfile(
		ctx context.Context, scope tenant.TenantContext, profileID string,
	) (Profile, error)
}

// NoProfiles is the source of a process that has no dynamic profiles: every
// lookup is ErrProfileNotFound.
//
// It is a real answer rather than a placeholder — a process with no profile
// storage cannot honour a profile reference, so a revision that names one is
// refused instead of being served by the default store it did not ask for.
// Production does not run on it: the binary passes the same ProfileRepository
// the Admin API writes through, so what a tenant creates is what the data plane
// resolves. What still runs on it is a process assembled without profile
// storage — and a test that wants a Router whose every dynamic lookup fails.
func NoProfiles() ProfileSource {
	return noProfiles{}
}

type noProfiles struct{}

func (noProfiles) ResolveProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (Profile, error) {
	if err := scope.Validate(); err != nil {
		return Profile{}, err
	}
	return Profile{}, fmt.Errorf("%w: %q", ErrProfileNotFound, profileID)
}

// MemoryProfileSource holds profiles in memory, keyed by (tenant, id).
//
// It is the smallest thing that satisfies ProfileSource, and that is what it is
// for: a test that needs a profile to resolve, without a tenant to seed or a
// control plane to write through. MemoryProfileRepository is what production
// storage is modelled on; this is what a Router test is wired to.
//
// It exists to make the immutability contract executable rather than
// aspirational: Put refuses to overwrite, so no test can accidentally depend on
// a profile whose content moved.
type MemoryProfileSource struct {
	mu       sync.RWMutex
	profiles map[profileKey]Profile
}

func NewMemoryProfileSource() *MemoryProfileSource {
	return &MemoryProfileSource{profiles: make(map[profileKey]Profile)}
}

// Put stores a valid profile under its own (TenantID, ID).
//
// An existing key is tenant.ErrAlreadyExists, whatever the content — including
// content identical to what is already there. The id is the version, so
// "replace p1" is never a legal operation; publishing different storage means
// publishing a different id.
//
// What is stored is a deep copy. Refusing to overwrite is only half of "the id
// is the version": a caller that kept the Profile it passed in still holds the
// pointers inside its SessionSpec, and writing through one of those would edit
// stored content without going through Put at all.
func (s *MemoryProfileSource) Put(p Profile) error {
	// Copied before it is validated rather than after. Validating the caller's
	// value and copying it later would leave a window between the two, and what
	// ended up stored would be content no Validate had ever seen.
	stored := p.clone()
	if err := stored.Validate(); err != nil {
		return err
	}
	key := profileKey{tenantID: stored.TenantID, profileID: stored.ID}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.profiles[key]; exists {
		return fmt.Errorf(
			"%w: backend profile %q of tenant %q",
			tenant.ErrAlreadyExists, stored.ID, stored.TenantID)
	}
	s.profiles[key] = stored
	return nil
}

// ResolveProfile returns a copy of the profile scope owns under this id.
//
// A copy, because Router calls this on every Resolve and compares the
// fingerprint of what comes back against the one its Bundle was built from.
// Handing out the stored value would hand out the pointers inside it, and a
// caller that wrote through one would make every later resolution of that id
// fail as ErrProfileChanged.
func (s *MemoryProfileSource) ResolveProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (Profile, error) {
	if err := scope.Validate(); err != nil {
		return Profile{}, err
	}
	// Keyed by the caller's tenant rather than filtered after the lookup:
	// another tenant's profile is not found here, it is not reachable.
	s.mu.RLock()
	defer s.mu.RUnlock()
	profile, found := s.profiles[profileKey{tenantID: scope.TenantID, profileID: profileID}]
	if !found {
		return Profile{}, fmt.Errorf("%w: %q", ErrProfileNotFound, profileID)
	}
	return profile.clone(), nil
}

// MaxProfilesPerTenant is how many backend profiles one tenant may own.
//
// It is a resource bound, not a product decision. Every profile a tenant
// resolves becomes a connection pool and its goroutines for the life of the
// process — Router builds each one once and never evicts it, because a Bundle
// cannot be taken away from the sessions pinned to it. Without a ceiling, a
// tenant with admin credentials could turn a loop over profile ids into as many
// pools as the database has connections, and the process would run out of file
// descriptors serving requests it was told to serve.
//
// Profiles are immutable and there is no delete, so this is also a lifetime
// budget rather than a concurrent one. Thirty-two is generous for the thing it
// bounds: a tenant needs one profile per storage arrangement it has ever
// published, not one per revision.
const MaxProfilesPerTenant = 32

// ErrProfileLimit reports a tenant that already owns MaxProfilesPerTenant
// profiles. It is a distinct sentinel because it is the one refusal an operator
// can do nothing about from the request side.
var ErrProfileLimit = errors.New("storagebundle: tenant already has the maximum number of backend profiles")

// ProfileRecord is a stored Profile together with what storing it recorded.
//
// The Profile is the content; the rest is provenance the tenant does not get to
// choose. Fingerprint is derived, CreatedBy is the authenticated principal that
// created the record, and CreatedAt is the storage clock — an admin request
// that carried any of the three would be describing a history that did not
// happen.
//
// Fingerprint is stored rather than only derived so that a row changed by
// something other than a repository can be detected on the way out. It is not
// an authenticator: anything that can edit the spec can edit the fingerprint
// beside it. It catches the accident — a partial restore, a manual UPDATE, a
// half-applied migration — which is the failure that actually happens, and it
// catches it before a Runtime is built against storage nobody published.
type ProfileRecord struct {
	Profile
	Fingerprint string    `json:"fingerprint"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
}

// Verify re-derives everything about a record that can be re-derived, and
// reports any disagreement as a fault in the stored data.
//
// It is given the identity the record was stored under rather than reading it
// off the record, because that is the disagreement worth catching: a row is
// found by its (tenant, id) columns and its content carries the same pair, so
// content that says something else is content that was moved, restored into the
// wrong row, or written by something that was not this repository. Trusting the
// content would then serve one tenant's storage arrangement to another.
//
// The Fingerprint field shadows the promoted Profile.Fingerprint method, which
// is why the re-derivation goes through the embedded value explicitly.
func (r ProfileRecord) Verify(tenantID string, profileID string) error {
	if r.TenantID != tenantID || r.ID != profileID {
		return fmt.Errorf(
			"%w: stored backend profile %q of tenant %q does not match its own identity",
			tenant.ErrConfigIntegrity, profileID, tenantID)
	}
	fingerprint, err := r.Profile.Fingerprint()
	if err != nil {
		return fmt.Errorf(
			"%w: stored backend profile %q of tenant %q is not a valid profile",
			tenant.ErrConfigIntegrity, profileID, tenantID)
	}
	if fingerprint != r.Fingerprint {
		return fmt.Errorf(
			"%w: stored backend profile %q of tenant %q does not match its recorded fingerprint",
			tenant.ErrConfigIntegrity, profileID, tenantID)
	}
	return nil
}

func (r ProfileRecord) clone() ProfileRecord {
	r.Profile = r.Profile.clone()
	return r
}

// ProfileRepository is the control plane's half of a ProfileSource: it stores
// profiles as well as resolving them.
//
// There is no Update and no Delete, and there will not be one. The id is the
// version: a Bundle is built from a profile once and shared by every session
// pinned to a revision that names it, so changing content under a stable id
// would move live conversations to different storage, and removing content
// would strand them. Publishing different storage means creating a different
// id.
//
// Every method is tenant-scoped, and the scope is the authority: a profile that
// belongs to another tenant is not filtered out of the answer, it is not
// reachable by the query.
type ProfileRepository interface {
	ProfileSource

	// CreateProfile stores a new profile and returns what was stored.
	//
	// An id that already exists in this tenant is tenant.ErrAlreadyExists even
	// when the content is identical, because "the same content" is not
	// something a caller can rely on having checked. A tenant at
	// MaxProfilesPerTenant is ErrProfileLimit.
	CreateProfile(
		ctx context.Context, scope tenant.TenantContext, profile Profile, createdBy string,
	) (ProfileRecord, error)

	// GetProfile returns one stored profile with its provenance.
	GetProfile(
		ctx context.Context, scope tenant.TenantContext, profileID string,
	) (ProfileRecord, error)

	// ListProfiles returns every profile of one tenant, ordered by id.
	ListProfiles(ctx context.Context, scope tenant.TenantContext) ([]ProfileRecord, error)
}

// TenantGate is the one question a profile repository asks about tenants:
// does this tenant exist, and may it be written to?
//
// It is an interface, and a narrow one, so an in-memory profile repository can
// share the tenant table of the control plane it belongs to without importing
// the whole of it. tenant.Repository satisfies it. Asking a table that is not
// the control plane's would let a profile be created for a tenant that does not
// exist, or for one being deleted.
type TenantGate interface {
	GetTenant(ctx context.Context, tenantID string) (tenant.Tenant, error)
}

// MemoryProfileRepository is the in-memory ProfileRepository.
//
// It is what the inmemory process profile runs on, and what the shared
// conformance suite runs against beside the PostgreSQL implementation. It holds
// its own map and borrows the tenant table, so a tenant that the control plane
// never created cannot own a profile here either.
type MemoryProfileRepository struct {
	tenants TenantGate

	mu       sync.RWMutex
	profiles map[profileKey]ProfileRecord
}

// NewMemoryProfileRepository returns a profile repository gated by tenants.
//
// The gate is required. A repository that answered without one would create
// profiles for tenants that do not exist, which is the state the PostgreSQL
// foreign key makes unrepresentable — and two implementations of one interface
// that disagree about that are not one interface.
func NewMemoryProfileRepository(tenants TenantGate) (*MemoryProfileRepository, error) {
	if tenants == nil {
		return nil, errors.New("storagebundle: profile repository requires a tenant source")
	}
	return &MemoryProfileRepository{
		tenants:  tenants,
		profiles: make(map[profileKey]ProfileRecord),
	}, nil
}

// CreateProfile implements ProfileRepository.
//
// The order of the checks is the contract. Everything decidable from the
// request alone is decided first, so a malformed profile never reaches the
// tenant table; the tenant is checked before anything is stored, so a profile
// cannot be created for a tenant that is gone or being deleted; and the limit
// is counted under the same lock as the insert, so two concurrent creations
// cannot both see thirty-one.
func (r *MemoryProfileRepository) CreateProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profile Profile,
	createdBy string,
) (ProfileRecord, error) {
	// Copied before it is validated, for the reason MemoryProfileSource.Put
	// documents: what ends up stored has to be content that was validated,
	// not content that was validated a moment earlier somewhere else.
	stored := profile.clone()
	if err := profileContextError(ctx); err != nil {
		return ProfileRecord{}, err
	}
	if err := scope.Validate(); err != nil {
		return ProfileRecord{}, err
	}
	if stored.TenantID != scope.TenantID {
		return ProfileRecord{}, fmt.Errorf(
			"%w: backend profile belongs to another tenant", tenant.ErrTenantScope)
	}
	if createdBy == "" {
		return ProfileRecord{}, fmt.Errorf(
			"%w: backend profile requires a creator", tenant.ErrInvalidArgument)
	}
	fingerprint, err := stored.Fingerprint()
	if err != nil {
		// Fingerprint validates first, so this is the profile's own refusal.
		return ProfileRecord{}, err
	}
	if err := r.requireActiveTenant(ctx, scope.TenantID); err != nil {
		return ProfileRecord{}, err
	}

	record := ProfileRecord{
		Profile:     stored,
		Fingerprint: fingerprint,
		CreatedBy:   createdBy,
		CreatedAt:   storedNow(),
	}
	key := profileKey{tenantID: stored.TenantID, profileID: stored.ID}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.profiles[key]; exists {
		return ProfileRecord{}, fmt.Errorf(
			"%w: backend profile %q of tenant %q",
			tenant.ErrAlreadyExists, stored.ID, stored.TenantID)
	}
	if r.countLocked(scope.TenantID) >= MaxProfilesPerTenant {
		return ProfileRecord{}, fmt.Errorf(
			"%w: tenant %q may own %d", ErrProfileLimit, scope.TenantID, MaxProfilesPerTenant)
	}
	r.profiles[key] = record
	return record.clone(), nil
}

// GetProfile implements ProfileRepository.
func (r *MemoryProfileRepository) GetProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (ProfileRecord, error) {
	return r.lookup(ctx, scope, profileID)
}

// ListProfiles implements ProfileRepository.
func (r *MemoryProfileRepository) ListProfiles(
	ctx context.Context,
	scope tenant.TenantContext,
) ([]ProfileRecord, error) {
	if err := profileContextError(ctx); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	records := make([]ProfileRecord, 0, len(r.profiles))
	keys := make([]profileKey, 0, len(r.profiles))
	for key, record := range r.profiles {
		if key.tenantID != scope.TenantID {
			continue
		}
		records = append(records, record.clone())
		keys = append(keys, key)
	}
	r.mu.RUnlock()

	// Verified outside the lock, and every record before any is returned: a
	// list that answered with the good half of a damaged table would be read as
	// "that profile was never created".
	for i, record := range records {
		if err := record.Verify(keys[i].tenantID, keys[i].profileID); err != nil {
			return nil, err
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	return records, nil
}

// ResolveProfile implements ProfileSource.
//
// This is the method Router calls on every resolution, which is why it hands
// back the Profile alone: the provenance is control-plane data, and the data
// plane has no use for it.
func (r *MemoryProfileRepository) ResolveProfile(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (Profile, error) {
	record, err := r.lookup(ctx, scope, profileID)
	if err != nil {
		return Profile{}, err
	}
	return record.Profile, nil
}

// lookup is the read path both readers share, including the integrity check.
// It returns a deep copy, for the reason MemoryProfileSource.ResolveProfile
// documents: a caller that wrote through a returned pointer would make every
// later resolution of that id fail as ErrProfileChanged.
func (r *MemoryProfileRepository) lookup(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (ProfileRecord, error) {
	if err := profileContextError(ctx); err != nil {
		return ProfileRecord{}, err
	}
	if err := scope.Validate(); err != nil {
		return ProfileRecord{}, err
	}
	if err := tenant.ValidateResourceID("backend profile id", profileID); err != nil {
		return ProfileRecord{}, err
	}
	// Keyed by the caller's tenant rather than filtered after the lookup:
	// another tenant's profile is not found here, it is not reachable.
	r.mu.RLock()
	record, found := r.profiles[profileKey{tenantID: scope.TenantID, profileID: profileID}]
	r.mu.RUnlock()
	if !found {
		return ProfileRecord{}, fmt.Errorf("%w: %q", ErrProfileNotFound, profileID)
	}
	record = record.clone()
	// Nothing in this process can change a stored record, so this check can only
	// fail on a bug in this package. It runs anyway, and it runs here: the
	// PostgreSQL repository's rows can change underneath it, the two are one
	// interface with one conformance suite, and a check that only one of them
	// performs is a check the suite cannot pin.
	if err := record.Verify(scope.TenantID, profileID); err != nil {
		return ProfileRecord{}, err
	}
	return record, nil
}

func (r *MemoryProfileRepository) requireActiveTenant(ctx context.Context, tenantID string) error {
	owner, err := r.tenants.GetTenant(ctx, tenantID)
	if err != nil {
		return err
	}
	if owner.Status != tenant.StatusActive {
		return fmt.Errorf(
			"%w: tenant %q has status %q", tenant.ErrTenantInactive, tenantID, owner.Status)
	}
	return nil
}

// countLocked counts one tenant's profiles. The map is bounded by
// MaxProfilesPerTenant per tenant, so a scan is cheaper than an index that
// could disagree with what is actually stored.
func (r *MemoryProfileRepository) countLocked(tenantID string) int {
	count := 0
	for key := range r.profiles {
		if key.tenantID == tenantID {
			count++
		}
	}
	return count
}

// storedNow is the creation clock. It is UTC and truncated to microseconds so a
// record read back equals the record that was written, whichever repository
// stored it: PostgreSQL timestamptz keeps microseconds, and a Go time that kept
// nanoseconds would come back different.
func storedNow() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// profileContextError mirrors the repository convention: a nil context is a bad
// request rather than a panic, and a context that is already done is refused
// before anything is read or written.
func profileContextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", tenant.ErrInvalidArgument)
	}
	return ctx.Err()
}

// profileKey is the cache and storage key of this package: a profile id means
// nothing without the tenant that owns it.
type profileKey struct {
	tenantID  string
	profileID string
}

// String renders the key for a singleflight group. The separator is a NUL so
// no pair of ids can collide by concatenation; both halves are already
// constrained by tenant.ValidateResourceID, which excludes it.
func (k profileKey) String() string {
	return k.tenantID + "\x00" + k.profileID
}
