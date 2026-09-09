// Package storagebundle decides which storage a Runtime talks to.
//
// One revision names a backend profile; this package turns that name into the
// set of upstream services the Runtime runs on, and keeps that set alive for
// exactly as long as some Runtime holds it. The pieces are deliberately small
// and separate:
//
//   - A Profile is an immutable description: which backend, and which
//     references name its credentials. It never carries a DSN, a URL or a key,
//     and its id is its version — content that changes must change id.
//   - A ProfileSource resolves a profile id, within one tenant, to a Profile.
//   - A Factory turns a Profile into a Bundle and the close that releases it.
//   - A Router caches Bundles per (tenant, profile id), builds each at most
//     once, and hands out Leases.
//   - A Lease is one holder's claim on a Bundle. Ownership is in the type: a
//     borrowed lease releases nothing, an owned one closes exactly once, and a
//     Router lease is a reference count. There is no ownership boolean and no
//     close hook to pass around.
//
// Two things are concentrated here rather than spread across the callers.
// Profiles are stored through one interface, ProfileRepository, whose in-memory
// implementation lives in this package and whose PostgreSQL one lives beside
// the tenant tables it is gated by — a profile cannot exist without a tenant,
// and the foreign key is where that is said. And a credential reference becomes
// a connection string in exactly one place, the Factory: the reference is
// checked against the tenant's entitlements before the environment is read, and
// the value never leaves the build — not in a Profile, not in a Bundle, and not
// in an error.
//
// What this package does not do, on purpose: it evicts nothing. A profile that
// is built stays built until the Router is closed.
package storagebundle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var (
	// ErrProfileNotFound reports that this tenant has no such profile. A
	// profile belonging to another tenant is reported the same way, so a
	// caller cannot probe for the existence of a profile it may not use.
	ErrProfileNotFound = errors.New("storagebundle: backend profile not found")

	// ErrProfileChanged reports that a profile id resolved to different
	// content than the content its Bundle was built from. The id is the
	// version, so this is a violated contract in the source, not a request the
	// caller can fix — the Bundle in hand may already be the wrong storage for
	// this profile, so nothing is rebuilt and nothing is evicted.
	ErrProfileChanged = errors.New(
		"storagebundle: backend profile content changed under an immutable id")

	// ErrInvalidProfile reports a profile that cannot be built as described.
	ErrInvalidProfile = errors.New("storagebundle: invalid backend profile")

	// ErrIncompleteBundle reports a Bundle with nothing in it. It exists so a
	// Resolver that hands back an empty Bundle fails here rather than as a nil
	// dereference inside a Runner.
	ErrIncompleteBundle = errors.New("storagebundle: bundle has no session service")

	// ErrRouterClosed reports a Resolve on a Router that is shutting down.
	ErrRouterClosed = errors.New("storagebundle: router is closed")

	// ErrUnsupportedBackend reports a backend this Factory cannot build.
	ErrUnsupportedBackend = errors.New(
		"storagebundle: backend is not buildable by this factory")

	// ErrPinsNotDurable reports a durable session backend in a process whose
	// session directory is not durable. The session would survive a restart
	// and its revision pin would not, so the conversation would silently
	// resume on whatever revision is published then — the exact failure the
	// pin exists to prevent.
	ErrPinsNotDurable = errors.New(
		"storagebundle: a durable session backend needs a durable session directory")

	// ErrNotSharedAcrossWorkers reports an in-process session backend in a
	// process that coordinates run leases with other Workers. A shared lock
	// over unshared state is not safety, it only looks like it.
	ErrNotSharedAcrossWorkers = errors.New(
		"storagebundle: an in-process session backend cannot serve several workers")
)

// Bundle is the set of upstream services one profile provides.
//
// It holds only what the platform actually consumes today. Memory, artifact
// and knowledge services are not reserved as nil fields: a nil field has no
// consumer and would only introduce a "is this the default or is it
// unconfigured" branch at every use. Adding a named field later is not a
// breaking change.
type Bundle struct {
	Session session.Service
}

// Validate reports whether this Bundle can serve a Runtime.
func (b Bundle) Validate() error {
	if b.Session == nil {
		return ErrIncompleteBundle
	}
	return nil
}

// Lease is one holder's claim on a Bundle.
//
// Release is idempotent and the Bundle must not be used after it. What Release
// actually does is the whole point of the type: see Borrow, Own and Router.
type Lease interface {
	Bundle() Bundle
	Release() error
}

// Resolver maps a revision's backend profile id onto the Bundle it names.
//
// The empty profile id means "this process's default store". It is mapped
// inside a Resolver and nowhere else: no caller may treat "" as a magic value
// of its own, because whether a process even has a default is the Resolver's
// business.
type Resolver interface {
	Resolve(ctx context.Context, scope tenant.TenantContext, profileID string) (Lease, error)
}

// Borrow returns a Lease over a Bundle somebody else owns. Release does
// nothing, however often it is called.
func Borrow(b Bundle) Lease {
	return &borrowedLease{bundle: b}
}

type borrowedLease struct {
	bundle Bundle
}

func (l *borrowedLease) Bundle() Bundle {
	return l.bundle
}

func (l *borrowedLease) Release() error {
	return nil
}

// Own returns a Lease that closes the Bundle. Release closes b.Session exactly
// once and every call returns the error of that one close.
//
// An owned lease has exactly one holder by construction. Handing the same one
// to two Runtimes would let the first Close pull the store out from under the
// second, which is why no Resolver in this package hands one out: Fixed serves
// borrowed leases, and a caller that owns a store passes the Lease itself.
func Own(b Bundle) Lease {
	return &ownedLease{bundle: b}
}

type ownedLease struct {
	bundle   Bundle
	once     sync.Once
	closeErr error
}

func (l *ownedLease) Bundle() Bundle {
	return l.bundle
}

func (l *ownedLease) Release() error {
	l.once.Do(func() {
		if l.bundle.Session != nil {
			l.closeErr = l.bundle.Session.Close()
		}
	})
	return l.closeErr
}

// Fixed returns a Resolver over one Bundle the caller owns.
//
// It is the compatibility path for callers that already hold a process session
// service and have no way to build another one. Every Resolve of the empty
// profile id returns a fresh borrowed lease, so repeated resolution is safe and
// releasing one holder's lease cannot close the store under another's.
//
// A non-empty profile id is refused. That is the point of it: this Resolver
// cannot honour a profile reference, and honouring it by ignoring it is how a
// revision ends up served by storage it did not ask for.
//
// The scope is checked the way Router checks it, even though this Resolver has
// nothing to look a tenant up in. Resolver is a tenant-scoped interface, so an
// unusable scope is a bad request at any implementation of it; a Fixed that
// answered where a Router would refuse would make "which storage does this
// revision get" depend on which Resolver the process happened to be wired with.
func Fixed(b Bundle) Resolver {
	return fixedResolver{bundle: b}
}

type fixedResolver struct {
	bundle Bundle
}

func (f fixedResolver) Resolve(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (Lease, error) {
	if ctx == nil {
		return nil, errors.New("storagebundle: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Before either branch, in Router's order. Every caller today validates the
	// scope before it reaches here — planRuntime does it three steps earlier —
	// which is the reason to repeat it rather than a reason to skip it: this is
	// the function that hands out storage, so it is the function that has to be
	// right on its own.
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if profileID == "" {
		if err := f.bundle.Validate(); err != nil {
			return nil, err
		}
		return Borrow(f.bundle), nil
	}
	// An id that could never name a profile is a bad request, and it is refused
	// as one before it reaches an error message. Reporting it as "not found"
	// would claim a lookup happened, and would put an unchecked string in the
	// error of a resolver that never examined it.
	if err := tenant.ValidateResourceID("backend profile id", profileID); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("%w: %q", ErrProfileNotFound, profileID)
}
