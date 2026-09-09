package storagebundle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Factory builds the Bundle a Profile describes.
//
// Build returns the Bundle together with the close that releases it, and the
// caller — in practice the Router — owns both from the moment they exist. On
// error it returns nothing to close: a Factory that failed halfway releases
// what it had built before it returns, because there is no second owner who
// could.
//
// ctx bounds the construction. It is the Router's own lifecycle context, not a
// request's: one caller going away must not cancel a build every other caller
// is waiting on.
type Factory interface {
	Build(ctx context.Context, profile Profile) (Bundle, func() error, error)
}

// ProcessConstraints is what the process's own storage arrangement forbids a
// tenant profile from doing.
//
// The two axes are the two invariants cmd/trpc-service/storage.go establishes
// at startup, carried to the one place that can otherwise reopen them. A
// per-tenant profile is a second storage decision made after the process-level
// one, and without these it could reintroduce exactly the combinations that
// file refuses to boot with.
type ProcessConstraints struct {
	// DurablePins reports whether this process's session directory survives a
	// restart. A durable session store under a non-durable directory would
	// keep the conversation and lose the revision it was pinned to.
	DurablePins bool

	// MultiWorker reports whether run leases are coordinated with other
	// Workers. An in-process session store under a shared lock is unshared
	// state behind a lock its peers cannot see anything through.
	MultiWorker bool
}

// SecretAuthorizer decides whether a tenant may name one credential reference.
//
// It is declared here, as the one question this package asks, rather than
// imported: a storage factory that depended on the security package's whole
// configuration would be a cycle waiting to happen, and this is the entire
// contract. *security.Entitlements satisfies it.
//
// Without it a Profile would be an unentitled channel to the process
// environment: the reference is written by whoever creates the profile, and
// resolving one that was never granted would hand a tenant a working
// connection to another tenant's database.
type SecretAuthorizer interface {
	AuthorizeSecretRef(tenantID string, ref string) error
}

// DefaultBuildTimeout bounds one dynamic build when FactoryOptions does not.
//
// It exists because the upstream session constructors do not take a context.
// The PostgreSQL one connects and creates tables while it is being built, on a
// context this process cannot cancel, so a database that accepts a TCP
// connection and then never answers would otherwise block the build forever —
// and with it the singleflight every other caller for that profile is waiting
// on, and Router.Close, which waits for builds to finish before it releases
// anything. Fifteen seconds is long enough for a cold connection plus table
// creation on a loaded server, and short enough that a wedged upstream is a
// failed request rather than a wedged process.
const DefaultBuildTimeout = 15 * time.Second

// FactoryOptions is everything NewSessionFactory needs to turn a Profile into
// a running session store.
//
// The seams for the upstream constructor and the two probes are unexported on
// purpose. They exist so this package's tests can exercise a build that blocks
// forever, one that fails halfway, and one that arrives after everyone stopped
// waiting — none of which a test can stage against a real database, and all of
// which are the failures that matter. Being unexported, they are not a
// supported way to substitute a backend from outside the package.
type FactoryOptions struct {
	// Constraints is what the process's own storage arrangement forbids.
	Constraints ProcessConstraints

	// Secrets decides whether a tenant may name the reference its profile
	// carries. Required: a factory without one could resolve any variable in
	// the environment for any tenant.
	Secrets SecretAuthorizer

	// Getenv reads the process environment. Required, and it must be the same
	// function the rest of the process was configured from — a factory reading
	// the real environment inside a test that configured a fake one would
	// resolve credentials nobody in that test granted.
	Getenv func(string) string

	// BuildTimeout bounds one build. Zero means DefaultBuildTimeout.
	BuildTimeout time.Duration

	// newSessions builds the upstream service. Defaults to sessionbackend.New.
	newSessions func(cfg sessionbackend.Config) (session.Service, error)

	// probePostgres checks that the target answers and takes the advisory lock
	// that serializes first builds against it. The returned release closes what
	// the probe holds; it is called exactly once, when the upstream constructor
	// returns, which is not necessarily before Build does.
	probePostgres func(ctx context.Context, dsn string, lockKey int64) (func() error, error)

	// probeRedis checks that the target answers. Redis needs no lock: nothing
	// upstream creates schema there.
	probeRedis func(ctx context.Context, url string) (func() error, error)
}

// NewSessionFactory returns the Factory a process builds tenant session stores
// with.
//
// It builds all three backends. What it will not do is decide anything about
// them: the process constraints, the entitlement table and the environment all
// arrive from outside, so a factory cannot be more permissive than the process
// that constructed it.
func NewSessionFactory(options FactoryOptions) (Factory, error) {
	if options.Secrets == nil {
		return nil, errors.New("storagebundle: session factory requires a secret authorizer")
	}
	if options.Getenv == nil {
		return nil, errors.New("storagebundle: session factory requires an environment lookup")
	}
	if options.BuildTimeout < 0 {
		return nil, errors.New("storagebundle: session factory build timeout must not be negative")
	}
	if options.BuildTimeout == 0 {
		options.BuildTimeout = DefaultBuildTimeout
	}
	if options.newSessions == nil {
		options.newSessions = sessionbackend.New
	}
	if options.probePostgres == nil {
		options.probePostgres = probePostgres
	}
	if options.probeRedis == nil {
		options.probeRedis = probeRedis
	}
	return sessionFactory{options: options}, nil
}

type sessionFactory struct {
	options FactoryOptions
}

// Build turns one Profile into a running session store.
//
// The order is the security boundary, and every step earns its place:
//
//  1. The context, then the shape, then the process constraints. All three are
//     free, none of them touches the environment, and a Router that is already
//     shutting down should not open a store nobody will ever reach.
//  2. The entitlement, before a single environment variable is read. A tenant
//     that may not name a reference must not be able to learn whether the
//     variable behind it is set, and the only way to guarantee that is to
//     refuse before looking.
//  3. The environment, then a parse this package performs itself. The driver's
//     own parse error quotes the string it failed on, so the value is checked
//     here and the driver's message is never wrapped.
//  4. The probe, which is where a build first touches the network — bounded,
//     abandonable, and for PostgreSQL holding the lock that serializes
//     concurrent first builds against one database.
//  5. The upstream constructor, which ignores the context it is given, so it
//     runs where it can be abandoned rather than waited on forever.
func (f sessionFactory) Build(
	ctx context.Context,
	profile Profile,
) (Bundle, func() error, error) {
	if ctx == nil {
		return Bundle{}, nil, errors.New("storagebundle: context is required")
	}
	// Checked before anything is constructed rather than after: a Router that
	// is already shutting down should not open a store nobody will ever reach.
	if err := ctx.Err(); err != nil {
		return Bundle{}, nil, err
	}
	// Re-validated rather than assumed. Router validates on the way in, but
	// this is the function that builds, and a Factory reached from anywhere
	// else has to refuse the same input — the upstream namespacing options
	// panic rather than return on input they dislike.
	if err := profile.Validate(); err != nil {
		return Bundle{}, nil, err
	}
	if err := f.allows(profile.Session.Backend); err != nil {
		return Bundle{}, nil, f.refuse(profile, err)
	}
	for _, ref := range profile.SecretRefs() {
		if err := f.options.Secrets.AuthorizeSecretRef(profile.TenantID, ref); err != nil {
			return Bundle{}, nil, f.refuse(profile, err)
		}
	}
	resolved, err := f.resolve(profile)
	if err != nil {
		return Bundle{}, nil, err
	}

	// One deadline over the probe and the construction together. Both are
	// abandonable, so this bounds what Build takes, not what the goroutines
	// behind it eventually do.
	buildCtx, cancel := context.WithTimeout(ctx, f.options.BuildTimeout)
	defer cancel()

	release, err := f.probe(buildCtx, profile, resolved)
	if err != nil {
		return Bundle{}, nil, err
	}
	// The release runs on whichever goroutine the constructor finishes on, and
	// only after it has finished. For PostgreSQL the release is the advisory
	// lock that serializes first builds against one database, and the
	// constructor is the thing that creates the tables the lock protects: a
	// Build that stopped waiting and gave the lock back would leave a second
	// Worker free to run CREATE TABLE IF NOT EXISTS underneath a constructor
	// that is still running, which is the race the lock exists to prevent.
	//
	// So it is bound to the constructor rather than to this call. When the
	// build is waited for, this is the same order as before — construct, then
	// release, then report both. When it is abandoned, the lock stays held by
	// the goroutine that is still creating the tables, and is given back the
	// moment that goroutine is done.
	outcome, buildErr := awaitOrAbandon(
		buildCtx,
		func() (built, error) {
			service, err := f.options.newSessions(resolved.config)
			return built{service: service, releaseErr: release()}, err
		},
		// The service that arrives after Build gave up has no owner. Nobody
		// will ever call the close that was never returned, so the goroutine
		// that produced it closes it.
		discardLateBuild,
	)
	// The release failure is part of the result: for PostgreSQL the release is
	// the advisory lock, and a process that reported success while still
	// holding it would block every other process's first build against that
	// database. An abandoned build reports no release failure because its
	// release has not happened yet — the deadline it returns is the failure.
	releaseErr := f.hide(outcome.releaseErr, resolved)
	switch {
	case buildErr != nil:
		return Bundle{}, nil, errors.Join(f.buildFailed(profile, buildErr, resolved), releaseErr)
	case releaseErr != nil:
		// The store was built and this call owns it, because it is about to
		// return no close for it.
		return Bundle{}, nil, errors.Join(
			releaseErr, f.hide(closeService(outcome.service), resolved))
	case outcome.service == nil:
		return Bundle{}, nil, fmt.Errorf(
			"storagebundle: backend profile %q of tenant %q: session backend returned no store",
			profile.ID, profile.TenantID)
	}
	return Bundle{Session: outcome.service}, outcome.service.Close, nil
}

// built is one finished construction: the store, and how giving the probe back
// went.
//
// The two travel together because they happen on one goroutine and belong to
// one owner. Whoever receives this value either returns the store to a caller
// or closes it, and in both cases the probe behind it has already been
// released — see Build.
type built struct {
	service    session.Service
	releaseErr error
}

// resolved is a Profile turned into the arguments a build needs: nothing in it
// may be logged.
type resolved struct {
	config sessionbackend.Config
	// lockKey serializes first builds against one PostgreSQL target. It is
	// zero for every other backend.
	lockKey int64
}

// resolve reads the referenced credential and turns the Profile into an
// upstream config.
//
// The parse this performs is not a duplicate of the driver's. It is the only
// one whose failure a caller ever sees: pgx and go-redis quote the string they
// could not parse, so their error is checked, discarded, and replaced with one
// that names the variable and the expected shape.
func (f sessionFactory) resolve(profile Profile) (resolved, error) {
	switch profile.Session.Backend {
	case sessionbackend.BackendInMemory:
		return resolved{config: sessionbackend.Config{Backend: sessionbackend.BackendInMemory}}, nil

	case sessionbackend.BackendPostgres:
		spec := profile.Session.Postgres
		dsn, envName, err := f.lookupSecret(profile, spec.DSNRef)
		if err != nil {
			return resolved{}, err
		}
		target, err := parsePostgresTarget(dsn)
		if err != nil {
			return resolved{}, f.unusable(profile, envName, "a PostgreSQL connection string")
		}
		config := sessionbackend.Config{
			Backend: sessionbackend.BackendPostgres,
			Postgres: sessionbackend.PostgresConfig{
				DSN:         dsn,
				Schema:      spec.Schema,
				TablePrefix: spec.TablePrefix,
			},
		}
		if err := config.Validate(); err != nil {
			return resolved{}, fmt.Errorf("%w: %w", ErrInvalidProfile, err)
		}
		return resolved{
			config:  config,
			lockKey: advisoryLockKey(target, spec.Schema, spec.TablePrefix),
		}, nil

	case sessionbackend.BackendRedis:
		spec := profile.Session.Redis
		url, envName, err := f.lookupSecret(profile, spec.URLRef)
		if err != nil {
			return resolved{}, err
		}
		if err := checkRedisURL(url); err != nil {
			return resolved{}, f.unusable(profile, envName, "a Redis connection URL")
		}
		config := sessionbackend.Config{
			Backend: sessionbackend.BackendRedis,
			Redis: sessionbackend.RedisConfig{
				URL:       url,
				KeyPrefix: spec.KeyPrefix,
			},
		}
		if err := config.Validate(); err != nil {
			return resolved{}, fmt.Errorf("%w: %w", ErrInvalidProfile, err)
		}
		return resolved{config: config}, nil

	default:
		// Unreachable: Validate and allows have both rejected everything else.
		return resolved{}, f.refuse(profile,
			fmt.Errorf("%w: %q", ErrUnsupportedBackend, string(profile.Session.Backend)))
	}
}

// probe checks that the target answers before the upstream constructor is
// allowed to touch it, and holds whatever has to be held while it does.
//
// Two things are bought here. A backend that is simply unreachable fails as a
// bounded, cancellable probe instead of inside a constructor that cannot be
// cancelled at all. And for PostgreSQL the probe connection carries the
// advisory lock that makes concurrent first builds safe: upstream creates its
// tables with CREATE TABLE IF NOT EXISTS, which races against itself on the
// system catalogue's unique index rather than merging politely.
func (f sessionFactory) probe(
	ctx context.Context,
	profile Profile,
	target resolved,
) (func() error, error) {
	var (
		release func() error
		err     error
	)
	switch target.config.Backend {
	case sessionbackend.BackendPostgres:
		dsn := target.config.Postgres.DSN
		release, err = awaitOrAbandon(
			ctx,
			func() (func() error, error) {
				return f.options.probePostgres(ctx, dsn, target.lockKey)
			},
			releaseLateProbe,
		)
		err = hideValue(err, dsn)
	case sessionbackend.BackendRedis:
		url := target.config.Redis.URL
		release, err = awaitOrAbandon(
			ctx,
			func() (func() error, error) { return f.options.probeRedis(ctx, url) },
			releaseLateProbe,
		)
		err = hideValue(err, url)
	default:
		return noRelease, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"storagebundle: backend profile %q of tenant %q: session store is unreachable: %w",
			profile.ID, profile.TenantID, err)
	}
	if release == nil {
		return noRelease, nil
	}
	return release, nil
}

// lookupSecret resolves one reference to its value, and returns the variable
// name beside it so an error can name the variable without naming the value.
func (f sessionFactory) lookupSecret(profile Profile, ref string) (string, string, error) {
	envName, err := secretref.EnvName(ref)
	if err != nil {
		// Unreachable through Validate, which checked the same rule.
		return "", "", fmt.Errorf("%w: %w", ErrInvalidProfile, err)
	}
	value := f.options.Getenv(envName)
	if strings.TrimSpace(value) == "" {
		return "", "", fmt.Errorf(
			"storagebundle: backend profile %q of tenant %q: environment variable %s is not set",
			profile.ID, profile.TenantID, envName)
	}
	return value, envName, nil
}

// unusable reports a credential that is present but is not what it should be.
//
// The wording is fixed and the value never appears, not even in part. This is
// the error a mistyped DSN produces, which is exactly the case where the value
// is most likely to be a real credential with one character wrong.
func (f sessionFactory) unusable(profile Profile, envName, want string) error {
	return fmt.Errorf(
		"storagebundle: backend profile %q of tenant %q: environment variable %s does not hold %s",
		profile.ID, profile.TenantID, envName, want)
}

// refuse wraps a refusal that is about this profile rather than about the
// world it points at.
func (f sessionFactory) refuse(profile Profile, err error) error {
	return fmt.Errorf(
		"storagebundle: backend profile %q of tenant %q: %w",
		profile.ID, profile.TenantID, err)
}

// buildFailed wraps a failure from the upstream constructor, hidden against the
// connection string it was built from. Upstream redacts its own errors; this
// redacts again because the failure may also be a context deadline this package
// produced, and the two paths must not differ in what they may disclose.
func (f sessionFactory) buildFailed(profile Profile, err error, target resolved) error {
	return f.hide(fmt.Errorf(
		"storagebundle: build session store for backend profile %q of tenant %q: %w",
		profile.ID, profile.TenantID, err), target)
}

// hide removes the credential this build resolved from an error, whichever
// backend it belongs to.
//
// It is the last thing every error path after resolve goes through, because
// that is where this package stops holding a reference and starts holding the
// value itself: the probe, the constructor, the release and the close all have
// the real connection string in hand, and all four of their errors are handed
// to a caller that was only ever entitled to name a variable.
func (f sessionFactory) hide(err error, target resolved) error {
	switch target.config.Backend {
	case sessionbackend.BackendPostgres:
		return hideValue(err, target.config.Postgres.DSN)
	case sessionbackend.BackendRedis:
		return hideValue(err, target.config.Redis.URL)
	default:
		return err
	}
}

// redactedCredential replaces a credential that reached an error message. It is
// spelled the way sessionbackend spells it, so one message cannot show two
// different ways of saying that something was removed.
const redactedCredential = "[REDACTED]"

// hideValue removes a resolved connection value from an error, whole and then
// in fragments.
//
// It is stricter than sessionbackend.Scrub rather than a substitute for it, and
// the difference is the audience. Scrub keeps the user, host and database on
// purpose: the errors it guards belong to the process's own storage, and they
// go to the operator who wrote that DSN. These errors do not. The value behind
// a tenant profile was resolved from a variable the tenant merely named, and
// the error travels back out through the Admin API — so what an operator needs
// to read is exactly what this caller may not be shown.
//
// The two passes are not redundant, and the order is not arbitrary.
// sessionbackend.Scrub redacts what it can extract from a connection string,
// which is the password — so a target that has no password has nothing for it
// to extract, and an upstream that echoed the whole DSN back would come through
// it untouched, host, user, database and all. The whole-value pass covers that,
// and it has to run first: once Scrub has replaced the password inside the
// value, the value no longer occurs verbatim and everything around it would
// survive. Scrub then runs for the spellings the whole-value pass cannot match,
// which are the ones a driver mangles — a percent-decoded password, or one
// quoted back as a port.
//
// The unwrap chain is cut the moment either pass substitutes anything: an error
// that still wrapped the original would hand the value straight back to
// whoever unwrapped it, which is the one thing this exists to prevent. Nothing
// is cut when there was nothing to remove, so the sentinels a caller matches on
// — a context deadline, a refused entitlement, an unreachable host — survive
// every error that never carried the credential in the first place.
func hideValue(err error, value string) error {
	if err == nil {
		return nil
	}
	if value != "" {
		message := err.Error()
		if hidden := strings.ReplaceAll(message, value, redactedCredential); hidden != message {
			err = errors.New(hidden)
		}
	}
	return sessionbackend.Scrub(err, value)
}

// allows reports whether this process may serve a tenant profile on this
// backend.
//
// The refusals are the process's own arrangement, not a capability check: a
// durable store in a process whose session directory does not survive a restart
// would keep the conversation and lose the revision it was pinned to, and an
// in-process store in a multi-worker process is unshared state behind a lock
// its peers cannot see anything through.
func (f sessionFactory) allows(backend sessionbackend.Backend) error {
	switch backend {
	case sessionbackend.BackendInMemory:
		if f.options.Constraints.MultiWorker {
			return ErrNotSharedAcrossWorkers
		}
		return nil
	case sessionbackend.BackendPostgres, sessionbackend.BackendRedis:
		if !f.options.Constraints.DurablePins {
			return ErrPinsNotDurable
		}
		return nil
	default:
		// Unreachable through Build, which validates first. It stays because
		// this method is the whole answer to "may this backend be built here",
		// and an unknown backend is not one of them.
		return fmt.Errorf("%w: %q", ErrUnsupportedBackend, string(backend))
	}
}

// awaitOrAbandon runs work on its own goroutine and stops waiting for it when
// ctx ends.
//
// It exists because the work it is given cannot be cancelled. The upstream
// session constructors take no context, and a driver that has connected but
// gone quiet leaves them blocked on a read with no deadline; waiting for one of
// those would block the caller, the singleflight behind it and, through it,
// Router.Close.
//
// Abandoning is not forgetting, and it costs no second goroutine: the worker
// finds out for itself that nobody is waiting any more and hands its own result
// to discard, which is what closes a store or releases a connection that
// arrived after everyone stopped waiting. Exactly one of the two ever takes the
// outcome, because the handover is decided under a mutex — so a late result is
// cleaned up once and never twice, and a result that lands in the moment
// between the deadline and the giving up is returned to the caller rather than
// closed behind its back.
//
// What cannot be promised is when. A work that never returns leaves its own
// goroutine parked for the life of the process, and that is the only one this
// leaves behind: the price of an upstream constructor that cannot be cancelled,
// and still better than parking the caller too.
//
// discard is called with whatever work produced, including the zero value of a
// work that failed, so it has to tolerate one.
func awaitOrAbandon[T any](
	ctx context.Context,
	work func() (T, error),
	discard func(T),
) (T, error) {
	type outcome struct {
		value T
		err   error
	}
	var (
		mu        sync.Mutex
		abandoned bool
	)
	// Buffered and never closed. The worker sends at most once and only when it
	// has found a waiter, so the send cannot block the goroutine that has
	// cleanup left to do, and no receive can race a close.
	results := make(chan outcome, 1)
	go func() {
		value, err := work()
		mu.Lock()
		if !abandoned {
			results <- outcome{value: value, err: err}
			mu.Unlock()
			return
		}
		mu.Unlock()
		// Nobody is waiting, so this goroutine owns what it just produced.
		discard(value)
	}()

	select {
	case <-ctx.Done():
		mu.Lock()
		// The result may have arrived between ctx ending and this lock. It was
		// produced for this caller, and taking it here is what keeps it from
		// being both delivered and discarded.
		select {
		case result := <-results:
			mu.Unlock()
			return result.value, result.err
		default:
		}
		abandoned = true
		mu.Unlock()
		var none T
		return none, ctx.Err()
	case result := <-results:
		return result.value, result.err
	}
}

// discardLateBuild closes a store that arrived after Build gave up. Whatever
// the probe held was already given back by the goroutine that built it.
func discardLateBuild(late built) {
	_ = closeService(late.service)
}

func closeService(service session.Service) error {
	if service == nil {
		return nil
	}
	return service.Close()
}

// releaseLateProbe releases a probe that arrived after Build gave up. For
// PostgreSQL this is what gives the advisory lock back.
func releaseLateProbe(release func() error) {
	if release != nil {
		_ = release()
	}
}

// noRelease is the release of a backend that holds nothing while it is built.
func noRelease() error { return nil }
