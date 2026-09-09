package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	sessiondirpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	redislease "github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// This file decides where one process keeps its state.
//
// # One profile, not three switches
//
// The three things a worker stores — the control plane, the session revision
// pin, and the conversation history — are a set, not three independent
// choices. A pin that survives a restart is worthless if the revision it names
// was lost with the in-memory control plane, and a persistent session whose pin
// is forgotten resumes on whatever revision is published now, which is the
// exact failure the pin exists to prevent. So there is one process-level
// profile and it moves all three together. Per-component switches would let an
// operator configure combinations that are known to be broken.
//
// # What it is not
//
// This is not a backend registry and it does not generalise. Two profiles are
// spelled out; a third means editing this file, which is the point. Redis is
// not a profile here: sessionbackend can build a Redis session service, but
// there is no Redis control-plane repository or session directory to pair it
// with, so offering it would offer exactly the broken combination above.
//
// # Coordination is a second axis, and the two constrain each other
//
// Where state lives and which Worker may currently run a Session are different
// questions, so run-lease coordination is configured separately. They are not
// independent, though, and the combinations are checked rather than left to the
// operator: a lock is only worth anything over state the lock's peers can all
// see. Coordinating through Redis while every Worker keeps its sessions in its
// own memory would be a shared lock over unshared state — a process that looks
// coordinated, passes a smoke test, and protects nothing. It is refused at
// startup.
const (
	// storageProfileEnvVar selects the profile. Unset or empty means inmemory,
	// so the demo still runs against an empty machine.
	storageProfileEnvVar = "TRPC_SERVICE_STORAGE_PROFILE"

	// postgresDSNEnvVar carries the connection string every component of the
	// postgres profile shares. It holds a password: it is never logged, and
	// every error built from it goes through sessionbackend.Scrub.
	postgresDSNEnvVar = "TRPC_SERVICE_POSTGRES_DSN"

	// postgresSchemaEnvVar places the tables. The schema must already exist —
	// nothing here creates one — and both this process's migrations and the
	// upstream session tables land in it. Optional; empty means the server's
	// default search_path, normally "public".
	postgresSchemaEnvVar = "TRPC_SERVICE_POSTGRES_SCHEMA"

	// coordinationEnvVar selects the run-lease backend. Unset or empty means
	// inmemory, which coordinates this process and nothing else, and which
	// reaches no network.
	coordinationEnvVar = "TRPC_SERVICE_SESSION_COORDINATION"

	// redisURLEnvVar carries the connection URL of the redis coordination
	// backend. Like the DSN it may hold a password: it is never logged, and
	// every error built from it goes through sessionbackend.Scrub.
	redisURLEnvVar = "TRPC_SERVICE_REDIS_URL"
)

// storageProfile names one whole storage arrangement.
type storageProfile string

const (
	// profileInMemory keeps everything in process memory. It needs no external
	// service and makes no network call, and every tenant, pin and conversation
	// is lost when the process exits.
	profileInMemory storageProfile = "inmemory"

	// profilePostgres puts the control plane, the session directory and the
	// upstream session store in one PostgreSQL database, on one DSN and one
	// schema.
	profilePostgres storageProfile = "postgres"
)

// coordinationBackend names where run leases are coordinated.
type coordinationBackend string

const (
	// coordinationInMemory coordinates the Workers inside this process and
	// nobody else. It needs no external service and makes no network call.
	coordinationInMemory coordinationBackend = "inmemory"

	// coordinationRedis coordinates Workers across processes through a shared
	// Redis instance.
	coordinationRedis coordinationBackend = "redis"
)

// errStorageConfig is the sentinel behind every startup refusal this file
// reports, so a caller can tell "you configured this wrong" from "the database
// was unreachable".
var errStorageConfig = errors.New("storage: invalid configuration")

// storageConfig is the whole storage configuration of one process.
type storageConfig struct {
	// dsn and schema are populated only under profilePostgres, and redisURL
	// only under coordinationRedis; see loadStorageConfig.
	profile      storageProfile
	dsn          string
	schema       string
	coordination coordinationBackend
	redisURL     string
}

// loadStorageConfig reads the environment and refuses anything it cannot serve,
// before the caller owns a single resource.
//
// getenv is a parameter rather than os.Getenv so a test can hand over an
// environment without touching the process's own.
//
// The PostgreSQL variables are read only under the profile that uses them. That
// is what "inmemory ignores stray PostgreSQL settings" means here: a DSN left
// in the environment from an integration run cannot alter what an inmemory
// process does, and — just as important — presence of a DSN never selects a
// profile. Storage that reaches the network is something an operator asks for
// explicitly.
func loadStorageConfig(getenv func(string) string) (storageConfig, error) {
	profile, err := parseStorageProfile(getenv(storageProfileEnvVar))
	if err != nil {
		return storageConfig{}, err
	}
	coordination, err := parseCoordinationBackend(getenv(coordinationEnvVar))
	if err != nil {
		return storageConfig{}, err
	}
	cfg := storageConfig{profile: profile, coordination: coordination}
	if profile == profilePostgres {
		cfg.dsn = getenv(postgresDSNEnvVar)
		cfg.schema = getenv(postgresSchemaEnvVar)
	}
	if coordination == coordinationRedis {
		cfg.redisURL = getenv(redisURLEnvVar)
	}
	if err := cfg.validate(); err != nil {
		return storageConfig{}, err
	}
	return cfg, nil
}

// parseStorageProfile maps the environment value to a profile.
//
// The match is exact: no trimming and no case folding. A profile decides
// whether this process writes to a shared database, so "Postgres", "pg" and a
// value with a stray trailing space are refused rather than guessed at. Being
// generous here means an operator who mistyped the profile silently gets the
// other one.
func parseStorageProfile(value string) (storageProfile, error) {
	switch storageProfile(value) {
	case "":
		return profileInMemory, nil
	case profileInMemory, profilePostgres:
		return storageProfile(value), nil
	default:
		return "", unknownProfileError(value)
	}
}

// unknownProfileError is shared so an operator sees the same refusal whether
// the profile came from the environment or from a config built in code.
func unknownProfileError(value string) error {
	return fmt.Errorf(
		"%w: unknown %s %q (want %q or %q; leave it unset for %q)",
		errStorageConfig, storageProfileEnvVar, value,
		profileInMemory, profilePostgres, profileInMemory,
	)
}

// parseCoordinationBackend maps the environment value to a coordination
// backend. The match is exact, for the same reason parseStorageProfile's is:
// an operator who mistyped "redis" would otherwise get a process that silently
// coordinates nothing while several Workers share one Session.
func parseCoordinationBackend(value string) (coordinationBackend, error) {
	switch coordinationBackend(value) {
	case "":
		return coordinationInMemory, nil
	case coordinationInMemory, coordinationRedis:
		return coordinationBackend(value), nil
	default:
		return "", unknownCoordinationError(value)
	}
}

func unknownCoordinationError(value string) error {
	return fmt.Errorf(
		"%w: unknown %s %q (want %q or %q; leave it unset for %q)",
		errStorageConfig, coordinationEnvVar, value,
		coordinationInMemory, coordinationRedis, coordinationInMemory,
	)
}

// validate reports whether this configuration can be opened. It contacts
// nothing: a configuration that validates can still fail to connect.
//
// Both axes are checked here, before openStorage creates anything, so a process
// that is going to be refused is refused without having opened a pool, dialled
// Redis or created an upstream table.
func (c storageConfig) validate() error {
	if err := c.validateProfile(); err != nil {
		return err
	}
	return c.validateCoordination()
}

func (c storageConfig) validateProfile() error {
	switch c.profile {
	case profileInMemory:
		return nil
	case profilePostgres:
		// Checked below.
	default:
		// Only a config built in code reaches this: loadStorageConfig cannot
		// produce another value, and an unset variable is normalised to
		// profileInMemory there rather than here. It is still a refusal and not
		// a fallback, because openStorage validates before it constructs and an
		// unrecognised profile has to fail there too. Treating it as "not
		// postgres, so in-memory" would be the fail-open this whole file exists
		// to prevent: a process that looks healthy and loses every conversation
		// on restart.
		return unknownProfileError(string(c.profile))
	}
	// Named explicitly, because "postgres backend requires a DSN" from the
	// layer below does not tell an operator which variable to set.
	if strings.TrimSpace(c.dsn) == "" {
		return fmt.Errorf(
			"%w: profile %q requires %s to be set",
			errStorageConfig, profilePostgres, postgresDSNEnvVar,
		)
	}
	// The schema rules belong to sessionbackend, which has to enforce them
	// anyway: the upstream schema option panics on input it dislikes instead of
	// returning an error. Restating them here would leave two copies to drift.
	// These messages quote the schema and never the DSN.
	if err := c.sessionConfig().Validate(); err != nil {
		return fmt.Errorf("%w: %s: %w", errStorageConfig, postgresSchemaEnvVar, err)
	}
	return nil
}

// validateCoordination checks the run-lease axis and, more importantly, checks
// it against the storage axis.
func (c storageConfig) validateCoordination() error {
	switch c.coordination {
	case coordinationInMemory:
		// Legal under either profile. Under postgres it means "one Worker", and
		// that is a real deployment: a single process on a persistent store.
		return nil
	case coordinationRedis:
		// Checked below.
	default:
		// Reached only by a config built in code; loadStorageConfig normalises
		// an unset variable to coordinationInMemory. It is a refusal rather
		// than a fallback for the same reason the profile's is: openStorage
		// validates before it constructs, and an unrecognised value must fail
		// there too.
		return unknownCoordinationError(string(c.coordination))
	}
	if c.profile != profilePostgres {
		return fmt.Errorf(
			"%w: %s=%q needs a Session store every Worker can see, but %s=%q keeps "+
				"sessions in this process's memory; a shared lock over unshared "+
				"state is not safety, it only looks like it",
			errStorageConfig, coordinationEnvVar, coordinationRedis,
			storageProfileEnvVar, c.profile,
		)
	}
	if strings.TrimSpace(c.redisURL) == "" {
		return fmt.Errorf(
			"%w: %s=%q requires %s to be set",
			errStorageConfig, coordinationEnvVar, coordinationRedis, redisURLEnvVar,
		)
	}
	// Parsing is configuration, not connection: goredis.ParseURL resolves
	// nothing and dials nothing. Doing it here means a mistyped URL is refused
	// before a pool exists, and the failure — which quotes the URL it could not
	// parse — is scrubbed like every other error built from a secret.
	//
	// Scrubbing comes first and the sentinel is attached afterwards, because
	// Scrub deliberately drops the chain it was handed: redacting the finished
	// error would take errStorageConfig with it and leave the caller unable to
	// tell a misconfiguration from an unreachable server.
	if _, err := goredis.ParseURL(c.redisURL); err != nil {
		safe := sessionbackend.Scrub(
			fmt.Errorf("parse %s: %w", redisURLEnvVar, err), c.redisURL)
		return fmt.Errorf("%w: %w", errStorageConfig, safe)
	}
	return nil
}

// sessionConfig renders the upstream session backend this profile implies. The
// session store shares the profile's DSN and schema; nothing configures it
// separately.
func (c storageConfig) sessionConfig() sessionbackend.Config {
	if c.profile != profilePostgres {
		return sessionbackend.DefaultConfig()
	}
	return sessionbackend.Config{
		Backend: sessionbackend.BackendPostgres,
		Postgres: sessionbackend.PostgresConfig{
			DSN:    c.dsn,
			Schema: c.schema,
		},
	}
}

// describe renders the configuration for the startup log: the profile, the
// coordination backend, whether each connection string is present, and the
// schema. Never the contents of a connection string.
func (c storageConfig) describe() string {
	fields := []string{fmt.Sprintf("profile=%s", c.profile)}
	if c.profile == profilePostgres {
		fields = append(fields,
			fmt.Sprintf("dsn=%s", presence(c.dsn)),
			fmt.Sprintf("schema=%q", c.schema),
		)
	}
	fields = append(fields, fmt.Sprintf("coordination=%s", c.coordination))
	if c.coordination == coordinationRedis {
		fields = append(fields, fmt.Sprintf("redis_url=%s", presence(c.redisURL)))
	}
	return strings.Join(fields, " ")
}

func presence(value string) string {
	if strings.TrimSpace(value) == "" {
		return "absent"
	}
	return "set"
}

// namedCloser is one resource the stack owns, kept with the name an operator
// needs to make sense of a close failure.
type namedCloser struct {
	name  string
	close func() error
}

// storageStack is what one process runs on: the three components, plus the
// resources behind them.
//
// It exists so ownership has one home. Under the postgres profile the
// repository and the directory share a pool that neither of them owns — both
// packages borrow the pool they are handed and close nothing — while the
// upstream session service owns a pool of its own, internally. Whoever built
// those has to close them, in the right order, on the shutdown path and on
// every partial startup failure alike. That is this type's whole job.
type storageStack struct {
	repository tenant.Repository
	// profiles is the control plane's backend-profile storage, and it is the
	// same value on both ends of this process: the Admin API creates profiles
	// through it and the Router resolves them through it, as a ProfileSource.
	// Two values built over one database would be one bug away from a Router
	// that cannot see what the Admin API just accepted.
	//
	// It owns nothing. Under the postgres profile it borrows the shared pool
	// like the repository does; under the in-memory profile it borrows the
	// repository itself as its tenant gate.
	profiles    storagebundle.ProfileRepository
	directory   sessiondir.Directory
	sessions    session.Service
	coordinator sessionlease.Coordinator

	// pool is the shared control-plane pool under the postgres profile, and nil
	// under every other one. It is exposed so a component built after the stack
	// can read and write through the same connections the repository and the
	// directory use, rather than opening a second pool over the same database.
	// Borrowing it comes with the rule the other borrowers follow: whoever takes
	// it closes nothing, because the stack owns it.
	pool *pgxpool.Pool

	// connString and redisURL are kept so close errors can be scrubbed too. A
	// pool reports a failure by echoing the string it was built from, and a
	// close on the shutdown path is logged like any other error.
	connString string
	redisURL   string

	closers []namedCloser
	closed  bool
}

// push records a resource to release. Order is the order of creation: close
// undoes it.
func (s *storageStack) push(name string, closeFn func() error) {
	s.closers = append(s.closers, namedCloser{name: name, close: closeFn})
}

// close releases everything the stack owns, newest first, and reports every
// failure rather than the first one.
//
// Reverse order is not decoration. The session service holds its own pool and
// may still be flushing; the shared pool must outlive the repository and
// directory that read through it. Closing in creation order would close a pool
// out from under a live user of it.
//
// Errors are joined, not logged and dropped: a session store that failed to
// flush on shutdown is exactly the kind of thing that turns into "the last turn
// of every conversation is missing" a week later.
//
// It is safe to call twice, which is what lets the startup path close a partial
// stack and the shutdown path close on the way out without either having to
// know what the other did.
func (s *storageStack) close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	for i := len(s.closers) - 1; i >= 0; i-- {
		closer := s.closers[i]
		if err := closer.close(); err != nil {
			// Both connection strings, because one stack can hold resources
			// built from either and a close failure echoes whichever it came
			// from. Scrub is idempotent, so the second pass is free.
			scrubbed := sessionbackend.Scrub(
				fmt.Errorf("close %s: %w", closer.name, err), s.connString)
			errs = append(errs, sessionbackend.Scrub(scrubbed, s.redisURL))
		}
	}
	s.closers = nil
	return errors.Join(errs...)
}

// storageDeps is the seam between the startup sequence and the steps that
// actually touch a database.
//
// It is here for one reason: the ordering rules in openPostgresStorage — ping
// before the upstream constructor, migrate before seeding, close in reverse on
// a partial failure — are the part most likely to break, and they cannot be
// tested against a real server on a machine that has none. With this, a unit
// test fails any single step and watches what gets closed.
//
// It is deliberately concrete functions and not an abstraction over pgx or
// go-redis. A fake supplies a nil pool and a nil client, and every step that
// would touch one is replaced alongside, so nothing dereferences them.
type storageDeps struct {
	openPool func(ctx context.Context, cfg storageConfig) (*pgxpool.Pool, func() error, error)
	ping     func(ctx context.Context, pool *pgxpool.Pool) error
	migrate  func(ctx context.Context, pool *pgxpool.Pool) error
	// newControlPlane builds the three views of the control plane that share one
	// pool. They are built together rather than one at a time because they are
	// one thing: a profile's foreign key points at the repository's tenants
	// table, so a process that built them separately could point them at
	// different databases.
	newControlPlane func(pool *pgxpool.Pool) (controlPlane, error)
	newSessions     func(cfg sessionbackend.Config) (session.Service, error)
	openRedis       func(ctx context.Context, cfg storageConfig) (goredis.UniversalClient, func() error, error)
	pingRedis       func(ctx context.Context, client goredis.UniversalClient) error
	newCoordinator  func(cfg storageConfig, client goredis.UniversalClient) (sessionlease.Coordinator, error)
}

// defaultStorageDeps is what the process runs with.
func defaultStorageDeps() storageDeps {
	return storageDeps{
		openPool:        openControlPlanePool,
		ping:            pingControlPlanePool,
		migrate:         migrateControlPlane,
		newControlPlane: newPostgresControlPlane,
		newSessions:     sessionbackend.New,
		openRedis:       openLeaseRedisClient,
		pingRedis:       pingLeaseRedisClient,
		newCoordinator:  newLeaseCoordinator,
	}
}

// openStorage builds the stack cfg asks for. The caller owns the result and
// must close it exactly once; on error nothing is returned to close, because
// openStorage has already released whatever it had built.
func openStorage(ctx context.Context, cfg storageConfig, deps storageDeps) (*storageStack, error) {
	// Re-checked rather than assumed: loadStorageConfig validates, but this is
	// the function that opens things, and "validate the whole configuration
	// before constructing anything" is only true if the check lives with the
	// construction.
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	switch cfg.profile {
	case profileInMemory:
		return openInMemoryStorage(ctx, cfg, deps)
	case profilePostgres:
		return openPostgresStorage(ctx, cfg, deps)
	default:
		// Unreachable: validate has already refused every other profile. It is
		// spelled out rather than left as a default branch so that adding a
		// profile to the type without adding it here fails loudly instead of
		// quietly building the in-memory stack.
		return nil, unknownProfileError(string(cfg.profile))
	}
}

// openInMemoryStorage builds the zero-dependency stack. Nothing here connects,
// resolves a name or reads a file: validate has already refused the only
// coordination backend that would.
func openInMemoryStorage(
	ctx context.Context,
	cfg storageConfig,
	deps storageDeps,
) (*storageStack, error) {
	sessions, err := deps.newSessions(cfg.sessionConfig())
	if err != nil {
		return nil, err
	}
	repository := tenant.NewMemoryRepository()
	// Gated by the repository it was just built beside, which is what makes the
	// in-memory stack hold the same rule the postgres one holds with a foreign
	// key: a profile cannot belong to a tenant this control plane never created.
	profiles, err := storagebundle.NewMemoryProfileRepository(repository)
	if err != nil {
		return nil, errors.Join(err, sessions.Close())
	}
	stack := &storageStack{
		repository: repository,
		profiles:   profiles,
		directory:  sessiondir.NewMemoryDirectory(),
		sessions:   sessions,
	}
	stack.push("session service", sessions.Close)

	if err := openCoordination(ctx, cfg, deps, stack); err != nil {
		return nil, errors.Join(err, stack.close())
	}
	return stack, nil
}

// openPostgresStorage builds the persistent stack.
//
// The order of the steps is the substance of this function:
//
//   - The pool is registered for closing in the same breath as it is created,
//     before anything can fail after it. A pool that is created and then
//     abandoned by an early return leaks connections for the life of the
//     process.
//   - Ping comes before the upstream session constructor. Upstream creates its
//     tables during construction, on a background context of its own that this
//     process cannot cancel, so an unreachable or wrong-schema database has to
//     be caught here — while the caller's deadline still applies — rather than
//     inside a call that ignores it.
//   - Migrations come before anything reads. Both migrations run on the shared
//     pool, so they land in the schema its search_path names.
//   - Coordination is created last, so it is closed first. A Worker that is
//     shutting down has to stop handing out and renewing run leases before the
//     store those leases protect goes away.
func openPostgresStorage(ctx context.Context, cfg storageConfig, deps storageDeps) (*storageStack, error) {
	stack := &storageStack{connString: cfg.dsn, redisURL: cfg.redisURL}
	// fail closes exactly what has been created so far, in reverse order, and
	// reports the failure together with anything that went wrong closing.
	// Scrubbing here as well as at each step is redundant on purpose: Scrub is
	// idempotent, and this is the boundary the DSN must not cross.
	fail := func(err error) (*storageStack, error) {
		return nil, errors.Join(
			sessionbackend.Scrub(sessionbackend.Scrub(err, cfg.dsn), cfg.redisURL),
			stack.close(),
		)
	}

	pool, closePool, err := deps.openPool(ctx, cfg)
	if err != nil {
		return fail(err)
	}
	stack.push("postgres pool", closePool)
	stack.pool = pool

	if err := deps.ping(ctx, pool); err != nil {
		return fail(err)
	}
	if err := deps.migrate(ctx, pool); err != nil {
		return fail(err)
	}

	plane, err := deps.newControlPlane(pool)
	if err != nil {
		return fail(err)
	}
	stack.repository, stack.profiles, stack.directory =
		plane.repository, plane.profiles, plane.directory

	sessions, err := deps.newSessions(cfg.sessionConfig())
	if err != nil {
		return fail(err)
	}
	stack.push("session service", sessions.Close)
	stack.sessions = sessions

	if err := openCoordination(ctx, cfg, deps, stack); err != nil {
		return fail(err)
	}
	return stack, nil
}

// openCoordination builds the run-lease coordinator and, under the redis
// backend, the client it borrows.
//
// The client is pushed before the coordinator, so close runs them the other way
// round: the coordinator stops renewing and releases while it still has a usable
// connection, and only then does the connection go. Doing it the other way
// would turn every shutdown into a burst of unavailable-backend errors on the
// way out.
//
// Under the in-memory backend nothing here connects. The redis URL is not even
// read: validate has already refused the combination that would need it.
func openCoordination(
	ctx context.Context,
	cfg storageConfig,
	deps storageDeps,
	stack *storageStack,
) error {
	var client goredis.UniversalClient
	if cfg.coordination == coordinationRedis {
		opened, closeClient, err := deps.openRedis(ctx, cfg)
		if err != nil {
			return err
		}
		stack.push("redis client", closeClient)
		// Like the pool's ping: NewClient does not dial, so without this an
		// unreachable Redis would first surface as a 503 on a live request
		// rather than as a refusal to start.
		if err := deps.pingRedis(ctx, opened); err != nil {
			return err
		}
		client = opened
	}

	coordinator, err := deps.newCoordinator(cfg, client)
	if err != nil {
		return err
	}
	stack.push("coordination", coordinator.Close)
	stack.coordinator = coordinator
	return nil
}

// openControlPlanePool opens the pool the repository and the directory share.
//
// One pool for both is the point: they are two views of one control plane, they
// are always configured together, and a single pool means a single search_path
// and a single set of connections to size. It is returned with its closer
// rather than closed here, because the caller owns it from the moment it
// exists.
//
// The upstream session service is not on this pool. It builds and owns its own
// from the same DSN, which is upstream's design, not a choice made here.
func openControlPlanePool(
	ctx context.Context,
	cfg storageConfig,
) (*pgxpool.Pool, func() error, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		// This is the error that most needs scrubbing. pgx redacts the copy of
		// the connection string it keeps on its own error, but the parse failure
		// it wraps is reported against the original: a password containing an
		// unencoded "/" is re-read as a port and quoted back in clear text.
		return nil, nil, sessionbackend.Scrub(
			fmt.Errorf("parse %s: %w", postgresDSNEnvVar, err), cfg.dsn)
	}
	if cfg.schema != "" {
		// Set on the pool config rather than with a SET after checkout, so every
		// connection the pool opens — including one it opens later to replace a
		// dropped one — carries the same search_path. Both migrations and every
		// statement of both components name their tables unqualified, so this is
		// the only thing placing them.
		poolConfig.ConnConfig.RuntimeParams["search_path"] = cfg.schema
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, nil, sessionbackend.Scrub(
			fmt.Errorf("open control-plane pool: %w", err), cfg.dsn)
	}
	// pgxpool.Close reports nothing and is safe to call once; the signature is
	// uniform so the stack can hold it next to the closers that do fail.
	return pool, func() error { pool.Close(); return nil }, nil
}

// pingControlPlanePool is the first and only place an unreachable database is
// allowed to surface. NewWithConfig does not dial, so without this the failure
// would appear somewhere further in — from a migration, or from inside the
// upstream constructor, which does not honour this context.
func pingControlPlanePool(ctx context.Context, pool *pgxpool.Pool) error {
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("reach the configured PostgreSQL database: %w", err)
	}
	return nil
}

// migrateControlPlane brings the schema up to date for both components this
// process owns. Both are idempotent, both take an advisory lock, and both are
// safe to run from every worker on every boot.
//
// The upstream session tables are not created here. Upstream creates them
// itself when its service is constructed, in the schema it is pointed at.
func migrateControlPlane(ctx context.Context, pool *pgxpool.Pool) error {
	if err := tenantpostgres.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate control plane: %w", err)
	}
	if err := sessiondirpostgres.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate session directory: %w", err)
	}
	return nil
}

// openLeaseRedisClient builds the client the run-lease coordinator borrows.
//
// It is returned with its closer rather than closed here, because the caller
// owns it from the moment it exists — the same rule the control-plane pool
// follows, and the reason the coordinator never closes the client it is handed.
//
// # Why ContextTimeoutEnabled is set
//
// Without it, go-redis v9 hands context.Background() to the socket read and
// write of every command, so the deadlines sessionlease sets — the acquire
// budget on Lifetime.Call, the per-call timeout of the renewal loop, the
// shutdown context of a release — stop at the client boundary and never become
// a read deadline. What would actually bound a command already on the wire is
// this client's own ReadTimeout and MaxRetries, which nothing in the lease
// timings knows about. With it set, go-redis takes the earlier of the context
// deadline and ReadTimeout, so Ping, Acquire, Renew and Release are each bounded
// by the context their own caller chose.
//
// It has to be set here. ParseURL leaves it false and there is no URL parameter
// for it: go-redis rejects query keys it does not recognise, so an operator
// cannot supply it and this line is the only place it can be turned on.
//
// Setting it is safe as a process-wide property of this client because this
// client is the coordinator's alone. openCoordination builds it, redislease.New
// is the only thing handed it, and no Session store shares it — there is no
// Redis storage profile to pair one with. Should a profile ever share one
// client between the two, this stops being a lease decision and becomes a
// decision about the shared client, and it belongs wherever that client is
// built.
func openLeaseRedisClient(
	_ context.Context,
	cfg storageConfig,
) (goredis.UniversalClient, func() error, error) {
	opts, err := goredis.ParseURL(cfg.redisURL)
	if err != nil {
		// ParseURL echoes the URL it could not parse, password included.
		return nil, nil, sessionbackend.Scrub(
			fmt.Errorf("parse %s: %w", redisURLEnvVar, err), cfg.redisURL)
	}
	opts.ContextTimeoutEnabled = true
	client := goredis.NewClient(opts)
	return client, client.Close, nil
}

// pingLeaseRedisClient is where an unreachable Redis is allowed to surface.
func pingLeaseRedisClient(ctx context.Context, client goredis.UniversalClient) error {
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("reach the configured Redis instance: %w", err)
	}
	return nil
}

// newLeaseCoordinator builds the coordinator this configuration asks for. The
// client is nil under the in-memory backend and unused there.
func newLeaseCoordinator(
	cfg storageConfig,
	client goredis.UniversalClient,
) (sessionlease.Coordinator, error) {
	switch cfg.coordination {
	case coordinationInMemory:
		// One store, one coordinator: this process is the only participant.
		return sessionlease.NewMemoryCoordinator(
			sessionlease.NewMemoryStore(), sessionlease.Config{})
	case coordinationRedis:
		return redislease.New(client, redislease.Options{})
	default:
		return nil, unknownCoordinationError(string(cfg.coordination))
	}
}

// controlPlane is the three views one pool is wrapped in. It is a struct rather
// than three return values so that a fourth view — or a change to which of them
// is which — does not silently re-order the tuple every caller unpacks.
type controlPlane struct {
	repository tenant.Repository
	profiles   storagebundle.ProfileRepository
	directory  sessiondir.Directory
}

// newPostgresControlPlane wraps the shared pool in the three components. No
// constructor connects and none takes ownership of the pool.
func newPostgresControlPlane(pool *pgxpool.Pool) (controlPlane, error) {
	repository, err := tenantpostgres.New(pool)
	if err != nil {
		return controlPlane{}, fmt.Errorf("create control-plane repository: %w", err)
	}
	// The same pool, so the profile table's foreign key points at the tenants
	// table this repository reads, and one Migrate created both.
	profiles, err := tenantpostgres.NewProfileRepository(pool)
	if err != nil {
		return controlPlane{}, fmt.Errorf("create backend profile repository: %w", err)
	}
	directory, err := sessiondirpostgres.New(pool)
	if err != nil {
		return controlPlane{}, fmt.Errorf("create session directory: %w", err)
	}
	return controlPlane{repository: repository, profiles: profiles, directory: directory}, nil
}
