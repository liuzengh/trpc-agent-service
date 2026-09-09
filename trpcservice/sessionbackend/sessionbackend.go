// Package sessionbackend builds the upstream session.Service that one process
// stores its conversation history in.
//
// The surface is deliberately small: a backend name, the single connection
// string that backend needs, and the namespacing knobs the integration tests
// need to run against a shared server. Every other upstream option keeps its
// default, so this package never turns into a second, drifting copy of the
// upstream option set. There is no backend registry, no capability table and
// no control-plane storage here; those belong to a later slice.
//
// Ownership: New returns a service the caller owns and must Close exactly
// once, after every Runner sharing it has stopped. This package never closes
// a service it has returned and keeps no reference to one.
package sessionbackend

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// Backend names a session storage implementation.
type Backend string

const (
	// BackendInMemory keeps sessions in process memory. It needs no external
	// service, and every session is lost when the process exits.
	BackendInMemory Backend = "inmemory"

	// BackendPostgres stores sessions in PostgreSQL. Upstream creates its own
	// tables on construction and deletes sessions softly; see
	// docs/session-backend.md.
	BackendPostgres Backend = "postgres"

	// BackendRedis stores sessions in Redis under the upstream key layout.
	BackendRedis Backend = "redis"
)

// DefaultBackend is the backend a process should pick when nothing else is
// configured. It is the only one that needs no external service, so the demo
// entrypoint keeps running against an empty machine.
const DefaultBackend = BackendInMemory

// ErrInvalidConfig is the sentinel behind every configuration error this
// package reports, so a caller can separate "you configured this wrong" from
// "the backend was unreachable" with errors.Is.
var ErrInvalidConfig = errors.New("sessionbackend: invalid configuration")

// redacted replaces a secret that would otherwise reach a caller's log.
const redacted = "[REDACTED]"

// maxNamespaceLen bounds a table prefix and a schema name individually.
// Upstream allows 64 characters, but PostgreSQL truncates identifiers at 63
// bytes and the longest upstream table name ("session_track_events") already
// spends 20 of them, so a prefix upstream accepts can still collide after
// truncation. 32 leaves room for the table suffix and is far above any real
// prefix.
//
// It bounds each namespace on its own and says nothing about the two together.
// Generated index names spend both at once and are what actually runs out of
// room first; validateGeneratedIndexNames is the check for that.
const maxNamespaceLen = 32

// maxIdentifierLen is PostgreSQL's identifier limit. NAMEDATALEN is 64, so a
// name longer than 63 bytes is truncated to 63 — with a NOTICE, not an error,
// which is what makes an over-long generated name a silent fault rather than a
// failed statement.
const maxIdentifierLen = 63

// longestGeneratedIndexTail is the longest "<table>_<suffix>" pair upstream
// builds an index name from, across the index set its migration creates:
// session_summaries carries a unique_active index, and no other pair in that
// set is longer.
//
// It is a literal rather than a computation over upstream's tables because
// those live in an internal package this module cannot import. An upstream
// release that adds a longer pair would make this bound too generous, which is
// why the pair is asserted against upstream's own naming in this package's
// tests rather than only being written down here.
const longestGeneratedIndexTail = "session_summaries_unique_active"

// identifierPattern mirrors the upstream SQL identifier rule. Upstream applies
// it through a Must-style helper that panics, so Config.Validate has to reject
// a bad prefix or schema before New reaches that helper.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// keyPrefixPattern bounds a Redis key prefix. Upstream does not validate it,
// but the prefix is concatenated into every key, so whitespace or a Redis
// cluster hash tag would silently reshape the key space.
var keyPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

// Config selects one session backend and carries only that backend's
// settings. The struct for an unselected backend is ignored, not validated.
type Config struct {
	// Backend names the implementation to build. It is required: an empty
	// backend is a configuration typo, not a request for the default. Use
	// DefaultConfig when you want the default.
	Backend Backend

	// Postgres applies when Backend is BackendPostgres.
	Postgres PostgresConfig

	// Redis applies when Backend is BackendRedis.
	Redis RedisConfig
}

// PostgresConfig configures BackendPostgres.
type PostgresConfig struct {
	// DSN is the connection string, in either URL form
	// ("postgres://user:password@host:5432/db?sslmode=disable") or libpq
	// keyword form ("host=... user=... password=..."). Required.
	//
	// It carries a password. Never log this field; log Describe instead.
	DSN string

	// TablePrefix namespaces every upstream table, which is what lets two
	// runs share one database without colliding. Upstream appends an
	// underscore when the prefix does not end in one. Optional.
	TablePrefix string

	// Schema is the PostgreSQL schema the tables live in. It must already
	// exist: upstream creates tables, not schemas. Optional; empty means the
	// server default, normally "public".
	Schema string
}

// RedisConfig configures BackendRedis.
type RedisConfig struct {
	// URL is the connection URL ("redis://user:password@host:6379/0").
	// Required.
	//
	// It carries a password. Never log this field; log Describe instead.
	URL string

	// KeyPrefix namespaces every upstream key, which is what lets two runs
	// share one Redis instance without colliding. Optional.
	KeyPrefix string
}

// DefaultConfig returns the configuration a process gets when it asks for no
// backend in particular: in-memory, no external dependency.
func DefaultConfig() Config {
	return Config{Backend: DefaultBackend}
}

// Describe renders the config for a log line. It names the backend and the
// namespacing knobs, and reports only whether a connection string is present,
// never its contents.
func (c Config) Describe() string {
	switch c.Backend {
	case BackendPostgres:
		return fmt.Sprintf(
			"backend=postgres dsn=%s table_prefix=%q schema=%q",
			presence(c.Postgres.DSN), c.Postgres.TablePrefix, c.Postgres.Schema,
		)
	case BackendRedis:
		return fmt.Sprintf(
			"backend=redis url=%s key_prefix=%q",
			presence(c.Redis.URL), c.Redis.KeyPrefix,
		)
	default:
		return fmt.Sprintf("backend=%q", string(c.Backend))
	}
}

func presence(value string) string {
	if strings.TrimSpace(value) == "" {
		return "absent"
	}
	return "set"
}

// Validate reports whether New can build this config. It is the whole of the
// input checking New performs, so a caller can reject a bad config at startup
// before it owns anything that needs closing.
//
// Validate contacts no external service: a config that validates can still
// fail to connect.
func (c Config) Validate() error {
	switch c.Backend {
	case BackendInMemory:
		return nil
	case BackendPostgres:
		return c.Postgres.validate()
	case BackendRedis:
		return c.Redis.validate()
	case "":
		return fmt.Errorf("%w: backend is required (one of %s)", ErrInvalidConfig, backendList())
	default:
		return fmt.Errorf(
			"%w: unknown backend %q (want one of %s)",
			ErrInvalidConfig, string(c.Backend), backendList(),
		)
	}
}

func (c PostgresConfig) validate() error {
	if strings.TrimSpace(c.DSN) == "" {
		return fmt.Errorf("%w: postgres backend requires a DSN", ErrInvalidConfig)
	}
	prefix := strings.TrimSuffix(c.TablePrefix, "_")
	if err := validateIdentifier("postgres table prefix", prefix); err != nil {
		return err
	}
	if err := validateIdentifier("postgres schema", c.Schema); err != nil {
		return err
	}
	return validateGeneratedIndexNames(c.Schema, prefix)
}

// validateGeneratedIndexNames rejects a schema and table prefix whose generated
// index names PostgreSQL would truncate.
//
// This is a joint bound, and it is the reason it exists at all: the per-field
// limit above is checked against each namespace on its own, but upstream spends
// both of them in one identifier. It builds every index as
//
//	idx_<schema>_<prefix>_<table>_<suffix>
//
// and never measures the result. PostgreSQL truncates an identifier over 63
// bytes to 63 bytes with a NOTICE rather than an error, so CREATE INDEX
// succeeds under a shortened name — and then upstream's own post-migration
// verification looks for the name it asked for, does not find it, and fails the
// whole constructor with "required unique index(es) invalid".
//
// Rejecting it here rather than at build time is the point. A Profile is
// immutable and cannot be deleted, so a Profile accepted with namespaces that
// are too long together is a Profile that occupies an id forever and can never
// produce a Bundle. The caller has to hear it as a bad request.
func validateGeneratedIndexNames(schema, prefix string) error {
	// The fixed part of the longest name upstream generates: "idx_", the
	// separator before each namespace it actually spends, and the longest
	// "<table>_<suffix>" pair in its index set.
	fixed := len("idx_") + len(longestGeneratedIndexTail)
	if schema != "" {
		fixed++
	}
	if prefix != "" {
		fixed++
	}
	if fixed+len(schema)+len(prefix) <= maxIdentifierLen {
		return nil
	}
	// The budget is reported rather than the values: these fields are
	// tenant-supplied and the error is answered to an API caller. What a caller
	// needs is how much room they have, not their own input read back.
	return fmt.Errorf(
		"%w: postgres schema and table prefix are too long together "+
			"(max %d characters between them, so the generated index names fit "+
			"PostgreSQL's %d-character limit)",
		ErrInvalidConfig, maxIdentifierLen-fixed, maxIdentifierLen,
	)
}

func (c RedisConfig) validate() error {
	if strings.TrimSpace(c.URL) == "" {
		return fmt.Errorf("%w: redis backend requires a URL", ErrInvalidConfig)
	}
	if c.KeyPrefix == "" {
		return nil
	}
	if len(c.KeyPrefix) > maxNamespaceLen {
		return fmt.Errorf(
			"%w: redis key prefix is too long (max %d characters)",
			ErrInvalidConfig, maxNamespaceLen,
		)
	}
	if !keyPrefixPattern.MatchString(c.KeyPrefix) {
		return fmt.Errorf(
			"%w: invalid redis key prefix (letters, digits, '_', '.', ':' and '-' only)",
			ErrInvalidConfig,
		)
	}
	return nil
}

// validateIdentifier rejects a table prefix or schema upstream would either
// refuse or, worse, accept into a generated SQL identifier. Empty is allowed:
// both knobs are optional.
//
// The rejected value is named by field and by rule, never quoted back. These
// fields are filled from a tenant-supplied storage profile and answered to an
// admin API caller, and an operator who pastes a DSN or a key into the wrong
// field of a request would otherwise read it back out of the 400 — where it
// lands in an access log, a proxy trace and a bug report. The field name and
// the rule are what a caller needs to fix the request; they already know what
// they sent.
func validateIdentifier(what, value string) error {
	if value == "" {
		return nil
	}
	if len(value) > maxNamespaceLen {
		return fmt.Errorf(
			"%w: %s is too long (max %d characters)",
			ErrInvalidConfig, what, maxNamespaceLen,
		)
	}
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf(
			"%w: invalid %s (must start with a letter or '_' and contain only letters, digits and '_')",
			ErrInvalidConfig, what,
		)
	}
	return nil
}

func backendList() string {
	return fmt.Sprintf("%q, %q, %q", BackendInMemory, BackendPostgres, BackendRedis)
}

// New builds the session service named by cfg.
//
// The caller owns the returned service and must Close it exactly once. New
// closes nothing it returns, and on error returns no service to close.
//
// BackendInMemory and BackendRedis do not reach the network here: the redis
// client is created lazily, so a URL that points nowhere usually surfaces on
// the first session call rather than from New. BackendPostgres does connect,
// because upstream creates its tables during construction; that work runs on
// an upstream-owned background context and cannot be cancelled by the caller,
// which is why New takes no context.
func New(cfg Config) (session.Service, error) {
	// Validate first: the upstream prefix and schema options panic on input
	// they dislike instead of returning an error, so nothing may reach them
	// unchecked.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch cfg.Backend {
	case BackendInMemory:
		return sessioninmemory.NewSessionService(), nil
	case BackendPostgres:
		return newPostgres(cfg.Postgres)
	case BackendRedis:
		return newRedis(cfg.Redis)
	default:
		// Unreachable: Validate has already rejected every other backend.
		return nil, fmt.Errorf("%w: unknown backend %q", ErrInvalidConfig, string(cfg.Backend))
	}
}

func newPostgres(cfg PostgresConfig) (session.Service, error) {
	opts := []sessionpostgres.ServiceOpt{sessionpostgres.WithPostgresClientDSN(cfg.DSN)}
	if cfg.TablePrefix != "" {
		opts = append(opts, sessionpostgres.WithTablePrefix(cfg.TablePrefix))
	}
	if cfg.Schema != "" {
		opts = append(opts, sessionpostgres.WithSchema(cfg.Schema))
	}
	service, err := sessionpostgres.NewService(opts...)
	if err != nil {
		return nil, Scrub(
			fmt.Errorf("sessionbackend: create postgres session service: %w", err),
			cfg.DSN,
		)
	}
	return service, nil
}

func newRedis(cfg RedisConfig) (session.Service, error) {
	opts := []sessionredis.ServiceOpt{sessionredis.WithRedisClientURL(cfg.URL)}
	if cfg.KeyPrefix != "" {
		opts = append(opts, sessionredis.WithKeyPrefix(cfg.KeyPrefix))
	}
	service, err := sessionredis.NewService(opts...)
	if err != nil {
		return nil, Scrub(
			fmt.Errorf("sessionbackend: create redis session service: %w", err),
			cfg.URL,
		)
	}
	return service, nil
}

// Scrub replaces every password of connectionString wherever it appears in
// err, and returns the result.
//
// Drivers echo the string they failed to parse or dial, so an unscrubbed error
// puts the password into whatever log the caller writes it to. pgx does redact
// its own ParseConfigError, but only the copy of the connection string it
// keeps: the error it wraps is a parse failure reported against the original,
// and one of those failures quotes the password back verbatim (see
// urlPasswords). So every error built from a connection string has to pass
// through here, not only the ones this package produces.
//
// It understands both shapes the upstream backends accept: a URL with userinfo
// ("postgres://user:password@host/db"), including the percent-encoded spelling
// a driver echoes back, and libpq keyword form ("host=... password='p@ss
// w0rd'").
//
// The scope of the guarantee, which must not be overstated:
//
//   - Only passwords are removed. The user, host and database name stay,
//     because an operator needs them to know which connection failed.
//   - A redacted error is a fresh error carrying the redacted text only. It
//     deliberately does not wrap the original, because an Unwrap chain would
//     hand the secret straight back — so errors.Is and errors.As no longer
//     match once redaction has happened. That is the price of the guarantee.
//   - An error that contained no password is returned exactly as it came in,
//     chain included, and a nil error stays nil.
//   - Redaction is decided on the rendered message. A value hidden in a struct
//     field of some error type, rather than in its Error text, is out of reach,
//     as is anything a driver writes to its own logs, traces or stderr.
//   - Matching is by substring, so a password a driver reshapes before printing
//     is only removed where the reshaped text is one this package knows about.
//     The one such text pgx is known to produce is handled; see
//     quotedPortPattern.
//
// Scrub is safe to apply twice: the second pass finds nothing left to replace
// and returns its input unchanged.
func Scrub(err error, connectionString string) error {
	if err == nil {
		return nil
	}
	original := err.Error()
	scrubbed := original
	secrets := secretsOf(connectionString)
	// Longest first, so redacting a short secret cannot leave a fragment of a
	// longer one that happens to contain it.
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		if len(secret) < minGlobalRedaction {
			// Too short to blank everywhere: a one-character secret occurs
			// inside ordinary words, and replacing every occurrence would
			// destroy the message while telling the operator nothing. It is
			// still removed from the one position a parser is known to quote it
			// into, immediately below.
			continue
		}
		scrubbed = strings.ReplaceAll(scrubbed, secret, redacted)
	}
	scrubbed = redactQuotedPort(scrubbed, secrets)
	if scrubbed == original {
		return err
	}
	return errors.New(scrubbed)
}

// quotedPortPattern matches the only error text observed to quote a password
// back in a shape that substring replacement cannot reach. url.Parse ends the
// authority at the first authorityDelimiters rune, so a password containing one
// unencoded is re-read as "host:port", and the parser quotes the text before
// that separator as the offending port. pgx renders that error verbatim:
//
//	postgres://user:p/secret@host:notaport/db
//	  -> cannot parse `postgres://user:xxxxxx@host:notaport/db`:
//	     failed to parse as URL (invalid port ":p" after host)
//
// The fragment is bounded only by the password, so it can be a single character
// — which is exactly why it needs a targeted rule rather than a global one.
var quotedPortPattern = regexp.MustCompile(`invalid port ":([^"]*)" after host`)

// redactQuotedPort replaces the port quoted by quotedPortPattern, and only that
// port, when it is part of one of the connection string's secrets.
//
// The containment test is what keeps an ordinary diagnostic intact: a genuine
// port typo is not part of the password, so `invalid port ":notaport"` survives
// and still tells the operator what was wrong. It is also what makes the
// function idempotent, since the redaction marker is part of no secret.
func redactQuotedPort(message string, secrets []string) string {
	return quotedPortPattern.ReplaceAllStringFunc(message, func(match string) string {
		fragment := quotedPortPattern.FindStringSubmatch(match)[1]
		if fragment == "" || !partOfAnySecret(fragment, secrets) {
			return match
		}
		return `invalid port ":` + redacted + `" after host`
	})
}

func partOfAnySecret(fragment string, secrets []string) bool {
	for _, secret := range secrets {
		if strings.Contains(secret, fragment) {
			return true
		}
	}
	return false
}

// secretsOf returns the substrings of a connection string that must never be
// logged. It understands both shapes the upstream backends accept: a URL with
// userinfo ("postgres://user:password@host/db") and libpq keyword form
// ("host=... password=...").
func secretsOf(connString string) []string {
	var secrets []string
	for _, password := range urlPasswords(connString) {
		// The spelling as written first: a driver echoes the string as it was
		// given to it.
		secrets = append(secrets, password)
		// And the decoded one, because a driver that unescapes the userinfo
		// before reporting it prints something the written form does not match.
		// An invalid escape sequence simply has no decoded spelling.
		if decoded, err := url.PathUnescape(password); err == nil && decoded != password && decoded != "" {
			secrets = append(secrets, decoded)
		}
	}
	for _, field := range keywordFields(connString) {
		if value, ok := strings.CutPrefix(field, "password="); ok && value != "" {
			secrets = append(secrets, value)
		}
	}
	return secrets
}

// authorityDelimiters end the authority of a URL. A password containing one
// unencoded is therefore not inside the userinfo as far as any parser is
// concerned, which is what urlPasswords has to work around.
const authorityDelimiters = "/?#"

// minGlobalRedaction is the shortest secret Scrub will blank everywhere in a
// message. Below it, a blind replacement costs more than it buys: "p" occurs
// inside "parse", "port" and "host", so replacing every occurrence would leave
// an unreadable error. Shorter secrets are not conceded — they are redacted by
// position instead, in redactQuotedPort.
const minGlobalRedaction = 3

// urlPasswords returns every spelling of the password of a URL-form connection
// string that an error might contain, or nil when there is none.
//
// A parseable URL has two that matter: the one written in the string, which is
// what a driver echoing the DSN prints, and the percent-decoded one, which is
// what the driver authenticates with.
//
// An unparseable URL is the case that matters more, because it is the one that
// leaks. The authority ends at the first authorityDelimiters rune, so a password
// containing one unencoded puts the "@" that would delimit the userinfo out of
// scope before it is ever looked for — for url.Parse and for authorityPassword
// alike. pgx then reports the text before that separator as the port:
//
//	postgres://user:s3cret/x@host:5432/db
//	  -> cannot parse `postgres://user:xxxxxx@host:5432/db`:
//	     failed to parse as URL (invalid port ":s3cret" after host)
//
// pgx redacted its own copy of the userinfo and printed the password anyway, so
// this function cannot rely on a parser to find it. When the grammar's bounds
// yield no password it guesses instead, from the whole span between the first
// ":" and the last "@", plus each delimiter-free fragment of that span, since a
// re-parse is what quotes a fragment back.
//
// Fragments are returned whatever their length. Scrub decides which of them are
// safe to blank everywhere and which have to be redacted by position instead.
func urlPasswords(connString string) []string {
	_, afterScheme, found := strings.Cut(connString, "://")
	if !found {
		return nil
	}
	var passwords []string
	if parsed, err := url.Parse(connString); err == nil && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok {
			passwords = append(passwords, password)
		}
	}
	// For a URL that parses, this always finds the written spelling, so the
	// guessing below is reached only when the grammar itself locates nothing.
	if written := authorityPassword(afterScheme); written != "" {
		return append(passwords, written)
	}
	at := strings.LastIndex(afterScheme, "@")
	if at < 0 {
		return passwords
	}
	_, span, found := strings.Cut(afterScheme[:at], ":")
	if !found || span == "" {
		return passwords
	}
	passwords = append(passwords, span)
	for _, fragment := range strings.FieldsFunc(span, isAuthorityDelimiter) {
		if fragment != span {
			passwords = append(passwords, fragment)
		}
	}
	return passwords
}

// authorityPassword returns the password written in the authority of a
// post-scheme connection string, by the URL grammar's own bounds: the authority
// ends at the first authorityDelimiters rune, the userinfo ends at the last "@"
// within it (so an unencoded "@" inside a password is still found), and the
// password is whatever follows the first ":".
//
// An empty result is a signal rather than an answer: for a string url.Parse
// accepts it means there is genuinely no password, and for one it rejects it
// means the password is in the shape that leaks.
func authorityPassword(afterScheme string) string {
	authority := afterScheme
	if end := strings.IndexAny(afterScheme, authorityDelimiters); end >= 0 {
		authority = afterScheme[:end]
	}
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return ""
	}
	_, password, found := strings.Cut(authority[:at], ":")
	if !found {
		return ""
	}
	return password
}

func isAuthorityDelimiter(r rune) bool {
	return strings.ContainsRune(authorityDelimiters, r)
}

// keywordFields splits a libpq keyword connection string into its key=value
// tokens, honouring the single quotes libpq allows around a value containing
// spaces. Splitting on whitespace alone would cut a quoted password in half
// and redact only the first half.
//
// It does not implement libpq's backslash escaping, so a password containing
// an escaped quote is redacted only up to that quote.
func keywordFields(connString string) []string {
	var (
		fields  []string
		current strings.Builder
		quoted  bool
	)
	flush := func() {
		if current.Len() > 0 {
			fields = append(fields, current.String())
			current.Reset()
		}
	}
	for i := 0; i < len(connString); i++ {
		switch c := connString[i]; {
		case c == '\'':
			quoted = !quoted
		case !quoted && (c == ' ' || c == '\t' || c == '\n' || c == '\r'):
			flush()
		default:
			current.WriteByte(c)
		}
	}
	flush()
	return fields
}
