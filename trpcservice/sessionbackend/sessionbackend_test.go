package sessionbackend

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// Every backend this package can build has to stay assignable to the one
// interface the Runner consumes. These assertions are the contract: an
// upstream release that changes a method set breaks the build here rather
// than at the call site, or silently at runtime.
var (
	_ session.Service = (*sessioninmemory.SessionService)(nil)
	_ session.Service = (*sessionpostgres.Service)(nil)
	_ session.Service = (*sessionredis.Service)(nil)
)

// The tests in this file must never contact postgres or redis, so `go test
// ./...` stays runnable on a machine with no infrastructure and no network.
// Anything needing a real server belongs in integration_test.go, behind the
// TRPC_SERVICE_SESSION_INTEGRATION gate.

// newTurn builds the two events of one conversation turn.
//
// Two upstream rules decide whether an appended event is still there on the
// next read, and a test that ignores either one passes against an empty
// session:
//
//  1. An event is only recorded when its Response is non-nil, not partial and
//     carries a payload. session.Session.UpdateUserSession applies this to
//     every backend, and the postgres persist path applies it again.
//  2. session.Session.ApplyEventFiltering discards the whole event list unless
//     it holds at least one user message. An assistant-only session therefore
//     reads back empty even though AppendEvent returned nil.
//
// So the fixture is a user message followed by the assistant reply, which is
// what a real turn looks like anyway.
func newTurn(invocationID, prompt, reply string) (*event.Event, *event.Event) {
	return newTestEvent(invocationID+"-user", model.NewUserMessage(prompt)),
		newTestEvent(invocationID+"-assistant", model.NewAssistantMessage(reply))
}

func newTestEvent(invocationID string, message model.Message) *event.Event {
	return event.NewResponseEvent(invocationID, "sessionbackend-test", &model.Response{
		Done:    true,
		Choices: []model.Choice{{Index: 0, Message: message}},
	})
}

func TestDefaultConfigIsInMemory(t *testing.T) {
	cfg := DefaultConfig()
	require.Equal(t, BackendInMemory, cfg.Backend)
	require.NoError(t, cfg.Validate())

	service, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, service)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	// The default backend has to be usable with no external service running.
	ctx := context.Background()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "sess"}
	created, err := service.CreateSession(ctx, key, session.StateMap{})
	require.NoError(t, err)
	require.NotNil(t, created)

	prompt, reply := newTurn("inv-1", "hello", "hello back")
	require.NoError(t, service.AppendEvent(ctx, created, prompt))
	require.NoError(t, service.AppendEvent(ctx, created, reply))

	loaded, err := service.GetSession(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Len(t, loaded.Events, 2)
}

// TestAssistantOnlySessionReadsBackEmpty pins the upstream filtering rule that
// newTurn exists to work around. It is not behaviour this package chose, but a
// caller that appends only assistant events loses them silently, so the spike
// records it as a fact rather than a surprise.
func TestAssistantOnlySessionReadsBackEmpty(t *testing.T) {
	service, err := New(DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	ctx := context.Background()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "assistant-only"}
	created, err := service.CreateSession(ctx, key, session.StateMap{})
	require.NoError(t, err)

	// AppendEvent reports success.
	require.NoError(t, service.AppendEvent(
		ctx, created, newTestEvent("inv-1", model.NewAssistantMessage("hello")),
	))

	loaded, err := service.GetSession(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Empty(t, loaded.Events, "an event list with no user message is dropped upstream")
}

func TestValidateAcceptsPersistentBackends(t *testing.T) {
	// Validate must decide on the config alone. If any of these cases tried to
	// connect, this test would fail or hang on a machine with no server.
	cases := map[string]Config{
		"postgres dsn only": {
			Backend:  BackendPostgres,
			Postgres: PostgresConfig{DSN: "postgres://u:p@127.0.0.1:55432/db?sslmode=disable"},
		},
		"postgres keyword dsn": {
			Backend:  BackendPostgres,
			Postgres: PostgresConfig{DSN: "host=127.0.0.1 port=55432 user=u password=p dbname=db"},
		},
		"postgres with namespacing": {
			Backend: BackendPostgres,
			Postgres: PostgresConfig{
				DSN:         "postgres://u:p@127.0.0.1:55432/db",
				TablePrefix: "spike_run_1",
				Schema:      "spike_schema",
			},
		},
		"postgres prefix with trailing underscore": {
			Backend: BackendPostgres,
			Postgres: PostgresConfig{
				DSN:         "postgres://u:p@127.0.0.1:55432/db",
				TablePrefix: "spike_",
			},
		},
		"redis url only": {
			Backend: BackendRedis,
			Redis:   RedisConfig{URL: "redis://127.0.0.1:56379/0"},
		},
		"redis with key prefix": {
			Backend: BackendRedis,
			Redis:   RedisConfig{URL: "redis://127.0.0.1:56379/0", KeyPrefix: "spike:run-1"},
		},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, cfg.Validate())
		})
	}
}

func TestValidateRejectsBadConfig(t *testing.T) {
	cases := map[string]struct {
		cfg  Config
		want string
	}{
		"empty backend": {
			cfg:  Config{},
			want: "backend is required",
		},
		"unknown backend": {
			cfg:  Config{Backend: Backend("mysql")},
			want: `unknown backend "mysql"`,
		},
		"backend is case sensitive": {
			cfg:  Config{Backend: Backend("InMemory")},
			want: `unknown backend "InMemory"`,
		},
		"postgres without dsn": {
			cfg:  Config{Backend: BackendPostgres},
			want: "postgres backend requires a DSN",
		},
		"postgres with blank dsn": {
			cfg:  Config{Backend: BackendPostgres, Postgres: PostgresConfig{DSN: "   "}},
			want: "postgres backend requires a DSN",
		},
		"redis without url": {
			cfg:  Config{Backend: BackendRedis},
			want: "redis backend requires a URL",
		},
		"redis with blank url": {
			cfg:  Config{Backend: BackendRedis, Redis: RedisConfig{URL: "\t"}},
			want: "redis backend requires a URL",
		},
		// Upstream validates the prefix and the schema through a helper that
		// panics. These cases prove the panic is unreachable through New.
		"table prefix with a quote": {
			cfg: Config{Backend: BackendPostgres, Postgres: PostgresConfig{
				DSN: "postgres://u:p@127.0.0.1:55432/db", TablePrefix: "spike';DROP TABLE x;--",
			}},
			want: "invalid postgres table prefix",
		},
		"table prefix starting with a digit": {
			cfg: Config{Backend: BackendPostgres, Postgres: PostgresConfig{
				DSN: "postgres://u:p@127.0.0.1:55432/db", TablePrefix: "1spike",
			}},
			want: "invalid postgres table prefix",
		},
		"table prefix too long": {
			cfg: Config{Backend: BackendPostgres, Postgres: PostgresConfig{
				DSN: "postgres://u:p@127.0.0.1:55432/db", TablePrefix: strings.Repeat("a", maxNamespaceLen+1),
			}},
			want: "postgres table prefix is too long (max 32 characters)",
		},
		"schema with a space": {
			cfg: Config{Backend: BackendPostgres, Postgres: PostgresConfig{
				DSN: "postgres://u:p@127.0.0.1:55432/db", Schema: "my schema",
			}},
			want: "invalid postgres schema",
		},
		"key prefix with a space": {
			cfg: Config{Backend: BackendRedis, Redis: RedisConfig{
				URL: "redis://127.0.0.1:56379/0", KeyPrefix: "spike run",
			}},
			want: "invalid redis key prefix",
		},
		"key prefix with a cluster hash tag": {
			cfg: Config{Backend: BackendRedis, Redis: RedisConfig{
				URL: "redis://127.0.0.1:56379/0", KeyPrefix: "spike{shard}",
			}},
			want: "invalid redis key prefix",
		},
		"key prefix too long": {
			cfg: Config{Backend: BackendRedis, Redis: RedisConfig{
				URL: "redis://127.0.0.1:56379/0", KeyPrefix: strings.Repeat("k", maxNamespaceLen+1),
			}},
			want: "redis key prefix is too long (max 32 characters)",
		},
		// Each of these passes the per-field limit and fails the joint one:
		// upstream spends both namespaces in one generated index name.
		"schema and table prefix too long together": {
			cfg: Config{Backend: BackendPostgres, Postgres: PostgresConfig{
				DSN:    "postgres://u:p@127.0.0.1:55432/db",
				Schema: strings.Repeat("s", 20), TablePrefix: strings.Repeat("p", 20),
			}},
			want: "postgres schema and table prefix are too long together",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Contains(t, err.Error(), tc.want)

			// New must reject exactly what Validate rejects, and must never
			// hand back a service the caller would then have to close.
			service, err := New(tc.cfg)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Nil(t, service)
		})
	}
}

// longestUpstreamIndexTail returns the longest "<table>_<suffix>" pair in the
// index set upstream's postgres migration creates.
//
// The set is restated here because it lives in an internal package this module
// cannot import. That restatement is the risk the two tests below exist to
// bound: the joint length check is arithmetic against upstream's naming rule,
// and arithmetic against a rule nobody can import goes stale silently.
func longestUpstreamIndexTail() string {
	pairs := []struct{ table, suffix string }{
		{"session_states", "unique_active"},
		{"session_states", "expires"},
		{"session_events", "lookup"},
		{"session_events", "expires"},
		{"session_track_events", "lookup"},
		{"session_track_events", "expires"},
		{"session_summaries", "unique_active"},
		{"session_summaries", "expires"},
		{"app_states", "unique_active"},
		{"app_states", "expires"},
		{"user_states", "unique_active"},
		{"user_states", "expires"},
	}
	longest := ""
	for _, pair := range pairs {
		if tail := pair.table + "_" + pair.suffix; len(tail) > len(longest) {
			longest = tail
		}
	}
	return longest
}

// The constant the joint bound is computed from has to be the worst case, or
// the bound is too generous and lets through exactly the config it exists to
// refuse.
func TestLongestGeneratedIndexTailIsTheWorstCaseUpstreamBuilds(t *testing.T) {
	require.Equal(t, longestUpstreamIndexTail(), longestGeneratedIndexTail)
	require.Equal(t, 63, maxIdentifierLen, "PostgreSQL's NAMEDATALEN-1")
}

// The joint bound is exact, so both sides of it are asserted. One character
// under is a deployment that works and must not be refused; one character over
// is the fault, and it is worth restating what that fault is, because it is not
// a failed statement:
//
// upstream builds "idx_<schema>_<prefix>_<table>_<suffix>" and never measures
// it. PostgreSQL truncates a name over 63 bytes to 63 bytes with a NOTICE, so
// CREATE INDEX succeeds under a shortened name; upstream then verifies the name
// it asked for, does not find it, and fails the whole constructor. Against a
// tenant profile that is worse than a rejected request, because the profile is
// immutable: the id is spent and can never produce a Bundle.
func TestValidateBoundsSchemaAndTablePrefixTogether(t *testing.T) {
	const dsn = "postgres://u:p@127.0.0.1:55432/db"

	// The name upstream generates from the two namespaces, so the test measures
	// the same string PostgreSQL would.
	indexName := func(schema, prefix string) string {
		name := longestGeneratedIndexTail
		if prefix != "" {
			name = prefix + "_" + name
		}
		if schema != "" {
			name = schema + "_" + name
		}
		return "idx_" + name
	}
	postgres := func(schema, prefix string) Config {
		return Config{Backend: BackendPostgres, Postgres: PostgresConfig{
			DSN: dsn, Schema: schema, TablePrefix: prefix,
		}}
	}

	t.Run("both namespaces", func(t *testing.T) {
		// 63 - len("idx_") - 2 separators - len(the longest tail).
		const budget = 26
		schema, prefix := strings.Repeat("s", 13), strings.Repeat("p", budget-13)

		require.Len(t, indexName(schema, prefix), maxIdentifierLen,
			"the fit changed; this test no longer sits on the boundary")
		require.NoError(t, postgres(schema, prefix).Validate(),
			"a name that fits exactly is a working deployment")

		over := prefix + "p"
		require.Len(t, indexName(schema, over), maxIdentifierLen+1)
		err := postgres(schema, over).Validate()
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.Contains(t, err.Error(), "max 26 characters between them")
		require.NotContains(t, err.Error(), schema, "the error echoes a tenant-supplied value")
		require.NotContains(t, err.Error(), over, "the error echoes a tenant-supplied value")
	})

	// The schema-only case is this process's own store, which names a schema and
	// no prefix. It has one character more to spend, and the per-field limit of
	// 32 is above it — so a 28-character schema passes every other check in this
	// package and still breaks the constructor.
	t.Run("schema only", func(t *testing.T) {
		const budget = 27
		schema := strings.Repeat("s", budget)
		require.Len(t, indexName(schema, ""), maxIdentifierLen)
		require.NoError(t, postgres(schema, "").Validate())

		over := schema + "s"
		require.Less(t, len(over), maxNamespaceLen+1, "this case must clear the per-field limit")
		err := postgres(over, "").Validate()
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.Contains(t, err.Error(), "max 27 characters between them")
	})

	// A trailing underscore is upstream's separator, not part of the prefix, so
	// it must not be counted twice.
	t.Run("a trailing underscore is not spent twice", func(t *testing.T) {
		const budget = 26
		schema, prefix := strings.Repeat("s", 13), strings.Repeat("p", budget-13)
		require.NoError(t, postgres(schema, prefix+"_").Validate())
	})
}

// These fields are filled from a tenant's storage profile and their refusal is
// answered to an admin API caller as a 400. An operator who pastes a DSN, a URL
// or a password into the wrong field must not read it back out of the error:
// that body reaches an access log, a proxy trace and a bug report, and the
// caller already knows what they sent.
func TestValidateNeverEchoesTheRejectedValue(t *testing.T) {
	const pasted = "postgres://admin:hunter2@db.internal:5432/sessions"

	for name, cfg := range map[string]Config{
		"pasted into the table prefix": {
			Backend: BackendPostgres,
			Postgres: PostgresConfig{
				DSN: "postgres://u:p@127.0.0.1:55432/db", TablePrefix: pasted,
			},
		},
		"pasted into the schema": {
			Backend: BackendPostgres,
			Postgres: PostgresConfig{
				DSN: "postgres://u:p@127.0.0.1:55432/db", Schema: pasted,
			},
		},
		"pasted into the key prefix": {
			Backend: BackendRedis,
			Redis: RedisConfig{
				URL: "redis://127.0.0.1:56379/0", KeyPrefix: pasted,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := cfg.Validate()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.NotContains(t, err.Error(), pasted)
			require.NotContains(t, err.Error(), "hunter2")
			// Still says which field broke which rule, or the caller cannot fix
			// the request.
			require.Regexp(t, `(table prefix|schema|key prefix)`, err.Error())
		})
	}

	// A value short enough to reach the pattern check rather than the length
	// check takes the other branch, and must not echo either.
	for name, cfg := range map[string]Config{
		"short secret in the schema": {
			Backend: BackendPostgres,
			Postgres: PostgresConfig{
				DSN: "postgres://u:p@127.0.0.1:55432/db", Schema: "hunter2!",
			},
		},
		"short secret in the key prefix": {
			Backend: BackendRedis,
			Redis: RedisConfig{
				URL: "redis://127.0.0.1:56379/0", KeyPrefix: "hunter2 ",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := cfg.Validate()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.NotContains(t, err.Error(), "hunter2")
		})
	}
}

func TestValidateIgnoresUnselectedBackends(t *testing.T) {
	// A process that carries settings for all three backends and selects one
	// must not be rejected for the two it is not using.
	cfg := Config{
		Backend:  BackendInMemory,
		Postgres: PostgresConfig{TablePrefix: "not valid at all"},
		Redis:    RedisConfig{KeyPrefix: "not valid either"},
	}
	require.NoError(t, cfg.Validate())

	service, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, service.Close())
}

func TestDescribeHidesCredentials(t *testing.T) {
	const password = "sup3r-s3cret"
	cases := map[string]Config{
		"postgres url dsn": {
			Backend:  BackendPostgres,
			Postgres: PostgresConfig{DSN: "postgres://user:" + password + "@127.0.0.1:55432/db"},
		},
		"postgres keyword dsn": {
			Backend:  BackendPostgres,
			Postgres: PostgresConfig{DSN: "host=127.0.0.1 user=user password=" + password},
		},
		"redis url": {
			Backend: BackendRedis,
			Redis:   RedisConfig{URL: "redis://user:" + password + "@127.0.0.1:56379/0"},
		},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			described := cfg.Describe()
			require.NotContains(t, described, password)
			require.Contains(t, described, "set")
		})
	}

	require.Contains(t, Config{Backend: BackendRedis}.Describe(), "absent")
	require.Contains(t, DefaultConfig().Describe(), "inmemory")
}
