package storagebundle

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// inMemoryProfile is the profile that needs nothing outside the process: no
// credential to resolve, no target to reach.
func inMemoryProfile(tenantID string, id string) Profile {
	return Profile{
		TenantID: tenantID,
		ID:       id,
		Session:  SessionSpec{Backend: sessionbackend.BackendInMemory},
	}
}

func postgresProfile(tenantID string, id string) Profile {
	return Profile{
		TenantID: tenantID,
		ID:       id,
		Session: SessionSpec{
			Backend:  sessionbackend.BackendPostgres,
			Postgres: &PostgresSpec{DSNRef: "env:TENANT_A_SESSION_DSN"},
		},
	}
}

func redisProfile(tenantID string, id string) Profile {
	return Profile{
		TenantID: tenantID,
		ID:       id,
		Session: SessionSpec{
			Backend: sessionbackend.BackendRedis,
			Redis:   &RedisSpec{URLRef: "env:TENANT_A_SESSION_URL"},
		},
	}
}

func TestProfileValidateAcceptsEachBackendInItsOwnShape(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile Profile
	}{
		{"inmemory", inMemoryProfile("tenant-a", "p1")},
		{"postgres", postgresProfile("tenant-a", "p1")},
		{
			"postgres with namespacing",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{
						DSNRef:      "env:TENANT_A_DSN",
						Schema:      "tenant_a",
						TablePrefix: "sess_",
					},
				},
			},
		},
		{"redis", redisProfile("tenant-a", "p1")},
		{
			"redis with key prefix",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendRedis,
					Redis:   &RedisSpec{URLRef: "env:TENANT_A_URL", KeyPrefix: "tenant-a:sess"},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.profile.Validate())
		})
	}
}

// Every refusal is ErrInvalidProfile, so a caller can tell "this can never be
// built" from "this could not be built right now" without matching strings.
func TestProfileValidateRejectsIdentityBackendAndShape(t *testing.T) {
	valid := inMemoryProfile("tenant-a", "p1")

	for _, tc := range []struct {
		name    string
		profile Profile
		wants   string
	}{
		{"no tenant", Profile{ID: "p1", Session: valid.Session}, "tenant"},
		{
			"tenant is not a resource id",
			Profile{TenantID: "tenant a", ID: "p1", Session: valid.Session},
			"tenant",
		},
		{"no id", Profile{TenantID: "tenant-a", Session: valid.Session}, "backend profile id"},
		{
			"id is not a resource id",
			Profile{TenantID: "tenant-a", ID: "p 1", Session: valid.Session},
			"backend profile id",
		},
		{
			"id long enough to be a payload",
			Profile{
				TenantID: "tenant-a",
				ID:       strings.Repeat("p", 200),
				Session:  valid.Session,
			},
			"backend profile id",
		},
		{
			"no backend",
			Profile{TenantID: "tenant-a", ID: "p1"},
			"session backend is required",
		},
		{
			"unknown backend",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session:  SessionSpec{Backend: sessionbackend.Backend("cassandra")},
			},
			"unknown session backend",
		},
		{
			"inmemory carrying postgres settings",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend:  sessionbackend.BackendInMemory,
					Postgres: &PostgresSpec{DSNRef: "env:DSN"},
				},
			},
			"takes no settings",
		},
		{
			"inmemory carrying redis settings",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendInMemory,
					Redis:   &RedisSpec{URLRef: "env:URL"},
				},
			},
			"takes no settings",
		},
		{
			"postgres without its settings",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session:  SessionSpec{Backend: sessionbackend.BackendPostgres},
			},
			"requires postgres settings",
		},
		{
			"postgres carrying redis settings too",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend:  sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{DSNRef: "env:DSN"},
					Redis:    &RedisSpec{URLRef: "env:URL"},
				},
			},
			"must not carry redis settings",
		},
		{
			"postgres without a dsn ref",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend:  sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{Schema: "tenant_a"},
				},
			},
			"dsn_ref is required",
		},
		{
			"postgres schema that is not an identifier",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{
						DSNRef: "env:DSN",
						Schema: `public"; drop table sessions --`,
					},
				},
			},
			"schema",
		},
		{
			// Each namespace is well within its own limit and the pair is not.
			// It has to be refused here, at create time: a Profile is immutable
			// and cannot be deleted, so one accepted with namespaces upstream
			// cannot fit into an index name spends its id forever on content
			// that can never produce a Bundle.
			"postgres schema and table prefix too long together",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{
						DSNRef:      "env:DSN",
						Schema:      "tenant_a_sessions_production",
						TablePrefix: "conversations",
					},
				},
			},
			"too long together",
		},
		{
			"redis without its settings",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session:  SessionSpec{Backend: sessionbackend.BackendRedis},
			},
			"requires redis settings",
		},
		{
			"redis carrying postgres settings too",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend:  sessionbackend.BackendRedis,
					Redis:    &RedisSpec{URLRef: "env:URL"},
					Postgres: &PostgresSpec{DSNRef: "env:DSN"},
				},
			},
			"must not carry postgres settings",
		},
		{
			"redis without a url ref",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session:  SessionSpec{Backend: sessionbackend.BackendRedis, Redis: &RedisSpec{}},
			},
			"url_ref is required",
		},
		{
			"redis key prefix with a space",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendRedis,
					Redis:   &RedisSpec{URLRef: "env:URL", KeyPrefix: "tenant a"},
				},
			},
			"key prefix",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.profile.Validate()
			require.ErrorIs(t, err, ErrInvalidProfile)
			require.ErrorContains(t, err, tc.wants)

			// Nothing that fails to validate has a fingerprint. Content that
			// could never be built must not be recordable as if it had been.
			fingerprint, fingerprintErr := tc.profile.Fingerprint()
			require.ErrorIs(t, fingerprintErr, ErrInvalidProfile)
			require.Empty(t, fingerprint)
		})
	}
}

// A reference is a name, and the likeliest way to get this wrong is to paste
// the connection string in where its name belonged. When that happens the value
// must not come back in the error, which is where it would end up in a log.
func TestProfileValidateRejectsSecretsPastedInPlaceOfReferences(t *testing.T) {
	const dsn = "postgres://svc:hunter2@db.internal:5432/sessions"
	profile := Profile{
		TenantID: "tenant-a",
		ID:       "p1",
		Session: SessionSpec{
			Backend:  sessionbackend.BackendPostgres,
			Postgres: &PostgresSpec{DSNRef: dsn},
		},
	}

	err := profile.Validate()
	require.ErrorIs(t, err, ErrInvalidProfile)
	require.ErrorIs(t, err, secretref.ErrScheme)
	require.NotContains(t, err.Error(), "hunter2")
	require.NotContains(t, err.Error(), dsn)

	const url = "redis://:hunter2@cache.internal:6379/0"
	err = Profile{
		TenantID: "tenant-a",
		ID:       "p1",
		Session: SessionSpec{
			Backend: sessionbackend.BackendRedis,
			Redis:   &RedisSpec{URLRef: url},
		},
	}.Validate()
	require.ErrorIs(t, err, ErrInvalidProfile)
	require.NotContains(t, err.Error(), "hunter2")
	require.NotContains(t, err.Error(), url)
}

// The fingerprint is what the Router compares to decide whether a cached Bundle
// still answers the profile that asked for it, so it has to be a function of
// the content and of all of it.
func TestProfileFingerprintIsStableAndContentAddressed(t *testing.T) {
	profile := postgresProfile("tenant-a", "p1")

	first, err := profile.Fingerprint()
	require.NoError(t, err)
	second, err := profile.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Len(t, first, 64, "sha-256 rendered as hex")

	// A separate value with the same content fingerprints the same: the
	// comparison is on content, not on identity.
	same, err := postgresProfile("tenant-a", "p1").Fingerprint()
	require.NoError(t, err)
	require.Equal(t, first, same)

	for _, tc := range []struct {
		name    string
		changed Profile
	}{
		{"another tenant", postgresProfile("tenant-b", "p1")},
		{"another id", postgresProfile("tenant-a", "p2")},
		{"another backend", inMemoryProfile("tenant-a", "p1")},
		{
			"another dsn reference",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend:  sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{DSNRef: "env:OTHER_DSN"},
				},
			},
		},
		{
			"a schema added",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{
						DSNRef: "env:TENANT_A_SESSION_DSN",
						Schema: "tenant_a",
					},
				},
			},
		},
		{
			"a table prefix added",
			Profile{
				TenantID: "tenant-a",
				ID:       "p1",
				Session: SessionSpec{
					Backend: sessionbackend.BackendPostgres,
					Postgres: &PostgresSpec{
						DSNRef:      "env:TENANT_A_SESSION_DSN",
						TablePrefix: "sess_",
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed, err := tc.changed.Fingerprint()
			require.NoError(t, err)
			require.NotEqual(t, first, changed)
		})
	}
}

// A key prefix is part of the content too. Two redis profiles that differ only
// in where their keys land are different storage, and a fingerprint that missed
// it would let one be served from the other's Bundle.
func TestProfileFingerprintCoversRedisKeyPrefix(t *testing.T) {
	plain, err := redisProfile("tenant-a", "p1").Fingerprint()
	require.NoError(t, err)

	prefixed, err := Profile{
		TenantID: "tenant-a",
		ID:       "p1",
		Session: SessionSpec{
			Backend: sessionbackend.BackendRedis,
			Redis:   &RedisSpec{URLRef: "env:TENANT_A_SESSION_URL", KeyPrefix: "tenant-a"},
		},
	}.Fingerprint()
	require.NoError(t, err)
	require.NotEqual(t, plain, prefixed)
}

// tenant.ErrInvalidArgument is the platform's "bad request", and a profile that
// names no tenant has to arrive as one so callers above can map it to a status
// code without matching on this package's own sentinel.
func TestProfileValidateReportsInvalidArgument(t *testing.T) {
	err := Profile{ID: "p1", Session: SessionSpec{Backend: sessionbackend.BackendInMemory}}.
		Validate()
	require.ErrorIs(t, err, ErrInvalidProfile)
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
}
