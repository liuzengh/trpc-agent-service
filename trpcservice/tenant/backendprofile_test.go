package tenant_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// backendProfileConfigJSON is a config that names a backend profile, in the byte
// order RevisionConfig marshals to. It is a golden value for the same reason
// legacyRevisionConfigJSON is: the field is part of what a revision pins, so a
// change to its encoding has to fail here rather than at read time on every
// revision that uses it.
const backendProfileConfigJSON = `{"agent_name":"support-agent",` +
	`"instruction":"Help the user.",` +
	`"model":{"provider":"deterministic","name":"echo-v1",` +
	`"secret_ref":"env:LEGACY_KEY","temperature":0.5,"max_tokens":256},` +
	`"tool_refs":["orders.read"],` +
	`"backend_profile_id":"tenant-a-postgres"}`

// Validating the field must not have changed what a config without one encodes
// to.
//
// backend_profile_id is omitempty and was already declared before it was
// validated, so this is not a new risk — but it is the risk that matters here.
// A revision's stored ConfigDigest is re-checked on read, so a single extra byte
// in the encoding of a config that sets no profile would report every revision
// ever written as ErrConfigIntegrity.
func TestBackendProfileIDLeavesConfigsWithoutOneUntouched(t *testing.T) {
	var config tenant.RevisionConfig
	require.NoError(t, json.Unmarshal([]byte(legacyRevisionConfigJSON), &config))
	require.Equal(t, "", config.BackendProfileID)
	require.NoError(t, config.Validate(), "the empty profile id is the default store")

	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	require.Equal(t, legacyRevisionConfigJSON, string(encoded))
	require.NotContains(t, string(encoded), "backend_profile_id")

	digest, err := config.Digest()
	require.NoError(t, err)
	require.Equal(t, legacyRevisionConfigDigest, digest)
}

// A legal profile id round-trips, validates, and lands on a different digest
// than the same config without one: which storage a revision runs on is part of
// what the revision pins, not metadata beside it.
func TestBackendProfileIDRoundTripsAndChangesTheDigest(t *testing.T) {
	var config tenant.RevisionConfig
	require.NoError(t, json.Unmarshal([]byte(backendProfileConfigJSON), &config))
	require.Equal(t, "tenant-a-postgres", config.BackendProfileID)
	require.NoError(t, config.Validate())

	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	require.Equal(t, backendProfileConfigJSON, string(encoded))

	digest, err := config.Digest()
	require.NoError(t, err)
	require.NotEqual(t, legacyRevisionConfigDigest, digest)
}

// An id that could never name a profile is refused where the revision is
// written, not where it is served.
//
// It is the same rule every other resource id in this package is held to, and
// the reason to apply it here is that this one becomes a lookup key, a cache key
// and a singleflight key inside the storage router. Refusing it once, at the
// boundary, is the only place the answer is the same for every consumer.
func TestRevisionConfigRefusesAMalformedBackendProfileID(t *testing.T) {
	for _, profileID := range []string{
		"   ",
		"../../etc/passwd",
		"tenant-a/p1",
		"tenant-a\x00p1",
		"p1\n",
		"-leading-dash",
		".leading-dot",
		"postgres://user:hunter2@db.internal:5432/sessions",
		strings.Repeat("p", 129),
	} {
		t.Run(profileID, func(t *testing.T) {
			config := validRevisionConfig()
			config.BackendProfileID = profileID

			err := config.Validate()
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			// The rejected value never reaches the message. This error is
			// rendered straight into an HTTP body by the admin API, and the
			// likeliest way to get an illegal id here is a paste of something
			// that was not an id — the DSN in the table above is the case that
			// makes echoing it a disclosure rather than an untidiness.
			require.NotContains(t, err.Error(), profileID)
			require.Contains(t, err.Error(), "backend profile id")

			// Digest validates first, so an unservable config has no fingerprint
			// either — it cannot be recorded as if it had one.
			digest, digestErr := config.Digest()
			require.ErrorIs(t, digestErr, tenant.ErrInvalidArgument)
			require.Empty(t, digest)
		})
	}
}

// The rule is ValidateResourceID's, not a stricter local one.
//
// These are the ids the pattern really allows, asserted so the check above is
// known to be the shared rule rather than something that happens to reject the
// examples someone thought of. A profile id is compared byte for byte against
// what a source stores, so narrowing it here would make legal profiles
// unreachable.
func TestRevisionConfigAcceptsTheResourceIDsEveryOtherFieldAccepts(t *testing.T) {
	for _, profileID := range []string{
		"p1",
		"tenant-a-postgres",
		"Tenant_A.Postgres-1",
		"9",
		strings.Repeat("p", 128),
	} {
		t.Run(profileID, func(t *testing.T) {
			config := validRevisionConfig()
			config.BackendProfileID = profileID
			require.NoError(t, config.Validate())
			require.NoError(t, tenant.ValidateResourceID("backend profile id", profileID))
		})
	}
}

// Whitespace is not an absent value.
//
// Trimming first would make "  " mean "the process default", and the revision
// would then be served by storage it did not name — which is the one outcome
// the profile reference exists to prevent. The other string fields in this
// config are trimmed before they are checked; this one is not, on purpose.
func TestBlankBackendProfileIDIsNotTheDefaultStore(t *testing.T) {
	blank := validRevisionConfig()
	blank.BackendProfileID = "  "
	require.ErrorIs(t, blank.Validate(), tenant.ErrInvalidArgument)

	absent := validRevisionConfig()
	require.NoError(t, absent.Validate())
}

// A revision stored before this check existed, carrying an id that was never
// servable, stops validating once the process is upgraded.
//
// That is the intended direction. The storage router refuses such an id at
// resolve time, so the revision could not be served either way; what changes is
// where it is refused — at load, with the platform's own wording, instead of
// halfway into a request. It cannot be republished without being corrected.
func TestAStoredRevisionWithAnIllegalBackendProfileIDFailsClosed(t *testing.T) {
	stored := strings.Replace(
		backendProfileConfigJSON,
		`"backend_profile_id":"tenant-a-postgres"`,
		`"backend_profile_id":"../tenant-b-postgres"`,
		1,
	)
	var config tenant.RevisionConfig
	require.NoError(t, json.Unmarshal([]byte(stored), &config),
		"the bytes still parse; it is the content that is refused")

	require.ErrorIs(t, config.Validate(), tenant.ErrInvalidArgument)

	revision := tenant.AgentRevision{
		ID:         "revision-1",
		TenantID:   "tenant-a",
		AgentAppID: "assistant",
		RevisionNo: 1,
		Config:     config,
		Status:     tenant.RevisionStatusDraft,
		CreatedBy:  "admin",
	}
	require.ErrorIs(
		t,
		revision.ValidateForCreate(tenant.TenantContext{TenantID: "tenant-a"}),
		tenant.ErrInvalidArgument,
	)
}

// validRevisionConfig is the smallest config that validates, so a test that
// changes one field is testing that field.
func validRevisionConfig() tenant.RevisionConfig {
	return tenant.RevisionConfig{
		AgentName: "support-agent",
		Model: tenant.ModelConfig{
			Provider: "deterministic",
			Name:     "echo-v1",
		},
	}
}
