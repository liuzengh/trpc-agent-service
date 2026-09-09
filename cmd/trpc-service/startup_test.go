package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
)

// runTestAdminKey is a syntactically valid admin credential for tests that need
// the security configuration to load so they can assert something further along.
// It authenticates nothing: no server in this package is ever handed it.
const runTestAdminKey = "cmd-test-admin-key-0123456789abcdef"

// The startup order is a security property, so it is asserted rather than
// documented.
//
// The environment below is wrong in two independent ways at once: the security
// manifest names a file that does not exist, and the storage profile asks for
// postgres with no DSN. Only one of them can be reported first, and it has to be
// the security one — a process whose credentials are misconfigured must not have
// opened a pool, run a migration against a shared database, or dialled Redis on
// its way to finding that out.
//
// The stub records every storage constructor it is asked for, so "storage was
// not reached" is a positive observation and not an inference from the error
// message.
func TestRunLoadsSecurityBeforeStorage(t *testing.T) {
	absentManifest := filepath.Join(t.TempDir(), "security.json")
	env := map[string]string{
		security.ConfigFileEnvVar: absentManifest,
		storageProfileEnvVar:      string(profilePostgres),
	}
	stub := &stubStorage{}

	err := runWith("127.0.0.1:0", func(name string) string { return env[name] }, stub.deps())

	require.ErrorContains(t, err, security.ConfigFileEnvVar)
	require.NotErrorIs(t, err, errStorageConfig)
	require.Empty(t, stub.steps, "a storage constructor ran before the security configuration was checked")

	// The control: with the same broken storage configuration and a security
	// configuration that loads, the storage refusal is what comes back. Without
	// this, the assertion above would also pass if runWith had simply refused
	// everything for some unrelated reason.
	env[security.ConfigFileEnvVar] = ""
	env[security.AdminAPIKeyEnvVar] = runTestAdminKey
	stub = &stubStorage{}

	err = runWith("127.0.0.1:0", func(name string) string { return env[name] }, stub.deps())

	require.ErrorIs(t, err, errStorageConfig)
	require.ErrorContains(t, err, postgresDSNEnvVar)
	require.Empty(t, stub.steps, "storage was opened despite an invalid storage configuration")
}

// The listen address is checked before either of them. It is the only thing
// keeping the control plane off the network, and a process that had already
// connected to a database before deciding it must not run is a process that
// touched shared state it had no business touching.
func TestRunValidatesTheListenAddressFirst(t *testing.T) {
	env := map[string]string{
		security.ConfigFileEnvVar: filepath.Join(t.TempDir(), "security.json"),
		storageProfileEnvVar:      string(profilePostgres),
	}
	stub := &stubStorage{}

	err := runWith("0.0.0.0:8080", func(name string) string { return env[name] }, stub.deps())

	require.ErrorContains(t, err, "refusing to listen")
	require.NotErrorIs(t, err, errStorageConfig)
	require.Empty(t, stub.steps)
}
