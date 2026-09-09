package postgres

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These assertions are about SQL text, which is usually not worth pinning. Each
// one here is a decision that cannot be observed from outside the package
// without a database, and that would fail silently or expensively if it drifted.

// The profile table has to be part of the same migration as the rest of the
// control plane. A second migration path would mean a deployment that had run
// one and not the other, and a control plane whose profiles table exists only
// sometimes is one the admin API cannot depend on.
func TestMigrationsCreateTheBackendProfileTable(t *testing.T) {
	var statement string
	for _, candidate := range migrations {
		if strings.Contains(candidate, "backend_profiles") {
			statement = candidate
			break
		}
	}
	require.NotEmpty(t, statement, "the profile table is missing from the migration")

	require.Contains(t, statement, "CREATE TABLE IF NOT EXISTS",
		"every statement in this migration has to be safe to run twice")

	// The constraint names are the only thing that tells a duplicate id apart
	// from a missing tenant, and both are mapped by name in profiles.go.
	require.Contains(t, statement, constraintProfilesPrimaryKey)
	require.Contains(t, statement, constraintProfilesTenant)

	// The primary key is (tenant_id, id), so no lookup can reach another
	// tenant's row by guessing a profile id.
	require.Contains(t, statement, "PRIMARY KEY (tenant_id, id)")

	// The foreign key has no ON DELETE clause: deleting a tenant that still owns
	// profiles must fail rather than quietly take the profiles with it.
	require.NotContains(t, statement, "ON DELETE")
}

// The id is the version, and that is a property of the storage rather than of
// the Go code in front of it: no statement in this package may update or delete
// a profile row.
func TestNoStatementRewritesAProfile(t *testing.T) {
	for _, statement := range []string{
		insertProfileSQL, selectProfileSQL, listProfilesSQL,
		countProfilesSQL, selectTenantForProfileWriteSQL,
	} {
		upper := strings.ToUpper(statement)
		require.NotContains(t, upper, "UPDATE BACKEND_PROFILES")
		require.NotContains(t, upper, "DELETE FROM BACKEND_PROFILES")
	}
	for _, statement := range migrations {
		require.NotContains(t, strings.ToUpper(statement), "ON UPDATE")
	}
}

// The list order is part of the ProfileRepository contract and the in-memory
// implementation sorts Go strings, which is byte order. A database created with
// a locale-aware default collation sorts differently, so the collation is named
// rather than inherited.
func TestListProfilesOrdersByByteValue(t *testing.T) {
	require.Contains(t, listProfilesSQL, `ORDER BY id COLLATE "C"`)
}

// FOR NO KEY UPDATE, not FOR UPDATE. It conflicts with itself, which serialises
// concurrent profile creates so the count and the insert are one decision, but
// it does not conflict with the FOR KEY SHARE that app and revision inserts take
// on the same tenant row — so creating a profile does not block them.
func TestProfileCreateLocksTheTenantRowWithoutBlockingItsForeignKeys(t *testing.T) {
	require.Contains(t, selectTenantForProfileWriteSQL, "FOR NO KEY UPDATE")

	// The lock and the count have to be in the same statement sequence against
	// the same tenant row, which is what makes the limit enforceable. The count
	// is scoped to one tenant for the same reason.
	require.Contains(t, countProfilesSQL, "WHERE tenant_id = $1")
}
