// Package asset holds the ownership vocabulary shared by every tenant asset
// (knowledge base, skill, channel binding, model endpoint).
//
// An asset records who created it and whether its author has shared it with the
// tenant. The author owns the row; tenant managers (admin/owner) manage every
// row of their tenant. Package asset deliberately owns only the vocabulary —
// who may do what is decided by the web layer from the authenticated claims,
// because that rule also depends on the caller's tenant and role.
package asset

// Visibility values stored in each asset row.
const (
	// VisibilityPrivate means the row is visible to its author and to tenant
	// managers only. This is the default: creating an asset does not publish it.
	VisibilityPrivate = "private"
	// VisibilityShared means the author has published the row to the tenant:
	// every member may read it, but only the author (or a tenant manager) may
	// change or delete it.
	VisibilityShared = "shared"
)

// VisibilityOrDefault normalizes a stored/incoming value, defaulting to private.
// An unrecognized value is treated as private rather than as shared: a typo must
// never widen access.
func VisibilityOrDefault(v string) string {
	if v == VisibilityShared {
		return VisibilityShared
	}
	return VisibilityPrivate
}

// Shared reports whether the visibility makes the row tenant-readable.
func Shared(v string) bool { return v == VisibilityShared }
