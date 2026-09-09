package postgres

import "github.com/liuzengh/trpc-agent-service/trpcservice/channels"

// The four platform scans — two recovery, two dispatch — take an optional
// channels.ScanScope, and this is how it reaches the database.
//
// One statement text serves both the scoped and the unscoped scan. The
// alternative, a second text built when a scope is present, would double the
// number of statements a connection keeps in its prepared-statement cache for
// no change in what any of them returns; binding NULL instead switches the
// predicate off, because NULL IS NULL is true and the comparison beside it is
// never reached. The casts are what let the driver send an untyped NULL: with
// no cast PostgreSQL cannot infer a type for a parameter that appears only
// beside another copy of itself.
//
// Every scan therefore binds the scope as $1 and $2 and numbers its own
// parameters from $3. That is the one thing to keep in mind when editing those
// statements, and the reason the fragments below are the only place either
// column name is spelled.
const (
	scopeFilterSQL = `
		AND ($1::text IS NULL OR tenant_id = $1::text)
		AND ($2::text IS NULL OR channel_binding_id = $2::text)`

	// aliasedScopeFilterSQL is the same filter for a statement whose outer row
	// is o.
	aliasedScopeFilterSQL = `
		AND ($1::text IS NULL OR o.tenant_id = $1::text)
		AND ($2::text IS NULL OR o.channel_binding_id = $2::text)`
)

// scopeArgs returns the two values a scan binds for the filter above. An absent
// scope binds NULL twice, which is what makes the filter a no-op and keeps the
// platform scan crossing tenants exactly as it did before scoping existed.
//
// The scope is not re-validated here. It arrived inside a request whose
// Validate already refused a half-filled one, and these values are bind
// parameters rather than statement text.
func scopeArgs(scope channels.ScanScope) (any, any) {
	if !scope.Scoped() {
		return nil, nil
	}
	return scope.TenantID, scope.BindingID
}
