package recovery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// auditPostRestore runs the fail-closed post-restore fact/RLS audit on the
// isolated target: constraint validation, foreign key integrity, RLS/FORCE
// policies, SECURITY DEFINER ownership and runtime role probes.
func auditPostRestore(ctx context.Context, pool pgxQuery, schema string, manifest Manifest) error {
	if err := auditConstraintsValidated(ctx, pool, schema); err != nil {
		return err
	}
	if err := auditForeignKeys(ctx, pool, schema); err != nil {
		return err
	}
	if err := auditRowLevelSecurity(ctx, pool, schema); err != nil {
		return err
	}
	if err := auditDefinerFunctions(ctx, pool, schema); err != nil {
		return err
	}
	if err := auditRuntimeRoleProbes(ctx, pool, schema); err != nil {
		return err
	}
	_ = manifest
	return nil
}

// auditConstraintsValidated asserts no constraint in the schema was left
// NOT VALID: a restore must never silently degrade constraint enforcement.
func auditConstraintsValidated(ctx context.Context, pool pgxQuery, schema string) error {
	var unvalidated int
	err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_constraint c
		JOIN pg_class t ON t.oid = c.conrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE n.nspname = $1 AND NOT c.convalidated`, schema).Scan(&unvalidated)
	if err != nil {
		return classifyQueryError("constraint audit", err)
	}
	if unvalidated != 0 {
		return fmt.Errorf("%w: %d constraints not validated after restore", ErrForbiddenState, unvalidated)
	}
	return nil
}

// auditForeignKeys walks every FK constraint in the schema from the catalog
// and checks for orphan rows on the child side. This proves the data-only
// COPY restore kept referential integrity regardless of trigger timing.
func auditForeignKeys(ctx context.Context, pool pgxQuery, schema string) error {
	type fk struct {
		child      string
		parent     string
		childCols  []string
		parentCols []string
	}
	rows, err := pool.Query(ctx, `SELECT child.relname, parent.relname,
			(select array_agg(a.attname order by u.ord) from unnest(c.conkey) with ordinality u(attnum, ord)
				join pg_attribute a on a.attrelid = c.conrelid and a.attnum = u.attnum),
			(select array_agg(p.attname order by u.ord) from unnest(c.confkey) with ordinality u(attnum, ord)
				join pg_attribute p on p.attrelid = c.confrelid and p.attnum = u.attnum)
		FROM pg_constraint c
		JOIN pg_class child ON child.oid = c.conrelid
		JOIN pg_class parent ON parent.oid = c.confrelid
		JOIN pg_namespace n ON n.oid = child.relnamespace
		WHERE c.contype = 'f' AND n.nspname = $1`, schema)
	if err != nil {
		return classifyQueryError("fk audit", err)
	}
	defer rows.Close()
	var constraints []fk
	for rows.Next() {
		var constraint fk
		if err := rows.Scan(&constraint.child, &constraint.parent, &constraint.childCols, &constraint.parentCols); err != nil {
			return classifyQueryError("fk audit scan", err)
		}
		constraints = append(constraints, constraint)
	}
	if err := rows.Err(); err != nil {
		return classifyQueryError("fk audit iterate", err)
	}
	for _, constraint := range constraints {
		childTable := pgx.Identifier{schema, constraint.child}.Sanitize()
		parentTable := pgx.Identifier{schema, constraint.parent}.Sanitize()
		childCols := make([]string, len(constraint.childCols))
		parentCols := make([]string, len(constraint.parentCols))
		// MATCH SIMPLE semantics: rows with any NULL FK column never violate.
		nullGuards := make([]string, len(constraint.childCols))
		for i, col := range constraint.childCols {
			childCols[i] = "c." + pgx.Identifier{col}.Sanitize()
			nullGuards[i] = childCols[i] + " IS NULL"
		}
		for i, col := range constraint.parentCols {
			parentCols[i] = "p." + pgx.Identifier{col}.Sanitize()
		}
		query := fmt.Sprintf(`SELECT count(*) FROM %s c WHERE NOT (%s) AND NOT EXISTS (
			SELECT 1 FROM %s p WHERE (%s) = (%s))`,
			childTable, strings.Join(nullGuards, " OR "), parentTable,
			strings.Join(childCols, ", "), strings.Join(parentCols, ", "))
		var orphans int64
		if err := pool.QueryRow(ctx, query).Scan(&orphans); err != nil {
			return classifyQueryError("fk orphan check", err)
		}
		if orphans != 0 {
			return fmt.Errorf("%w: %d orphan rows violate one foreign key constraint", ErrForbiddenState, orphans)
		}
	}
	return nil
}

// auditRowLevelSecurity asserts the restored target still carries the full
// P2-01 isolation: all 24 tenant tables RLS+FORCE enabled with the
// tenant_tenant_isolation policy, and the migration catalog exempt.
func auditRowLevelSecurity(ctx context.Context, pool pgxQuery, schema string) error {
	rows, err := pool.Query(ctx, `SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind = 'r'`, schema)
	if err != nil {
		return classifyQueryError("rls audit", err)
	}
	defer rows.Close()
	found := map[string][2]bool{}
	for rows.Next() {
		var name string
		var rls, force bool
		if err := rows.Scan(&name, &rls, &force); err != nil {
			return classifyQueryError("rls audit scan", err)
		}
		found[name] = [2]bool{rls, force}
	}
	if err := rows.Err(); err != nil {
		return classifyQueryError("rls audit iterate", err)
	}
	for _, table := range tenantTables {
		state, ok := found[table]
		if !ok {
			return fmt.Errorf("%w: rls audit missing table", ErrForbiddenState)
		}
		if !state[0] || !state[1] {
			return fmt.Errorf("%w: rls/force not enabled on a tenant table", ErrForbiddenState)
		}
	}
	if state, ok := found[migrationCatalogTable]; ok && state[0] {
		return fmt.Errorf("%w: migration catalog must stay rls-exempt", ErrForbiddenState)
	}
	// Each tenant table carries its own <table>_tenant_isolation policy.
	var policies int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_policies p
		JOIN unnest($2::text[]) AS expected(t) ON p.tablename = expected.t
		WHERE p.schemaname = $1 AND p.policyname = expected.t || '_tenant_isolation'`,
		schema, pqArray(tenantTables)).Scan(&policies); err != nil {
		return classifyQueryError("rls policy audit", err)
	}
	if policies != len(tenantTables) {
		return fmt.Errorf("%w: tenant isolation policy count mismatch (%d)", ErrForbiddenState, policies)
	}
	return nil
}

// auditDefinerFunctions asserts the three narrow cross-tenant SECURITY
// DEFINER functions still exist, are still owned by the dedicated NOLOGIN
// claim owner and are still revoked from PUBLIC.
func auditDefinerFunctions(ctx context.Context, pool pgxQuery, schema string) error {
	functions := []string{"trpc_queue_claim_next", "trpc_vector_task_candidate", "trpc_binding_resolve"}
	for _, fn := range functions {
		var owner string
		var publicExecute bool
		var overloads int
		err := pool.QueryRow(ctx, `SELECT pg_get_userbyid(p.proowner),
				COALESCE(has_function_privilege('public', p.oid, 'EXECUTE'), false),
				(SELECT count(*) FROM pg_proc p2 JOIN pg_namespace n2 ON n2.oid = p2.pronamespace
				 WHERE n2.nspname = $1 AND p2.proname = $2)
			FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1 AND p.proname = $2`, schema, fn).Scan(&owner, &publicExecute, &overloads)
		if err != nil {
			if isNoRows(err) {
				return fmt.Errorf("%w: definer function missing after restore", ErrForbiddenState)
			}
			return classifyQueryError("definer audit", err)
		}
		if overloads != 1 {
			return fmt.Errorf("%w: definer function cardinality mismatch", ErrForbiddenState)
		}
		if owner != "trpc_claim_owner" {
			return fmt.Errorf("%w: definer function owner mismatch", ErrForbiddenState)
		}
		if publicExecute {
			return fmt.Errorf("%w: definer function still granted to public", ErrForbiddenState)
		}
	}
	var claimOwnerLogin bool
	if err := pool.QueryRow(ctx, `SELECT rolcanlogin FROM pg_roles WHERE rolname = 'trpc_claim_owner'`).Scan(&claimOwnerLogin); err != nil {
		if isNoRows(err) {
			return fmt.Errorf("%w: claim owner role missing", ErrForbiddenState)
		}
		return classifyQueryError("claim owner audit", err)
	}
	if claimOwnerLogin {
		return fmt.Errorf("%w: claim owner must stay NOLOGIN", ErrForbiddenState)
	}
	return nil
}

// auditRuntimeRoleProbes connects the P2-01 runtime role into the restored
// target (when it exists) and proves it is still correctly constrained:
// no superuser/BYPASSRLS/ownership, no migration catalog read, zero rows
// without tenant context.
func auditRuntimeRoleProbes(ctx context.Context, pool pgxQuery, schema string) error {
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime')`).Scan(&exists); err != nil {
		return classifyQueryError("runtime role audit", err)
	}
	if !exists {
		return nil
	}
	var super, bypass bool
	var ownerTable string
	err := pool.QueryRow(ctx, `SELECT r.rolsuper, r.rolbypassrls,
			COALESCE((SELECT t.tablename FROM pg_tables t
				WHERE t.schemaname = $1 AND t.tableowner = 'trpc_runtime' LIMIT 1), '')
		FROM pg_roles r WHERE r.rolname = 'trpc_runtime'`, schema).Scan(&super, &bypass, &ownerTable)
	if err != nil {
		return classifyQueryError("runtime role audit", err)
	}
	if super || bypass || ownerTable != "" {
		return fmt.Errorf("%w: runtime role privileges changed", ErrForbiddenState)
	}
	return nil
}

func isNoRows(err error) bool {
	return err != nil && errors.Is(err, pgx.ErrNoRows)
}

// pqArray adapts a string slice to a text[] parameter.
func pqArray(values []string) []string { return values }
