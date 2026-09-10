package migrations

import (
	"regexp"
	"strings"
	"testing"
)

func TestBaselineForcesRLSOnEveryTenantTable(t *testing.T) {
	tablePattern := regexp.MustCompile(`(?s)CREATE TABLE\s+([A-Za-z0-9_]+)\s*\((.*?)\n\);`)
	policyPattern := regexp.MustCompile(`(?is)CREATE POLICY\s+[A-Za-z0-9_]+\s+ON\s+([A-Za-z0-9_]+)`)
	policies := make(map[string]bool)
	for _, match := range policyPattern.FindAllStringSubmatch(baselineSQL, -1) {
		policies[strings.ToLower(match[1])] = true
	}
	for _, match := range tablePattern.FindAllStringSubmatch(baselineSQL, -1) {
		table, body := strings.ToLower(match[1]), strings.ToLower(match[2])
		if !regexp.MustCompile(`\btenant_id\b`).MatchString(body) {
			continue
		}
		enable := "alter table " + table + " enable row level security"
		force := "alter table " + table + " force row level security"
		lower := strings.ToLower(baselineSQL)
		if !strings.Contains(lower, enable) {
			t.Errorf("tenant table %s does not ENABLE RLS", table)
		}
		if !strings.Contains(lower, force) {
			t.Errorf("tenant table %s does not FORCE RLS", table)
		}
		if !policies[table] {
			t.Errorf("tenant table %s has no RLS policy", table)
		}
	}
}

func TestSplitStatementsIgnoresSemicolonsInComments(t *testing.T) {
	t.Parallel()
	source := `-- RLS is defence in depth. Tenant-scoped clients set app.tenant_id;
-- the platform routing role remains the cross-tenant access point.
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_tenants ON tenants
    USING (id = current_setting('app.tenant_id', true));`

	statements := splitStatements(source)
	if len(statements) != 2 {
		t.Fatalf("statements = %d, want 2: %#v", len(statements), statements)
	}
	if !strings.Contains(statements[0], "ALTER TABLE tenants ENABLE ROW LEVEL SECURITY") {
		t.Fatalf("statement[0] = %q, want ALTER TABLE statement", statements[0])
	}
	if !strings.Contains(statements[1], "CREATE POLICY tenant_scope_tenants ON tenants") {
		t.Fatalf("statement[1] = %q, want CREATE POLICY statement", statements[1])
	}
}

func TestSplitStatementsKeepsSemicolonsInStringLiterals(t *testing.T) {
	t.Parallel()
	source := "INSERT INTO t (v) VALUES ('a;b'); SELECT 1;"

	statements := splitStatements(source)
	if len(statements) != 2 {
		t.Fatalf("statements = %d, want 2: %#v", len(statements), statements)
	}
	if want := "INSERT INTO t (v) VALUES ('a;b')"; statements[0] != want {
		t.Fatalf("statement[0] = %q, want %q", statements[0], want)
	}
	if want := "SELECT 1"; statements[1] != want {
		t.Fatalf("statement[1] = %q, want %q", statements[1], want)
	}
}

func TestSplitStatementsDropsBlankFragments(t *testing.T) {
	t.Parallel()
	source := "SELECT 1;\n\nSELECT 2;;"

	statements := splitStatements(source)
	if len(statements) != 2 {
		t.Fatalf("statements = %d, want 2: %#v", len(statements), statements)
	}
}

func TestSplitStatementsKeepsSemicolonsInDoubleQuotedIdentifiers(t *testing.T) {
	t.Parallel()
	statements := splitStatements(`CREATE TABLE "audit;log" (id BIGINT); SELECT 1;`)
	if len(statements) != 2 {
		t.Fatalf("statements = %d, want 2: %#v", len(statements), statements)
	}
	if want := `CREATE TABLE "audit;log" (id BIGINT)`; statements[0] != want {
		t.Fatalf("statement[0] = %q, want %q", statements[0], want)
	}
}

func TestSplitStatementsKeepsSemicolonsInDollarQuotedBodies(t *testing.T) {
	t.Parallel()
	source := "CREATE FUNCTION f() RETURNS void AS $$ BEGIN PERFORM 1; END; $$ LANGUAGE plpgsql; SELECT 1;"
	statements := splitStatements(source)
	if len(statements) != 2 {
		t.Fatalf("statements = %d, want 2: %#v", len(statements), statements)
	}
	if !strings.Contains(statements[0], "PERFORM 1; END;") {
		t.Fatalf("statement[0] = %q, want complete dollar-quoted body", statements[0])
	}
}

func TestSplitStatementsIgnoresNestedBlockCommentSemicolons(t *testing.T) {
	t.Parallel()
	source := "/* outer; /* inner; */ still outer; */ SELECT 1;"
	statements := splitStatements(source)
	if len(statements) != 1 || !strings.Contains(statements[0], "SELECT 1") {
		t.Fatalf("statements = %#v, want one SELECT statement", statements)
	}
}

func TestSplitStatementsSkipsCommentOnlyFragments(t *testing.T) {
	t.Parallel()
	source := "-- leading comment;\n/* block comment; */;\nSELECT 1;\n-- trailing comment;\n"
	statements := splitStatements(source)
	if len(statements) != 1 || statements[0] != "SELECT 1" {
		t.Fatalf("statements = %#v, want only SELECT 1", statements)
	}
}
