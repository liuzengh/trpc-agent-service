package controlplane

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestEveryScopedQueryCarriesATenantPredicate is the check the package doc
// promises: Scope/TxScope do not silently rewrite SQL, so this walks this
// package's own Go source instead and fails if any query passed to a scoped
// Exec/Query/QueryRow is missing a tenant predicate.
//
// It is a build-time lint, not a runtime guard. It cannot see a query
// assembled from anything other than literals and simple concatenation — a
// table name picked by a switch over constants resolves fine, a table name
// that came from a request would not, and nothing here would notice. That is
// a real limitation: integration_test.go's cross-tenant rejection tests are
// the backstop, because they exercise the database's own answer to a query
// that got this wrong, rather than reading the query's source text.
func TestEveryScopedQueryCarriesATenantPredicate(t *testing.T) {
	// A query has to mention tenant_id somewhere to be tenant-scoped at all:
	// as `tenant_id = ?` in a WHERE clause, or as a column in an INSERT's
	// column list. Checking for either shape catches the real failure mode —
	// a query that forgot the tenant entirely. It does not check that the
	// predicate is placed correctly or that its bound value is this Scope's
	// own; that is integration_test.go's cross-tenant rejection test.
	tenantPredicate := regexp.MustCompile(`(?i)\btenant_id\b`)

	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked, skipped := 0, 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			constants := collectLocalStrings(fn)
			ast.Inspect(fn, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "Exec", "Query", "QueryRow":
				default:
					return true
				}
				// Only scoped receivers count. A raw *sql.DB / *sql.Tx call
				// inside this package (e.g. the DB-level CreateTenant
				// exception) is not this test's business.
				if !isScopedReceiver(sel.X) {
					return true
				}
				if len(call.Args) < 2 {
					return true
				}
				q, ok := resolveString(call.Args[1], constants)
				if !ok {
					// Not a shape this lint can reason about; counted so the
					// blind spot stays visible in the test output.
					skipped++
					return true
				}
				checked++
				if !tenantPredicate.MatchString(q) && !isIntentionallyUnscoped(q) {
					t.Errorf("%s: scoped %s is missing a tenant_id = ? predicate:\n%s",
						name, sel.Sel.Name, q)
				}
				return true
			})
			return true
		})
	}
	if checked == 0 {
		t.Fatal("the test found no scoped queries to check; it has stopped testing anything")
	}
	t.Logf("checked %d scoped queries, %d skipped as unresolvable", checked, skipped)
}

// isIntentionallyUnscoped is the audit trail for every exception to the rule,
// not a way to widen it silently: adding a query here requires a reason.
//
// schema_migrations never goes through Scope at all, so it is not listed.
func isIntentionallyUnscoped(query string) bool {
	return false
}

// isScopedReceiver recognizes the local names this package uses for its two
// scope types. A method value stored under some other name and called later
// would slip past, which is why the naming convention is not to do that.
func isScopedReceiver(expr ast.Expr) bool {
	switch v := expr.(type) {
	case *ast.Ident:
		return v.Name == "s" || v.Name == "tx" || v.Name == "t" || v.Name == "scope"
	case *ast.SelectorExpr:
		return isScopedReceiver(v.X)
	case *ast.ParenExpr:
		return isScopedReceiver(v.X)
	}
	return false
}

// collectLocalStrings maps every `name := <string expression>` in a function
// body to its value, where that expression is built only out of string
// literals and +. That covers the two helpers in this package that pick a
// table name from a fixed set of constants — the shape that would otherwise
// be invisible to a check that only reads call sites.
func collectLocalStrings(fn *ast.FuncDecl) map[string]string {
	out := make(map[string]string)
	ast.Inspect(fn, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		lhs, ok := as.Lhs[0].(*ast.Ident)
		if !ok {
			return true
		}
		if v, ok := resolveString(as.Rhs[0], out); ok {
			out[lhs.Name] = v
		}
		return true
	})
	return out
}

func resolveString(expr ast.Expr, constants map[string]string) (string, bool) {
	switch v := expr.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(v.Value)
		if err != nil {
			return "", false
		}
		return s, true
	case *ast.Ident:
		s, ok := constants[v.Name]
		return s, ok
	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			return "", false
		}
		left, ok := resolveString(v.X, constants)
		if !ok {
			return "", false
		}
		right, ok := resolveString(v.Y, constants)
		if !ok {
			return "", false
		}
		return left + right, true
	case *ast.ParenExpr:
		return resolveString(v.X, constants)
	}
	return "", false
}
