package application

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestDomainAndApplicationHaveNoTransportOrPersistenceImports(t *testing.T) {
	for _, dir := range []string{".", "../domain"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, imp := range file.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if strings.Contains(path, "pgx") || strings.Contains(path, "gin-gonic") || strings.Contains(path, "/adapter/") || strings.Contains(path, "/infra/") || strings.HasPrefix(path, "database/") {
					t.Errorf("inner package imports infrastructure: %s %s", entry.Name(), path)
				}
			}
		}
	}
}
