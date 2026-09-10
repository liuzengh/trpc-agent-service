package migrations

import (
	"context"
	"errors"
	"testing"
)

func TestMySQLMigrationEntryPointsRejectInvalidContexts(t *testing.T) {
	if err := ApplyMySQL(nil, nil); !errors.Is(err, ErrMigration) {
		t.Fatalf("Apply nil = %v", err)
	}
	if err := VerifyMySQL(nil, nil); !errors.Is(err, ErrMigration) {
		t.Fatalf("Verify nil = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ApplyMySQL(ctx, nil); !errors.Is(err, ErrMigration) {
		t.Fatalf("Apply cancelled nil db = %v", err)
	}
	if err := VerifyMySQL(ctx, nil); !errors.Is(err, ErrMigration) {
		t.Fatalf("Verify cancelled nil db = %v", err)
	}
}

func TestSplitMySQLStatementsHandlesTrailingQuotesAndComments(t *testing.T) {
	for _, script := range []string{"SELECT 'unterminated", "SELECT \\\"unterminated", "SELECT 'escaped\\\\quote'", "SELECT 'doubled''quote'", "-- comment", "/* comment"} {
		_ = splitMySQLStatements(script)
	}
}
