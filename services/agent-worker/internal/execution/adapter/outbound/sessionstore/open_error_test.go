package sessionstore

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestOpenFailureContainsOnlyClosedStageAndSQLState(t *testing.T) {
	for _, code := range []string{"28P01", "42501", "secret", "42\n01", "42001:password"} {
		cause := fmt.Errorf("dsn=postgres://private-password:secret@host: %w", &pgconn.PgError{Code: code, Message: "private-password", Detail: "credential-body"})
		err := openFailure("target_query", cause)
		stage, state, ok := OpenFailure(err)
		if !ok || stage != "target_query" || (sqlStatePattern.MatchString(code) && state != code) || (!sqlStatePattern.MatchString(code) && state != "") {
			t.Fatalf("bounded failure projection = %q %q %v", stage, state, ok)
		}
		if strings.Contains(err.Error(), "private-password") || strings.Contains(err.Error(), "credential-body") || strings.Contains(err.Error(), "dsn=") || errors.Unwrap(err) != nil {
			t.Fatal("raw diagnostic escaped session adapter")
		}
	}
	if stage, state, ok := OpenFailure(ErrIdentity); ok || stage != "" || state != "" {
		t.Fatal("existing identity mapping changed")
	}
	if stage, state, ok := OpenFailure(openFailure("candidate_acl", nil)); !ok || stage != "candidate_acl" || state != "" {
		t.Fatal("false ACL predicate diagnostic lost")
	}
}
