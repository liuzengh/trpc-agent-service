package postgresadapter

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCommitTraceOutcomeSeparatesUnknown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		attempted bool
		want      string
	}{
		{"accepted", nil, true, "accepted"}, {"precommit", errors.New("body canary"), false, "failed"}, {"server rejection", &pgconn.PgError{Code: "23514", Message: "body canary"}, true, "failed"}, {"rollback", pgx.ErrTxCommitRollback, true, "failed"}, {"lost response", errors.New("network body canary"), true, "UNKNOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := completionOutcome(tc.err, tc.attempted); got != tc.want {
				t.Fatal(got)
			}
		})
	}
}
