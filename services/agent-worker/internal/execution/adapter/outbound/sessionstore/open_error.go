package sessionstore

import (
	"errors"
	"regexp"

	"github.com/jackc/pgx/v5/pgconn"
)

// OpenError preserves only a closed operation stage and a validated SQLSTATE.
// It intentionally has no Unwrap: PostgreSQL messages, connection errors and
// DSNs may carry credentials or values and must not escape this adapter.
type OpenError struct {
	stage    string
	sqlState string
}

func (e *OpenError) Error() string {
	if e.sqlState != "" {
		return "session open failed at " + e.stage + " (SQLSTATE " + e.sqlState + ")"
	}
	return "session open failed at " + e.stage
}

// OpenFailure is the bounded diagnostic projection for an infrastructure
// observer. Identity, preparation and content sentinels retain their mapping.
func OpenFailure(err error) (stage, sqlState string, ok bool) {
	var failure *OpenError
	if !errors.As(err, &failure) || failure == nil {
		return "", "", false
	}
	return failure.stage, failure.sqlState, true
}

var sqlStatePattern = regexp.MustCompile(`^[0-9A-Z]{5}$`)

func openFailure(stage string, cause error) error {
	switch stage {
	case "parse_config", "connect", "target_query", "namespace_query", "table_query", "candidate_acl":
	default:
		panic("unknown internal session open stage")
	}
	failure := &OpenError{stage: stage}
	var pgError *pgconn.PgError
	if errors.As(cause, &pgError) && pgError != nil && sqlStatePattern.MatchString(pgError.Code) {
		failure.sqlState = pgError.Code
	}
	return failure
}
