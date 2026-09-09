package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestRedactDriverErrorDropsPostgresPayload(t *testing.T) {
	const sensitive = "external-event-secret"
	driverError := &pgconn.PgError{
		Code:           uniqueViolation,
		ConstraintName: "channel_inbox_messages_event_key",
		Message:        "duplicate value " + sensitive,
		Detail:         "Key (external_event_id)=(" + sensitive + ") already exists",
		Where:          "statement carrying " + sensitive,
	}

	redacted := redactDriverError(driverError)
	require.NotContains(t, redacted.Error(), sensitive)
	require.Contains(t, redacted.Error(), uniqueViolation)
	require.Contains(t, redacted.Error(), driverError.ConstraintName)

	var reachable *pgconn.PgError
	require.False(t, errors.As(redacted, &reachable), "the original driver error must not remain unwrap-reachable")
}
