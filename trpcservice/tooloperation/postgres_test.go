package tooloperation

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestPostgresDurationIntervalNeverShortensPositiveDeadline(t *testing.T) {
	tests := []struct {
		name         string
		duration     time.Duration
		microseconds int64
	}{
		{name: "zero", duration: 0, microseconds: 0},
		{name: "sub-microsecond positive", duration: time.Nanosecond, microseconds: 1},
		{name: "exact microsecond", duration: time.Microsecond, microseconds: 1},
		{name: "positive remainder", duration: time.Microsecond + time.Nanosecond, microseconds: 2},
		{name: "negative remainder", duration: -time.Microsecond - time.Nanosecond, microseconds: -2},
		{name: "minute", duration: time.Minute, microseconds: 60_000_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			interval := durationInterval(test.duration)
			if !interval.Valid || interval.Microseconds != test.microseconds ||
				interval.Days != 0 || interval.Months != 0 {
				t.Fatalf("durationInterval(%s) = %+v, want %d microseconds", test.duration, interval, test.microseconds)
			}
		})
	}
}

func TestPostgresResolutionReplayUsesPersistedTimestampPrecision(t *testing.T) {
	retryAt := time.Date(2026, 8, 31, 10, 0, 0, 123456789, time.UTC)
	req := ResolveRequest{
		ResolutionID: "resolution-1", TenantID: "tenant-a", OperationKey: "operation-1",
		ExpectedVersion: 7, Action: ResolveRetryNotApplied,
		ActorHash: HashPayload([]byte("actor")), ReasonCode: "proved_absent",
		RetryAt: retryAt,
	}
	stored := Resolution{
		ResolutionID: req.ResolutionID, TenantID: req.TenantID, OperationKey: req.OperationKey,
		FromVersion: req.ExpectedVersion, ToVersion: req.ExpectedVersion + 1,
		Action: req.Action, ActorHash: req.ActorHash, ReasonCode: req.ReasonCode,
		RetryAt: retryAt.Truncate(time.Microsecond),
	}
	if !samePostgresResolutionRequest(stored, req) {
		t.Fatal("sub-microsecond timestamptz truncation broke idempotent resolution replay")
	}
	changed := req
	changed.ReasonCode = "different_reason"
	if samePostgresResolutionRequest(stored, changed) {
		t.Fatal("changed resolution request was treated as idempotent")
	}
}

func TestPostgresSchemaPrivacyAndTenantKeyContract(t *testing.T) {
	schema := strings.ToLower(PostgreSQLSchemaDDL)
	for _, required := range []string{
		"primary key (tenant_id, operation_key)",
		"primary key (tenant_id, operation_key, attempt_no)",
		"primary key (tenant_id, resolution_id)",
		"foreign key (tenant_id, operation_key)",
		"lease_owner_hash text",
		"payload_hash text",
		"result_hash text",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("PostgreSQL schema is missing contract fragment %q", required)
		}
	}
	for _, forbidden := range []string{
		"raw_args text", "raw_arguments text", "raw_payload text",
		"provider_response text", "response_body text", "error_message text",
		"lease_owner text",
	} {
		if strings.Contains(schema, forbidden) {
			t.Fatalf("PostgreSQL schema persists forbidden field %q", forbidden)
		}
	}
}

func TestPostgresSQLSourceExactLeaseAndIntervalContract(t *testing.T) {
	sourceBytes, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	exactStart := strings.Index(source, "func (p *Postgres) LeaseOperation(")
	exactEnd := strings.Index(source, "func (p *Postgres) Renew(")
	if exactStart < 0 || exactEnd <= exactStart {
		t.Fatal("cannot locate LeaseOperation SQL implementation")
	}
	exact := source[exactStart:exactEnd]
	for _, required := range []string{
		"WHERE tenant_id = $1 AND operation_key = $2",
		"FOR UPDATE",
		"State(state) != StateReserved && State(state) != StateRetryableNotApplied",
		"INSERT INTO tool_operation_attempts",
		"durationInterval(ttl)",
	} {
		if !strings.Contains(exact, required) {
			t.Fatalf("exact lease implementation is missing %q", required)
		}
	}
	leaseStart := strings.Index(source, "func (p *Postgres) Lease(")
	if leaseStart < 0 || exactStart <= leaseStart {
		t.Fatal("cannot locate batch Lease SQL implementation")
	}
	batch := source[leaseStart:exactStart]
	if !strings.Contains(batch, "reclaimExpiredReservedForTenantTx(ctx, tx, req.TenantID") {
		t.Fatal("batch Lease must reclaim only current-tenant reserved attempts")
	}
	if strings.Contains(batch, "reclaimExpiredTx(ctx, tx") {
		t.Fatal("batch Lease must not invoke the global/unknown-producing reconciler")
	}
	for _, forbidden := range []string{"req.TTL.String()", "ttl.String()", "retryDelay.String()"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("PostgreSQL interval must not use Go duration text: found %q", forbidden)
		}
	}
}
