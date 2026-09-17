package main

import (
	"strings"
	"testing"
)

func TestTestDSNForDatabaseOverridesKeywordDatabase(t *testing.T) {
	dsn, err := testDSNForDatabase("host=/tmp/trpc-pg16 port=55432 user=postgres dbname=postgres sslmode=verify-full application_name=matrix", "trpc_agent_service_test_0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "dbname='trpc_agent_service_test_0123456789abcdef'") {
		t.Fatalf("missing test database in DSN: %s", dsn)
	}
	if !strings.Contains(dsn, "dbname=postgres") {
		t.Fatalf("admin database should remain visible before override for auditability: %s", dsn)
	}
	if !strings.Contains(dsn, "sslmode=verify-full") || !strings.Contains(dsn, "application_name=matrix") {
		t.Fatalf("connection settings were not preserved: %s", dsn)
	}
}

func TestTestDSNForDatabaseOverridesURLDatabase(t *testing.T) {
	dsn, err := testDSNForDatabase("postgres://user:pass@example.com:5432/postgres?sslmode=require", "trpc_agent_service_test_0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if dsn != "postgres://user:pass@example.com:5432/trpc_agent_service_test_0123456789abcdef?sslmode=require" {
		t.Fatalf("test DSN=%s", dsn)
	}
}

func TestQuoteConninfoValue(t *testing.T) {
	if got := quoteConninfoValue(`dir/with'\slash`); got != `'dir/with\'\\slash'` {
		t.Fatalf("quoteConninfoValue()=%s", got)
	}
}

func TestAssertPostgresAdapterCoverageFloor(t *testing.T) {
	cases := []struct {
		name      string
		floor     string
		coverage  string
		expectErr bool
	}{
		{name: "empty floor is report only", floor: "", coverage: "0.0%", expectErr: false},
		{name: "at floor passes", floor: "53.0", coverage: "53.6%", expectErr: false},
		{name: "above floor passes", floor: "50", coverage: "53.6%", expectErr: false},
		{name: "percent suffix accepted", floor: "53.0%", coverage: "53.6%", expectErr: false},
		{name: "below floor fails", floor: "60", coverage: "53.6%", expectErr: true},
		{name: "invalid floor fails closed", floor: "abc", coverage: "53.6%", expectErr: true},
		{name: "invalid coverage fails closed", floor: "50", coverage: "N/A", expectErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TRPC_MIN_POSTGRES_ADAPTER_COVERAGE", tc.floor)
			err := assertPostgresAdapterCoverageFloor(tc.coverage)
			if (err != nil) != tc.expectErr {
				t.Fatalf("coverage %q floor %q: err=%v, expectErr=%v", tc.coverage, tc.floor, err, tc.expectErr)
			}
		})
	}
}

func TestAssertRuntimeSliceCoverageFloorDelegatesGenericAssertion(t *testing.T) {
	t.Setenv("TRPC_MIN_RUNTIME_SLICE_COVERAGE", "40.0")
	if err := assertCoverageFloor("Runtime slice coordination core", "TRPC_MIN_RUNTIME_SLICE_COVERAGE", "41.5%"); err != nil {
		t.Fatalf("above floor: unexpected error %v", err)
	}
	err := assertCoverageFloor("Runtime slice coordination core", "TRPC_MIN_RUNTIME_SLICE_COVERAGE", "39.9%")
	if err == nil {
		t.Fatal("below floor: expected error")
	}
	if !strings.Contains(err.Error(), "Runtime slice coordination core coverage 39.9% is below required 40.0%") {
		t.Fatalf("unexpected error message: %v", err)
	}
	t.Setenv("TRPC_MIN_POSTGRES_ADAPTER_COVERAGE", "60.0")
	if err := assertPostgresAdapterCoverageFloor("0.0%"); err == nil {
		t.Fatal("adapter floor must stay independent of the runtime slice floor env")
	}
}
