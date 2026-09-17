package config_test

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestValidateBackendTopologyRequiresSummaryToShareSessionAuthority(t *testing.T) {
	valid := []config.BackendBinding{
		{Domain: "session", BackendProfileID: "pg", BackendVersion: 2, Required: []string{"atomic_turn_commit"}},
		{Domain: "summary", BackendProfileID: "pg", BackendVersion: 2, Required: []string{"summary_cas"}},
	}
	if err := config.ValidateBackendTopology(valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range [][]config.BackendBinding{
		{{Domain: "summary", BackendProfileID: "pg", BackendVersion: 2, Required: []string{"summary_cas"}}},
		{{Domain: "session", BackendProfileID: "pg-a", BackendVersion: 2, Required: []string{"atomic_turn_commit"}}, {Domain: "summary", BackendProfileID: "pg-b", BackendVersion: 2, Required: []string{"summary_cas"}}},
		{{Domain: "session", BackendProfileID: "pg", BackendVersion: 2, Required: []string{"strong_ryw"}}, {Domain: "summary", BackendProfileID: "pg", BackendVersion: 2, Required: []string{"summary_cas"}}},
	} {
		if err := config.ValidateBackendTopology(invalid); !errors.Is(err, config.ErrInvalid) {
			t.Fatalf("topology error = %v, want invalid", err)
		}
	}
}
