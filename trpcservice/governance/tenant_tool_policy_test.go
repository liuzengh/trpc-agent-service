package governance

import (
	"context"
	"errors"
	"testing"
)

func TestTenantToolPolicyEnforcesCurrentPlatformToolGrant(t *testing.T) {
	lookupErr := errors.New("grant store unavailable")
	tests := []struct {
		name       string
		tool       string
		granted    bool
		lookupErr  error
		wantDenied bool
		wantLookup bool
	}{
		{name: "granted platform tool", tool: "query_order", granted: true, wantLookup: true},
		{name: "revoked platform tool", tool: "query_order", wantDenied: true, wantLookup: true},
		{name: "grant lookup failure fails closed", tool: "query_order", lookupErr: lookupErr, wantDenied: true, wantLookup: true},
		{name: "framework tool bypasses tenant grant lookup", tool: "knowledge_search"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookupCalls := 0
			policy, err := NewTenantToolPolicy(
				NewStaticToolPolicy([]string{"query_order", "knowledge_search"}, nil),
				[]string{"query_order"},
				func(_ context.Context, tenantID, toolName string) (bool, error) {
					lookupCalls++
					if tenantID != "tenant-a" || toolName != "query_order" {
						t.Fatalf("lookup = %q/%q", tenantID, toolName)
					}
					return test.granted, test.lookupErr
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			execution := validExecutionContext()
			err = policy.Authorize(context.Background(), execution, ToolRequest{Name: test.tool, Arguments: []byte(`{}`)})
			if test.wantDenied != errors.Is(err, ErrToolDenied) {
				t.Fatalf("Authorize() error = %v, denied=%v", err, errors.Is(err, ErrToolDenied))
			}
			if test.lookupErr != nil && !errors.Is(err, lookupErr) {
				t.Fatalf("Authorize() error = %v, want lookup error", err)
			}
			wantCalls := 0
			if test.wantLookup {
				wantCalls = 1
			}
			if lookupCalls != wantCalls {
				t.Fatalf("lookup calls = %d, want %d", lookupCalls, wantCalls)
			}
		})
	}
}

func TestTenantToolPolicyValidatesConstructionAndPreservesBaseDenial(t *testing.T) {
	if _, err := NewTenantToolPolicy(nil, nil, func(context.Context, string, string) (bool, error) { return true, nil }); err == nil {
		t.Fatal("NewTenantToolPolicy(nil base) succeeded")
	}
	if _, err := NewTenantToolPolicy(NewStaticToolPolicy(nil, nil), nil, nil); err == nil {
		t.Fatal("NewTenantToolPolicy(nil lookup) succeeded")
	}
	lookupCalls := 0
	policy, err := NewTenantToolPolicy(
		NewStaticToolPolicy(nil, nil), []string{"query_order"},
		func(context.Context, string, string) (bool, error) { lookupCalls++; return true, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Authorize(context.Background(), validExecutionContext(), ToolRequest{Name: "query_order", Arguments: []byte(`{}`)}); !errors.Is(err, ErrToolDenied) {
		t.Fatalf("base denial = %v, want ErrToolDenied", err)
	}
	if lookupCalls != 0 {
		t.Fatalf("tenant lookup ran after base denial: %d", lookupCalls)
	}
}
