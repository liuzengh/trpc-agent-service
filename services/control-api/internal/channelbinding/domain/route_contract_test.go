package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestRouteWireMatchesFrozenGatewaySchema(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "..", "api", "events", "control", "v1", "route-projected.schema.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != "d332b451f5ed16f3a3a46593717e39d292d6bb83eabfb8cf0051c4483f035c4d" {
		t.Fatal("Gateway route contract changed without coordination")
	}
	decoded, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err = c.AddResource("route.schema.json", decoded); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Compile("route.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	a := fixtureAccount(t)
	b, _ := NewBinding("chb_a", "usr_owner", a, target("a"), testTime)
	state := RouteState{TenantID: a.TenantID, AccountID: a.ID}
	for _, enabled := range []bool{false, true, false} {
		a.Enabled = enabled
		b.Enabled = enabled
		next, changed, err := AdvanceRoute(state, a, &b, "evt_a")
		if err != nil || !changed {
			t.Fatal("route", err)
		}
		encoded, _, err := next.Projection.Encode()
		if err != nil {
			t.Fatal(err)
		}
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		if err = schema.Validate(value); err != nil {
			t.Fatal("Control producer does not satisfy Gateway schema", err)
		}
		state = next
		a.MinRouteGeneration = next.Generation
	}
}
