package toolapproval

import (
	"bytes"
	"testing"
)

func TestTicketArgumentsCanonicalizeAndRejectMutation(t *testing.T) {
	first, args, err := normalizeArguments([]byte(`{ "status":"resolved", "ticket_id":"TEST-42" }`))
	if err != nil || args.TicketID != "TEST-42" || !bytes.Equal(first, []byte(`{"ticket_id":"TEST-42","status":"resolved"}`)) {
		t.Fatal(string(first), args, err)
	}
	for _, raw := range []string{`{"ticket_id":"TEST-42","status":"deleted"}`, `{"ticket_id":"TEST-42","status":"closed","extra":true}`, `{"ticket_id":"../42","status":"closed"}`, `{"ticket_id":"TEST-42","status":"closed"} trailing`} {
		if _, _, err := normalizeArguments([]byte(raw)); err == nil {
			t.Fatal("accepted", raw)
		}
	}
}
func TestOperationIdentityChangesWithApprovedArguments(t *testing.T) {
	p := Proposal{TenantID: "t", RunID: "r", AttemptID: "a", InvocationID: "i", ToolCallID: "c", ToolName: "mcp_ticket"}
	if stableID(p, digest([]byte(`{"ticket_id":"T","status":"open"}`))) == stableID(p, digest([]byte(`{"ticket_id":"T","status":"closed"}`))) {
		t.Fatal("argument mutation reused approval identity")
	}
}
