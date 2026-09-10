package sessionstorage

import (
	"errors"
	"testing"
)

func TestValidateSession(t *testing.T) {
	if err := ValidateSession("tenant-a", "session-a"); err != nil {
		t.Fatalf("valid session = %v", err)
	}
	for _, test := range []struct {
		name      string
		tenantID  string
		sessionID string
	}{
		{name: "missing tenant", sessionID: "session-a"},
		{name: "missing session", tenantID: "tenant-a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateSession(test.tenantID, test.sessionID); !errors.Is(err, ErrInvalid) {
				t.Fatalf("ValidateSession(%q, %q) = %v", test.tenantID, test.sessionID, err)
			}
		})
	}
}
