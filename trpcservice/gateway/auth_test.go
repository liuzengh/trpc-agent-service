package gateway

import (
	"errors"
	"testing"
)

func TestAPIAuthProofRejectsInvalidIssuerState(t *testing.T) {
	var nilProof *apiAuthProof
	if err := nilProof.validate(); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("nil proof error = %v, want ErrUnauthenticated", err)
	}

	tests := []struct {
		name     string
		identity APIIdentity
	}{
		{
			name: "invalid tenant",
			identity: APIIdentity{
				TenantID: "not-a-tenant", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "subject",
			},
		},
		{
			name: "invalid app",
			identity: APIIdentity{
				TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "not-an-app", SubjectID: "subject",
			},
		},
		{
			name: "invalid subject",
			identity: APIIdentity{
				TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "bad\nsubject",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proof := &apiAuthProof{identity: test.identity}
			if err := proof.validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("proof validation error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestStaticAPIAuthenticatorRejectsInvalidMappedIdentity(t *testing.T) {
	_, err := NewStaticAPIAuthenticator(map[string]APIIdentity{
		"credential": {
			TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "bad\nsubject",
		},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid mapped identity error = %v, want ErrInvalid", err)
	}
}
