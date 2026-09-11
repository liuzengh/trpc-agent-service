package application

import (
	"context"
	"testing"
)

func TestEndpointProfileCreateAndUpdate(t *testing.T) {
	s, _, _, _ := setup(t)
	in := input()
	profile := "test"
	in.Config = &AccountConfigInput{ReceiveMode: "long_polling", EndpointProfile: &profile}
	r, e := s.CreateAccount(context.Background(), owner, "endpoint-create", in)
	if e != nil || r.Account.Config.EndpointProfile != "test" {
		t.Fatal(r, e)
	}
	profile = "official"
	r, e = s.UpdateAccount(context.Background(), owner, r.Account.ID, "endpoint-update", UpdateAccountInput{ExpectedAccountRevision: 1, Config: &AccountConfigInput{ReceiveMode: "long_polling", EndpointProfile: &profile}})
	if e != nil || r.Account.Config.EndpointProfile != "" || r.Account.ConnectionRevision != 2 {
		t.Fatal(r, e)
	}
}
