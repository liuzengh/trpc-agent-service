package netpolicy

import (
	"net"
	"testing"
)

func TestValidatePublicAddressesRejectsPrivateDestinations(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "::1"} {
		if err := ValidatePublicAddresses("example", []net.IPAddr{{IP: net.ParseIP(raw)}}); err == nil {
			t.Fatalf("ValidatePublicAddresses(%s) error = nil", raw)
		}
	}
	if err := ValidatePublicAddresses("example", []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}); err != nil {
		t.Fatalf("ValidatePublicAddresses(public) error = %v", err)
	}
}
