package egress

import (
	"errors"
	"net"
	"net/http"
	"testing"
)

func TestIsBlockedIPCoversPrivateAndMappedAddresses(t *testing.T) {
	tests := []string{
		"10.0.0.1",
		"172.16.0.1",
		"192.168.0.1",
		"100.64.0.1",
		"127.0.0.1",
		"169.254.169.254",
		"fd00::1",
		"::ffff:10.0.0.1",
	}
	for _, raw := range tests {
		if !IsBlockedIP(net.ParseIP(raw)) {
			t.Errorf("IsBlockedIP(%q) = false, want true", raw)
		}
	}
	if IsBlockedIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public address was blocked")
	}
}

func TestResolveAllowedIPsRejectsPrivateLiteral(t *testing.T) {
	_, err := ResolveAllowedIPs(nil, "192.168.1.10")
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("ResolveAllowedIPs error = %v, want ErrBlockedAddress", err)
	}
}

func TestHTTPTransportRejectsNonHTTPS(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "http://8.8.8.8/", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	_, err = NewHTTPTransport().RoundTrip(request)
	if err == nil {
		t.Fatal("HTTP request was accepted by HTTPS-only transport")
	}
}

func TestHTTPTransportRejectsPrivateDestinationAtDial(t *testing.T) {
	request, err := http.NewRequest(http.MethodGet, "https://127.0.0.1:443/", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	_, err = NewHTTPTransport().RoundTrip(request)
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("private destination error = %v, want ErrBlockedAddress", err)
	}
}
