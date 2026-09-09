package channel

import (
	"net/http"
	"strings"
	"testing"
)

func TestChannelAdaptersRejectOversizeBodiesWithoutGateway(t *testing.T) {
	body := strings.Repeat("x", int(maxAdapterInboundBodyBytes)+1)
	for _, test := range []struct {
		name  string
		parse func(*http.Request) error
	}{
		{
			name: "telegram",
			parse: func(req *http.Request) error {
				_, err := NewTelegramAdapter().ParseInbound(req, nil)
				return err
			},
		},
		{
			name: "wework",
			parse: func(req *http.Request) error {
				_, err := NewWeWorkAdapter().ParseInbound(req, nil)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			if err := test.parse(req); err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("oversize adapter body was accepted: %v", err)
			}
		})
	}
}

func TestReadAdapterInboundBodyRestoresExactBytes(t *testing.T) {
	const body = "<xml><Encrypt>ciphertext</Encrypt></xml>"
	req, err := http.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	first, err := readAdapterInboundBody(req, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := readAdapterInboundBody(req, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != body || string(second) != body {
		t.Fatalf("body restore changed bytes: first=%q second=%q", first, second)
	}
}
