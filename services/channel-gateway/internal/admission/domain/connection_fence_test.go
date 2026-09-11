package domain

import (
	"errors"
	"testing"
)

func TestWeComRequiresConnectionFence(t *testing.T) {
	in := valid().Input
	in.Key.Provider = "wecom"
	if !errors.Is(in.Validate(), ErrInvalidInput) {
		t.Fatal("WeCom without a trusted connection fence was accepted")
	}
}

func TestConnectionFenceProviderAndBoundary(t *testing.T) {
	good := ConnectionFence{InstanceID: "gateway-1", Epoch: 7, Revision: 2}
	for _, provider := range []string{"telegram", "wecom"} {
		in := valid().Input
		in.Key.Provider = provider
		in.ConnectionFence = &good
		err := in.Validate()
		if provider == "wecom" && err != nil {
			t.Fatal(err)
		}
		if provider == "telegram" && !errors.Is(err, ErrInvalidInput) {
			t.Fatal("Telegram accepted connection fence")
		}
	}
	for _, fence := range []ConnectionFence{{}, {InstanceID: "bad id", Epoch: 1, Revision: 1}, {InstanceID: "gateway-1", Epoch: 0, Revision: 1}, {InstanceID: "gateway-1", Epoch: 1, Revision: 0}, {InstanceID: "gateway-1", Epoch: -1, Revision: 1}} {
		if !errors.Is(fence.Validate(), ErrInvalidInput) {
			t.Fatalf("invalid fence accepted: %+v", fence)
		}
	}
}
