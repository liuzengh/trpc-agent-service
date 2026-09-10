package config

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

func TestDecodeRejectsUnknownFieldsAndTrailingData(t *testing.T) {
	for _, data := range []string{`{"schema_version":1,"default_agent_app_id":"app","policy_version":1,"unknown":true}`, `{"schema_version":1,"default_agent_app_id":"app","policy_version":1}{}`} {
		if _, err := DecodeV1([]byte(data)); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}

func TestProviderBindingsRequireIndependentSendSecret(t *testing.T) {
	for _, channel := range []string{"feishu", "wecom"} {
		binding := ChannelBinding{Channel: channel}
		if ValidSendSecret(binding) {
			t.Fatalf("channel=%s accepted missing send secret", channel)
		}
		binding.SendSecretRef = secrets.SecretRef{Ref: "send", Version: 1}
		if !ValidSendSecret(binding) {
			t.Fatalf("channel=%s rejected complete send secret", channel)
		}
	}
	if !ValidSendSecret(ChannelBinding{Channel: "fake"}) {
		t.Fatal("fake binding unexpectedly requires provider send credentials")
	}
}
func TestDigestNormalizesBindingOrder(t *testing.T) {
	first := ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: 1, ChannelBindings: []ChannelBinding{{BindingID: "b"}, {BindingID: "a"}}}
	second := first
	second.ChannelBindings = append([]ChannelBinding(nil), first.ChannelBindings...)
	second.ChannelBindings[0], second.ChannelBindings[1] = second.ChannelBindings[1], second.ChannelBindings[0]
	a, _, err := ContentDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := ContentDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("digest mismatch %s %s", a, b)
	}
}
