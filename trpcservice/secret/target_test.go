package secret

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestExternalIDHasherUsesTypedHMACNamespaces(t *testing.T) {
	provider := newTargetTestSecretProvider()
	hasher, err := NewExternalIDHasher(provider, "v1")
	if err != nil {
		t.Fatalf("new external id hasher: %v", err)
	}
	scope := tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}
	userHash, userVersion, err := hasher.Hash(context.Background(), scope, "binding-a", channels.ExternalIDUser, " user-1 ")
	if err != nil {
		t.Fatalf("hash user id: %v", err)
	}
	userHashAgain, _, err := hasher.Hash(context.Background(), scope, "binding-a", channels.ExternalIDUser, "user-1")
	if err != nil {
		t.Fatalf("hash user id again: %v", err)
	}
	if userHash != userHashAgain || userVersion != "v1" {
		t.Fatalf("user hash/version = %q/%q, want stable v1", userHash, userVersion)
	}
	chatHash, _, err := hasher.Hash(context.Background(), scope, "binding-a", channels.ExternalIDChat, "user-1")
	if err != nil {
		t.Fatalf("hash chat id: %v", err)
	}
	noThreadHash, _, err := hasher.Hash(context.Background(), scope, "binding-a", channels.ExternalIDNoThread, channels.NoThreadExternalID)
	if err != nil {
		t.Fatalf("hash no-thread sentinel: %v", err)
	}
	if userHash == chatHash || chatHash == noThreadHash {
		t.Fatal("typed external ID namespaces collided")
	}
	otherBindingHash, _, err := hasher.Hash(context.Background(), scope, "binding-b", channels.ExternalIDUser, "user-1")
	if err != nil {
		t.Fatalf("hash user id for another binding: %v", err)
	}
	if userHash == otherBindingHash {
		t.Fatal("external user hash crossed binding scope")
	}
	rotated, err := NewExternalIDHasher(provider, "v2")
	if err != nil {
		t.Fatalf("new rotated external id hasher: %v", err)
	}
	rotatedHash, rotatedVersion, err := rotated.Hash(context.Background(), scope, "binding-a", channels.ExternalIDUser, "user-1")
	if err != nil {
		t.Fatalf("hash user id after rotation: %v", err)
	}
	candidateHash, err := rotated.HashWithVersion(context.Background(), scope, "binding-a", channels.ExternalIDUser, "user-1", "v1")
	if err != nil {
		t.Fatalf("hash user id with previous version: %v", err)
	}
	if rotatedVersion != "v2" || rotatedHash == candidateHash || candidateHash != userHash {
		t.Fatalf("rotated hashes/version = %q/%q/%q, want v2 and stable v1 lookup", rotatedHash, candidateHash, rotatedVersion)
	}
}

func TestAEADTargetProtectorRoundTripAndCanonicalPayload(t *testing.T) {
	provider := newTargetTestSecretProvider()
	protector, err := newAEADTargetProtector(provider, "v1", bytes.NewReader(bytes.Repeat([]byte{0x2a}, 12)))
	if err != nil {
		t.Fatalf("new target protector: %v", err)
	}
	target := channels.TargetPlaintext{
		Version:        channels.TargetVersion,
		Channel:        channels.ChannelWeCom,
		TargetKind:     channels.TargetKindUser,
		ExternalUserID: "user-1",
		ProviderTarget: "user-target",
	}
	targetContext := channels.TargetContext{
		Scope:            tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		BindingID:        "binding-a",
		Channel:          channels.ChannelWeCom,
		EntityType:       channels.TargetEntityIdentity,
		InternalEntityID: "user-internal-a",
	}
	envelope, err := protector.Seal(context.Background(), targetContext, channels.TargetPurposeIdentityUser, target)
	if err != nil {
		t.Fatalf("seal target: %v", err)
	}
	if envelope.KeyVersion != "v1" || envelope.Algorithm != channels.TargetAlgorithmAES256GCM {
		t.Fatalf("envelope metadata = %#v", envelope)
	}
	if envelope.NonceB64 != "KioqKioqKioqKioq" || envelope.CiphertextB64 != "5h1x99D_13cIpJmY5LFuHQUmMoY33Wz0bivzX8BEI9MyDUBB9Pm7l0Ds10SMWL7R6-oW4YYq6X-8mZvMfdtA9ihgVaGM_1N2_fH37O5m2rQakEMrBrzSpi-bR5Bjd8_zAhGXOV0__eiB_dpzhjF9nApRB350n_xuEryGoU04SHT3eWpwqArnX6PKkLNXho9Q5FBI3CZXEHBlJTUTqadpi2Ti1KkoxxV0ulrF0tR4" {
		t.Fatalf("fixed target vector = %#v", envelope)
	}
	canonical, err := json.Marshal(target)
	if err != nil {
		t.Fatalf("marshal target: %v", err)
	}
	if string(canonical) != `{"version":1,"channel":"wecom","target_kind":"user","external_user_id":"user-1","external_chat_id":"","external_thread_id":"","provider_target":"user-target"}` {
		t.Fatalf("canonical target = %s", canonical)
	}
	opened, err := protector.Open(context.Background(), targetContext, channels.TargetPurposeIdentityUser, envelope)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	if !reflect.DeepEqual(opened, target) {
		t.Fatalf("opened target = %#v, want %#v", opened, target)
	}
}

func TestAEADTargetProtectorRejectsAADScopeMismatch(t *testing.T) {
	provider := newTargetTestSecretProvider()
	protector, err := newAEADTargetProtector(provider, "v1", bytes.NewReader(bytes.Repeat([]byte{0x2a}, 12)))
	if err != nil {
		t.Fatalf("new target protector: %v", err)
	}
	target := channels.TargetPlaintext{
		Version:        channels.TargetVersion,
		Channel:        channels.ChannelFeishu,
		TargetKind:     channels.TargetKindConversation,
		ExternalChatID: "chat-1",
		ProviderTarget: "chat-target",
	}
	targetContext := channels.TargetContext{
		Scope:            tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		BindingID:        "binding-a",
		Channel:          channels.ChannelFeishu,
		EntityType:       channels.TargetEntityConversation,
		InternalEntityID: "conversation-a",
	}
	envelope, err := protector.Seal(context.Background(), targetContext, channels.TargetPurposeConversationChat, target)
	if err != nil {
		t.Fatalf("seal target: %v", err)
	}
	wrongContext := targetContext
	wrongContext.Scope.TenantID = "tenant-b"
	if _, err := protector.Open(context.Background(), wrongContext, channels.TargetPurposeConversationChat, envelope); err == nil {
		t.Fatal("open with another tenant succeeded")
	}
	wrongContext = targetContext
	wrongContext.BindingID = "binding-b"
	if _, err := protector.Open(context.Background(), wrongContext, channels.TargetPurposeConversationChat, envelope); err == nil {
		t.Fatal("open with another binding succeeded")
	}
}

func TestTargetEnvelopeValidateRejectsInvalidShape(t *testing.T) {
	validNonce := base64.RawURLEncoding.EncodeToString(make([]byte, channels.TargetNonceSize))
	validCiphertext := base64.RawURLEncoding.EncodeToString(make([]byte, channels.TargetAuthenticationTagSize))
	tests := []channels.TargetEnvelope{
		{Algorithm: "AES-GCM", KeyVersion: "v1", NonceB64: validNonce, CiphertextB64: validCiphertext},
		{Algorithm: channels.TargetAlgorithmAES256GCM, KeyVersion: "v1", NonceB64: "AA", CiphertextB64: validCiphertext},
		{Algorithm: channels.TargetAlgorithmAES256GCM, KeyVersion: "v1", NonceB64: validNonce, CiphertextB64: "AQ"},
	}
	for i, envelope := range tests {
		if err := envelope.Validate(); err == nil {
			t.Fatalf("invalid envelope %d passed validation", i)
		}
	}
}

func TestAEADTargetProtectorReadsPreviousKeyVersion(t *testing.T) {
	provider := newTargetTestSecretProvider()
	oldProtector, err := newAEADTargetProtector(provider, "v1", bytes.NewReader(bytes.Repeat([]byte{0x01}, 12)))
	if err != nil {
		t.Fatalf("new old target protector: %v", err)
	}
	oldTarget := channels.TargetPlaintext{
		Version:        channels.TargetVersion,
		Channel:        channels.ChannelFeishu,
		TargetKind:     channels.TargetKindTopic,
		ExternalChatID: "chat-1", ExternalThreadID: "thread-1", ProviderTarget: "thread-target",
	}
	targetContext := channels.TargetContext{
		Scope:     tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		BindingID: "binding-a", Channel: channels.ChannelFeishu,
		EntityType: channels.TargetEntityConversation, InternalEntityID: "conversation-a",
	}
	oldEnvelope, err := oldProtector.Seal(context.Background(), targetContext, channels.TargetPurposeConversationTopic, oldTarget)
	if err != nil {
		t.Fatalf("seal old target: %v", err)
	}
	newProtector, err := newAEADTargetProtector(provider, "v2", bytes.NewReader(bytes.Repeat([]byte{0x02}, 12)))
	if err != nil {
		t.Fatalf("new current target protector: %v", err)
	}
	newEnvelope, err := newProtector.Seal(context.Background(), targetContext, channels.TargetPurposeConversationTopic, oldTarget)
	if err != nil {
		t.Fatalf("seal new target: %v", err)
	}
	if oldEnvelope.KeyVersion != "v1" || newEnvelope.KeyVersion != "v2" {
		t.Fatalf("envelope key versions = %q/%q", oldEnvelope.KeyVersion, newEnvelope.KeyVersion)
	}
	if _, err := newProtector.Open(context.Background(), targetContext, channels.TargetPurposeConversationTopic, oldEnvelope); err != nil {
		t.Fatalf("open old target after rotation: %v", err)
	}
}

func TestAEADTargetProtectorRejectsInvalidKey(t *testing.T) {
	provider := &targetTestSecretProvider{values: map[string]string{
		targetSecretKey(tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}, providerTargetKeyName, "v1"): base64.RawURLEncoding.EncodeToString([]byte("short")),
	}}
	protector, err := newAEADTargetProtector(provider, "v1", bytes.NewReader(bytes.Repeat([]byte{0x2a}, 12)))
	if err != nil {
		t.Fatalf("new target protector: %v", err)
	}
	_, err = protector.Seal(context.Background(), channels.TargetContext{
		Scope: tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}, BindingID: "binding-a",
		Channel: channels.ChannelWeCom, EntityType: channels.TargetEntityIdentity, InternalEntityID: "user-a",
	}, channels.TargetPurposeIdentityUser, channels.TargetPlaintext{
		Version: channels.TargetVersion, Channel: channels.ChannelWeCom, TargetKind: channels.TargetKindUser,
		ExternalUserID: "user-1", ProviderTarget: "user-target",
	})
	if err == nil {
		t.Fatal("seal with invalid key succeeded")
	}
}

type targetTestSecretProvider struct {
	values map[string]string
}

func newTargetTestSecretProvider() *targetTestSecretProvider {
	keyV1 := bytes.NewBuffer(make([]byte, 0, 32))
	for i := 0; i < 32; i++ {
		keyV1.WriteByte(byte(i))
	}
	keyV2 := bytes.Repeat([]byte{0xa5}, 32)
	values := map[string]string{}
	for _, item := range []struct {
		name  string
		value []byte
	}{
		{name: providerTargetKeyName, value: keyV1.Bytes()},
		{name: externalIDKeyName, value: keyV1.Bytes()},
	} {
		for _, version := range []string{"v1", "v2"} {
			value := item.value
			if version == "v2" {
				value = keyV2
			}
			values[targetSecretKey(tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}, item.name, version)] = base64.RawURLEncoding.EncodeToString(value)
		}
	}
	return &targetTestSecretProvider{values: values}
}

func (p *targetTestSecretProvider) ResolveSecret(_ context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	value := p.values[targetSecretKey(scope, ref.Name, ref.Version)]
	if value == "" {
		return "", errors.New("test secret not found")
	}
	return value, nil
}

func targetSecretKey(scope tenant.Scope, name, version string) string {
	return scope.TenantID + "\x00" + scope.AppID + "\x00" + name + "\x00" + version
}
