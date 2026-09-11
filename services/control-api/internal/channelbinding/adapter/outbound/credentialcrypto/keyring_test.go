package credentialcrypto

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func keys() map[string]Key {
	return map[string]Key{"k1": {Encryption: bytes.Repeat([]byte{1}, 32), MAC: bytes.Repeat([]byte{2}, 32)}, "k2": {Encryption: bytes.Repeat([]byte{3}, 32), MAC: bytes.Repeat([]byte{4}, 32)}}
}
func TestAEADBindingRandomNonceAndRewrap(t *testing.T) {
	ctx := context.Background()
	k, err := New("k1", keys())
	if err != nil {
		t.Fatal(err)
	}
	aad := []byte(`["channel-account-v1","tnt_a","cha_a","telegram","telegram.bot_token","ccr_a",1]`)
	plain := []byte("test-only-credential")
	id, a, err := k.Encrypt(ctx, aad, plain)
	if err != nil || id != "k1" {
		t.Fatal(err)
	}
	_, b, _ := k.Encrypt(ctx, aad, plain)
	if bytes.Equal(a, b) {
		t.Fatal("nonce reused")
	}
	out, err := k.Decrypt(ctx, id, aad, a)
	if err != nil || !bytes.Equal(out, plain) {
		t.Fatal("roundtrip", err)
	}
	for _, replacement := range [][2]string{{"tnt_a", "tnt_b"}, {"cha_a", "cha_b"}, {"telegram.bot_token", "telegram.webhook_secret"}, {"ccr_a", "ccr_b"}, {",1]", ",2]"}, {"channel-account-v1", "profile-v1"}} {
		wrong := bytes.Replace(aad, []byte(replacement[0]), []byte(replacement[1]), 1)
		if _, err = k.Decrypt(ctx, id, wrong, a); !errors.Is(err, ErrInvalidMaterial) {
			t.Fatal("AAD swap not rejected")
		}
	}
	tampered := bytes.Clone(a)
	tampered[len(tampered)-1] ^= 1
	if _, err = k.Decrypt(ctx, id, aad, tampered); !errors.Is(err, ErrInvalidMaterial) {
		t.Fatal("tamper accepted")
	}
	rotated, _ := New("k2", keys())
	newID, rewrapped, err := rotated.Rewrap(ctx, id, aad, a)
	if err != nil || newID != "k2" {
		t.Fatal(err)
	}
	out, err = rotated.Decrypt(ctx, newID, aad, rewrapped)
	if err != nil || !bytes.Equal(out, plain) {
		t.Fatal("rewrap altered value")
	}
}
func TestReceiptMACRotationAndContextCancellation(t *testing.T) {
	ctx := context.Background()
	old, _ := New("k1", keys())
	raw := []byte(`{"actor":"usr_a","operation":"replace","tenant":"tnt_a","value":"fixture"}`)
	id, mac, err := old.SignRequest(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := New("k2", keys())
	ok, err := current.VerifyRequest(ctx, id, mac, raw)
	if err != nil || !ok {
		t.Fatal("old receipts must remain verifiable")
	}
	altered := bytes.Replace(raw, []byte("usr_a"), []byte("usr_b"), 1)
	ok, err = current.VerifyRequest(ctx, id, mac, altered)
	if err != nil || ok {
		t.Fatal("actor not MAC bound")
	}
	_, err = current.VerifyRequest(ctx, "missing", mac, raw)
	if !errors.Is(err, ErrKeyUnavailable) {
		t.Fatal("missing old MAC key must fail closed")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err = current.Encrypt(canceled, []byte("aad"), []byte("value")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err = New("bad", map[string]Key{"bad": {bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{1}, 32)}}); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatal("same encryption/MAC key accepted")
	}
}
