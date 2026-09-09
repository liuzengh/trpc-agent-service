package wecom

import (
	"encoding/base64"
	"encoding/xml"
	"testing"
)

func TestMessageSignature(t *testing.T) {
	signature := messageSignature("token", "100", "nonce", "cipher")
	if !verifyMessageSignature("token", "100", "nonce", "cipher", signature) {
		t.Fatal("verifyMessageSignature() rejected valid signature")
	}
	if verifyMessageSignature("token", "100", "nonce", "tampered", signature) {
		t.Fatal("verifyMessageSignature() accepted tampered ciphertext")
	}
}

func TestEncryptDecryptRoundTripAndCorpID(t *testing.T) {
	key := testEncodingAESKey()
	message := []byte(`<xml><Content><![CDATA[hello]]></Content></xml>`)
	encrypted, err := encryptMessage(message, "corp-a", key)
	if err != nil {
		t.Fatalf("encryptMessage() error = %v", err)
	}
	got, err := decryptMessage(encrypted, "corp-a", key)
	if err != nil {
		t.Fatalf("decryptMessage() error = %v", err)
	}
	if string(got) != string(message) {
		t.Fatalf("decryptMessage() = %q, want %q", got, message)
	}
	if _, err := decryptMessage(encrypted, "corp-b", key); err == nil {
		t.Fatal("decryptMessage() accepted wrong corp_id")
	}
}

func TestEncryptedPassiveReply(t *testing.T) {
	key := testEncodingAESKey()
	message := []byte(`<xml><Content><![CDATA[reply]]></Content></xml>`)
	encoded, err := encryptedReplyXML(message, "corp-a", "token", key, "100", "nonce")
	if err != nil {
		t.Fatalf("encryptedReplyXML() error = %v", err)
	}
	var envelope encryptedEnvelope
	if err := xml.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("xml.Unmarshal() error = %v", err)
	}
	if !verifyMessageSignature(
		"token", "100", "nonce", envelope.Encrypt, envelope.MsgSignature,
	) {
		t.Fatal("passive reply signature is invalid")
	}
	got, err := decryptMessage(envelope.Encrypt, "corp-a", key)
	if err != nil {
		t.Fatalf("decrypt passive reply: %v", err)
	}
	if string(got) != string(message) {
		t.Fatalf("passive reply = %q, want %q", got, message)
	}
}

func TestProtocolValidation(t *testing.T) {
	if _, err := decodeAESKey("short"); err == nil {
		t.Fatal("decodeAESKey() accepted invalid length")
	}
	if _, err := decryptMessage("not-base64", "corp-a", testEncodingAESKey()); err == nil {
		t.Fatal("decryptMessage() accepted invalid Base64")
	}
	if _, err := pkcs7Unpad([]byte{0}); err == nil {
		t.Fatal("pkcs7Unpad() accepted invalid padding")
	}
}

func testEncodingAESKey() string {
	return base64.RawStdEncoding.EncodeToString(
		[]byte("0123456789abcdef0123456789abcdef"),
	)
}
