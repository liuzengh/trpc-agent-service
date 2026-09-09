package channel

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestWeWorkDecryptAndURLVerification(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	encodedKey := strings.TrimSuffix(base64.StdEncoding.EncodeToString(key), "=")
	binding := &tenant.ChannelBinding{
		Token: "verification-token",
		Config: map[string]string{
			"encoding_aes_key": encodedKey,
			"corp_id":          "corp-123",
		},
	}
	message := []byte("challenge-value")
	encrypted := encryptWeWorkForTest(t, key, message, "corp-123")
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "nonce-123"
	signature := signWeWorkForTest(binding.Token, timestamp, nonce, encrypted)
	query := url.Values{
		"echostr":       []string{encrypted},
		"timestamp":     []string{timestamp},
		"nonce":         []string{nonce},
		"msg_signature": []string{signature},
	}
	req := httptest.NewRequest("GET", "/webhook?"+query.Encode(), nil)
	got, err := NewWeWorkAdapter().VerifyURL(req, binding)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(message) {
		t.Fatalf("got %q want %q", got, message)
	}
}

func TestWeWorkDecryptRejectsWrongReceiver(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	binding := &tenant.ChannelBinding{
		Config: map[string]string{
			"encoding_aes_key": strings.TrimSuffix(base64.StdEncoding.EncodeToString(key), "="),
			"corp_id":          "expected-corp",
		},
	}
	encrypted := encryptWeWorkForTest(t, key, []byte("payload"), "attacker-corp")
	if _, err := decryptWeWorkMessage(binding, encrypted); err == nil {
		t.Fatal("expected receiver mismatch")
	}
}

func encryptWeWorkForTest(t *testing.T, key, message []byte, receiver string) string {
	t.Helper()
	plain := bytes.Repeat([]byte{0x42}, 16)
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(message)))
	plain = append(plain, length...)
	plain = append(plain, message...)
	plain = append(plain, []byte(receiver)...)
	padding := weWorkPKCS7BlockSize - len(plain)%weWorkPKCS7BlockSize
	plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(ciphertext, plain)
	return base64.StdEncoding.EncodeToString(ciphertext)
}

func signWeWorkForTest(token, timestamp, nonce, encrypted string) string {
	parts := []string{token, timestamp, nonce, encrypted}
	sort.Strings(parts)
	digest := sha1.Sum([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(digest[:])
}
