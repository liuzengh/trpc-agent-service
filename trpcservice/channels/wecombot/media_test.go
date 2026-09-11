package wecombot

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDownloadMediaDecryptsWeComAES256CBC(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	plain := []byte("wecom attachment bytes")
	encrypted := encryptMediaFixture(t, plain, key)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Disposition", `attachment; filename="report.pdf"`)
		writer.Header().Set("Content-Type", "application/pdf")
		_, _ = writer.Write(encrypted)
	}))
	defer server.Close()
	client, err := NewClient(Config{
		BotID: "bot", Secret: "secret", HTTPClient: server.Client(), MaxFileBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	file, err := client.downloadMedia(context.Background(), "file", mediaContent{
		URL: server.URL + "/download", AESKey: base64.StdEncoding.EncodeToString(key),
	})
	if err != nil {
		t.Fatalf("downloadMedia() error = %v", err)
	}
	if file.Name != "report.pdf" || file.MimeType != "application/pdf" || string(file.Data) != string(plain) {
		t.Fatalf("file = %#v", file)
	}
}

func encryptMediaFixture(t *testing.T, plain, key []byte) []byte {
	t.Helper()
	padLen := 32 - len(plain)%32
	padded := append(append([]byte(nil), plain...), make([]byte, padLen)...)
	for index := len(padded) - padLen; index < len(padded); index++ {
		padded[index] = byte(padLen)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	// AES-CBC itself still operates in 16-byte blocks; WeCom's PKCS#7
	// padding block size is 32 bytes.
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, padded)
	return encrypted
}
