package feishu

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
)

// decrypt decodes one Feishu encrypted callback body. The platform derives
// the AES-256 key as SHA-256(encrypt_key) and prepends the 16-byte IV to the
// CBC ciphertext before base64 encoding. Errors deliberately omit key and
// payload material.
func decrypt(encryptKey, encoded string) ([]byte, error) {
	if encryptKey == "" || encoded == "" {
		return nil, errors.New("feishu: encrypt key and payload are required")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) <= aes.BlockSize || (len(raw)-aes.BlockSize)%aes.BlockSize != 0 {
		return nil, errors.New("feishu: malformed encrypted payload")
	}
	key := sha256.Sum256([]byte(encryptKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, errors.New("feishu: initialize decrypt cipher")
	}
	plain := make([]byte, len(raw)-aes.BlockSize)
	cipher.NewCBCDecrypter(block, raw[:aes.BlockSize]).CryptBlocks(plain, raw[aes.BlockSize:])
	if len(plain) == 0 {
		return nil, errors.New("feishu: malformed encrypted payload")
	}
	padding := int(plain[len(plain)-1])
	if padding == 0 || padding > aes.BlockSize || padding > len(plain) {
		return nil, errors.New("feishu: invalid payload padding")
	}
	for _, value := range plain[len(plain)-padding:] {
		if int(value) != padding {
			return nil, errors.New("feishu: invalid payload padding")
		}
	}
	return plain[:len(plain)-padding], nil
}
