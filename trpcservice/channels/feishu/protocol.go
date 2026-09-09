package feishu

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const maxCallbackSkew = 5 * time.Minute

func verifySignature(timestamp, nonce, encryptKey string, body []byte, signature string) bool {
	if timestamp == "" || nonce == "" || encryptKey == "" || signature == "" {
		return false
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(timestamp + nonce + encryptKey))
	_, _ = hash.Write(body)
	expected := hex.EncodeToString(hash.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) == 1
}

func verifyTimestamp(timestamp string, now time.Time) error {
	unix, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return errors.New("invalid callback timestamp")
	}
	delta := now.Sub(time.Unix(unix, 0))
	if delta < 0 {
		delta = -delta
	}
	if delta > maxCallbackSkew {
		return errors.New("callback timestamp is outside the replay window")
	}
	return nil
}

func decryptCallback(encrypted, encryptKey string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return nil, fmt.Errorf("decode callback ciphertext: %w", err)
	}
	if len(data) < aes.BlockSize || (len(data)-aes.BlockSize)%aes.BlockSize != 0 {
		return nil, errors.New("callback ciphertext has invalid length")
	}
	key := sha256.Sum256([]byte(encryptKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("create callback cipher: %w", err)
	}
	iv := data[:aes.BlockSize]
	plaintext := append([]byte(nil), data[aes.BlockSize:]...)
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plaintext, plaintext)
	if len(plaintext) == 0 {
		return nil, errors.New("callback plaintext is empty")
	}
	padding := int(plaintext[len(plaintext)-1])
	if padding <= 0 || padding > aes.BlockSize || padding > len(plaintext) {
		return nil, errors.New("callback plaintext has invalid padding")
	}
	for _, value := range plaintext[len(plaintext)-padding:] {
		if int(value) != padding {
			return nil, errors.New("callback plaintext has invalid padding")
		}
	}
	return plaintext[:len(plaintext)-padding], nil
}
