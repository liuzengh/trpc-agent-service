// Package wecom adapts encrypted WeCom application callbacks and outbound
// application messages to the service's transport-neutral channel contracts.
package wecom

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1" // #nosec G505 -- SHA-1 is mandated by the WeCom callback protocol.
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

const weComPaddingBlockSize = 32

// Crypt verifies and decrypts one WeCom callback binding.
type Crypt struct {
	token     string
	receiveID string
	key       []byte
}

// NewCrypt validates the callback Token, EncodingAESKey, and expected CorpID.
func NewCrypt(token, encodingAESKey, receiveID string) (*Crypt, error) {
	if token == "" || encodingAESKey == "" || receiveID == "" {
		return nil, errors.New("wecom: token, encoding AES key, and receive ID are required")
	}
	key, err := base64.StdEncoding.DecodeString(encodingAESKey + "=")
	if err != nil || len(key) != 32 {
		return nil, errors.New("wecom: encoding AES key must decode to 32 bytes")
	}
	return &Crypt{token: token, receiveID: receiveID, key: key}, nil
}

// VerifySignature performs the protocol-defined constant-time signature check.
func (crypt *Crypt) VerifySignature(signature, timestamp, nonce, encrypted string) error {
	if crypt == nil || signature == "" || timestamp == "" || nonce == "" || encrypted == "" {
		return errors.New("wecom: complete callback signature parameters are required")
	}
	values := []string{crypt.token, timestamp, nonce, encrypted}
	sort.Strings(values)
	sum := sha1.Sum([]byte(values[0] + values[1] + values[2] + values[3])) // #nosec G401 -- protocol requirement.
	want := hex.EncodeToString(sum[:])
	if len(signature) != len(want) || subtle.ConstantTimeCompare([]byte(signature), []byte(want)) != 1 {
		return errors.New("wecom: invalid callback signature")
	}
	return nil
}

// VerifyAndDecrypt verifies a callback signature, decrypts it, and checks that
// the encrypted receive ID belongs to this binding.
func (crypt *Crypt) VerifyAndDecrypt(signature, timestamp, nonce, encrypted string) ([]byte, error) {
	if err := crypt.VerifySignature(signature, timestamp, nonce, encrypted); err != nil {
		return nil, err
	}
	return crypt.Decrypt(encrypted)
}

// Decrypt decodes the WeCom AES-CBC envelope.
func (crypt *Crypt) Decrypt(encrypted string) ([]byte, error) {
	if crypt == nil || len(crypt.key) != 32 {
		return nil, errors.New("wecom: callback crypt is not configured")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil || len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, errors.New("wecom: invalid encrypted callback")
	}
	block, err := aes.NewCipher(crypt.key)
	if err != nil {
		return nil, fmt.Errorf("wecom: initialize AES: %w", err)
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, crypt.key[:aes.BlockSize]).CryptBlocks(plain, ciphertext)
	plain, err = unpad(plain)
	if err != nil || len(plain) < 20 {
		return nil, errors.New("wecom: invalid encrypted callback padding")
	}
	messageLength := int(binary.BigEndian.Uint32(plain[16:20]))
	if messageLength < 0 || 20+messageLength > len(plain) {
		return nil, errors.New("wecom: invalid encrypted callback length")
	}
	message := plain[20 : 20+messageLength]
	receiveID := plain[20+messageLength:]
	if len(receiveID) != len(crypt.receiveID) || subtle.ConstantTimeCompare(receiveID, []byte(crypt.receiveID)) != 1 {
		return nil, errors.New("wecom: callback receive ID does not match binding")
	}
	return append([]byte(nil), message...), nil
}

func unpad(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, errors.New("empty padded value")
	}
	padding := int(value[len(value)-1])
	if padding < 1 || padding > weComPaddingBlockSize || padding > len(value) {
		return nil, errors.New("invalid padding")
	}
	for _, current := range value[len(value)-padding:] {
		if int(current) != padding {
			return nil, errors.New("invalid padding")
		}
	}
	return value[:len(value)-padding], nil
}
