package wecom

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"sort"
)

const pkcs7BlockSize = 32

func decodeAESKey(encodingKey string) ([]byte, error) {
	if len(encodingKey) != 43 {
		return nil, errors.New("EncodingAESKey must contain 43 characters")
	}
	key, err := base64.StdEncoding.DecodeString(encodingKey + "=")
	if err != nil {
		return nil, fmt.Errorf("decode EncodingAESKey: %w", err)
	}
	if len(key) != 32 {
		return nil, errors.New("decoded EncodingAESKey must contain 32 bytes")
	}
	return key, nil
}

func messageSignature(token, timestamp, nonce, encrypted string) string {
	parts := []string{token, timestamp, nonce, encrypted}
	sort.Strings(parts)
	hash := sha1.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func verifyMessageSignature(token, timestamp, nonce, encrypted, signature string) bool {
	expected := messageSignature(token, timestamp, nonce, encrypted)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) == 1
}

func encryptMessage(message []byte, receiveID, encodingKey string) (string, error) {
	key, err := decodeAESKey(encodingKey)
	if err != nil {
		return "", err
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate message random prefix: %w", err)
	}
	plaintext := make([]byte, 0, 20+len(message)+len(receiveID)+pkcs7BlockSize)
	plaintext = append(plaintext, random...)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(message)))
	plaintext = append(plaintext, length[:]...)
	plaintext = append(plaintext, message...)
	plaintext = append(plaintext, receiveID...)
	plaintext = pkcs7Pad(plaintext)

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create AES cipher: %w", err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(ciphertext, plaintext)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func decryptMessage(encrypted, expectedReceiveID, encodingKey string) ([]byte, error) {
	key, err := decodeAESKey(encodingKey)
	if err != nil {
		return nil, err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return nil, fmt.Errorf("decode encrypted message: %w", err)
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, errors.New("encrypted message length is invalid")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(plaintext, ciphertext)
	plaintext, err = pkcs7Unpad(plaintext)
	if err != nil {
		return nil, err
	}
	if len(plaintext) < 20 {
		return nil, errors.New("decrypted message is too short")
	}
	messageLength := int(binary.BigEndian.Uint32(plaintext[16:20]))
	if messageLength < 0 || 20+messageLength > len(plaintext) {
		return nil, errors.New("decrypted message length is invalid")
	}
	message := plaintext[20 : 20+messageLength]
	receiveID := plaintext[20+messageLength:]
	if subtle.ConstantTimeCompare(receiveID, []byte(expectedReceiveID)) != 1 {
		return nil, errors.New("decrypted receive ID does not match corp_id")
	}
	return append([]byte(nil), message...), nil
}

func encryptedReplyXML(message []byte, receiveID, token, encodingKey, timestamp, nonce string) ([]byte, error) {
	encrypted, err := encryptMessage(message, receiveID, encodingKey)
	if err != nil {
		return nil, err
	}
	envelope := encryptedEnvelope{
		Encrypt:      encrypted,
		MsgSignature: messageSignature(token, timestamp, nonce, encrypted),
		Timestamp:    timestamp,
		Nonce:        nonce,
	}
	result, err := xml.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal encrypted reply: %w", err)
	}
	return result, nil
}

func pkcs7Pad(value []byte) []byte {
	padding := pkcs7BlockSize - len(value)%pkcs7BlockSize
	return append(value, bytes.Repeat([]byte{byte(padding)}, padding)...)
}

func pkcs7Unpad(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return nil, errors.New("PKCS7 value is empty")
	}
	padding := int(value[len(value)-1])
	if padding <= 0 || padding > pkcs7BlockSize || padding > len(value) {
		return nil, errors.New("PKCS7 padding is invalid")
	}
	for _, item := range value[len(value)-padding:] {
		if int(item) != padding {
			return nil, errors.New("PKCS7 padding is invalid")
		}
	}
	return value[:len(value)-padding], nil
}

type encryptedEnvelope struct {
	XMLName      xml.Name `xml:"xml"`
	Encrypt      string   `xml:"Encrypt"`
	MsgSignature string   `xml:"MsgSignature"`
	Timestamp    string   `xml:"TimeStamp"`
	Nonce        string   `xml:"Nonce"`
}
