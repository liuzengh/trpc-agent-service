package gateway

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const eventCursorVersion = 1

type CursorCodec struct {
	mu  sync.RWMutex
	key []byte
}

// Close clears the in-memory cursor signing key during role shutdown.
func (c *CursorCodec) Close() error {
	if c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		clear(c.key)
		c.key = nil
	}
	return nil
}

type eventCursor struct {
	Version   int    `json:"v"`
	TenantID  string `json:"tenant_id"`
	RequestID string `json:"request_id"`
	Sequence  uint64 `json:"sequence"`
}

func NewCursorCodec(key []byte) (*CursorCodec, error) {
	if len(key) < 32 {
		return nil, runtime.ErrInvariantViolation
	}
	return &CursorCodec{key: append([]byte(nil), key...)}, nil
}

func (c *CursorCodec) Encode(key ExecutionKey, sequence uint64) (string, error) {
	if c == nil || key.TenantID == "" || key.RequestID == "" || sequence < 1 {
		return "", runtime.ErrInvariantViolation
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.key) < 32 {
		return "", runtime.ErrInvariantViolation
	}
	payload, err := json.Marshal(eventCursor{Version: eventCursorVersion, TenantID: key.TenantID, RequestID: key.RequestID, Sequence: sequence})
	if err != nil {
		return "", err
	}
	signature := c.sign(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (c *CursorCodec) Decode(value string, expected ExecutionKey) (uint64, error) {
	if c == nil || expected.TenantID == "" || expected.RequestID == "" {
		return 0, runtime.ErrInvariantViolation
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.key) < 32 {
		return 0, runtime.ErrInvariantViolation
	}
	if len(value) == 0 || len(value) > 2048 || strings.Count(value, ".") != 1 {
		return 0, runtime.ErrInvalidEnvelope
	}
	parts := strings.SplitN(value, ".", 2)
	payload, payloadErr := base64.RawURLEncoding.DecodeString(parts[0])
	signature, signatureErr := base64.RawURLEncoding.DecodeString(parts[1])
	expectedSignature := c.sign(payload)
	if payloadErr != nil || signatureErr != nil || len(signature) != len(expectedSignature) ||
		subtle.ConstantTimeCompare(signature, expectedSignature) != 1 {
		return 0, runtime.ErrInvalidEnvelope
	}
	var cursor eventCursor
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&cursor)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil || trailingErr != io.EOF || cursor.Version != eventCursorVersion || cursor.Sequence < 1 {
		return 0, runtime.ErrInvalidEnvelope
	}
	if cursor.TenantID != expected.TenantID || cursor.RequestID != expected.RequestID {
		return 0, runtime.ErrTenantScope
	}
	return cursor.Sequence, nil
}

func (c *CursorCodec) sign(payload []byte) []byte {
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}
