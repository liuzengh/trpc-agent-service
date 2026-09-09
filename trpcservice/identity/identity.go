// Package identity derives stable, opaque identifiers for the demo runtime.
package identity

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func Derive(secret []byte, parts ...string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(mac.Sum(nil))
}

func RunnerUserID(secret []byte, bindingID, externalUserID string) string {
	return "u_" + Derive(secret, bindingID, externalUserID)
}

func SessionID(secret []byte, bindingID, conversationID string) string {
	return "s_" + Derive(secret, bindingID, "direct", conversationID)
}

// GroupRunnerUserID derives the future group subject scope without exposing a
// group HTTP path in Phase 1.
func GroupRunnerUserID(secret []byte, bindingID, conversationID string) string {
	return "g_" + Derive(secret, bindingID, "group", conversationID)
}

func GroupSessionID(secret []byte, bindingID, conversationID string) string {
	return "s_" + Derive(secret, bindingID, "group", conversationID)
}

func TraceID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func RequestID() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func TaskID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return "t_" + hex.EncodeToString(raw[:]), nil
}
