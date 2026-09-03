package channels

import (
	"crypto/sha256"
	"encoding/base64"
)

// RuntimeIdentity hides raw provider IDs from Session keys while preserving a
// stable mapping inside one channel binding.
func RuntimeIdentity(
	bindingID string,
	externalUserID string,
	externalChatID string,
	externalThreadID string,
	chatType string,
) (string, string) {
	userID := opaqueID("usr_", bindingID, externalUserID)
	principal := externalUserID
	if chatType == "group" {
		principal = externalChatID
	}
	sessionID := opaqueID(
		"ses_",
		bindingID,
		chatType,
		principal,
		externalThreadID,
	)
	return userID, sessionID
}

func opaqueID(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return prefix + base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}
