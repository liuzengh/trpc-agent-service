package channel

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// MemoryActorID returns the platform storage identity for an authenticated
// provider user. Provider IDs are unique only within their identity authority;
// equal text from another channel or account must not imply the same person.
// The account is the stable, non-secret ChannelBinding.AccountID, not a token.
// Conversation is deliberately excluded so one account's user can reuse their
// own Memory across direct and group sessions. Cross-account linking requires
// an explicit, separately verified migration; this function never aliases raw
// historical user IDs.
func MemoryActorID(tenantID, channelType, accountID, externalUserID string) (string, error) {
	for _, value := range []string{tenantID, channelType, accountID, externalUserID} {
		if !validSessionComponent(value) {
			return "", ErrInvalidSessionIdentity
		}
	}
	// Length-delimited fields avoid separator collisions in provider IDs.
	identity := fmt.Sprintf("memory-actor/v1\x00%d:%s%d:%s%d:%s%d:%s",
		len(tenantID), tenantID, len(channelType), channelType,
		len(accountID), accountID, len(externalUserID), externalUserID)
	digest := sha256.Sum256([]byte(identity))
	return "actor_" + hex.EncodeToString(digest[:]), nil
}
