package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func stableID(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = fmt.Fprintf(hash, "%d:%s|", len(value), value)
	}
	return prefix + hex.EncodeToString(hash.Sum(nil))[:32]
}

func inboundPayloadHash(request InboundRequest) string {
	return stableID(
		"sha256_",
		request.Scope.TenantID,
		request.Scope.AppID,
		request.Scope.ChannelBindingID,
		request.UserID,
		request.SessionID,
		request.ChatType,
		request.Text,
	)
}
