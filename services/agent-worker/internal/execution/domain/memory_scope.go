package domain

import (
	"encoding/json"
	"strings"
)

// MemoryScopeID derives a logical namespace from the observed social identity
// and the stable Agent ID in a verified Manifest. The caller must first validate
// and authorize the request and Manifest; this function does not grant access.
// Session, deployment revision and physical backend placement are deliberately
// absent. Moving a backend therefore does not imply migrating its data.
func (r Requested) MemoryScopeID(agentID string) (string, error) {
	for _, value := range []string{r.Route.TenantID, r.Route.AccountID, r.Input.SenderID, agentID} {
		if strings.TrimSpace(value) == "" {
			return "", ErrInvalid
		}
	}
	if r.Route.Provider != "telegram" && r.Route.Provider != "wecom" {
		return "", ErrInvalid
	}
	// Array encoding preserves component boundaries even for separator-like IDs.
	b, _ := json.Marshal([]string{"worker-memory-scope/v1", r.Route.TenantID, r.SocialIdentityID(), agentID})
	return StableID("mem", string(b)), nil
}
