package governance

import (
	"errors"
	"regexp"
	"strings"
	"unicode"
)

// Caller must come from verified ingress, not a model argument or chat text.
type Caller struct {
	UserID   string
	ChatType string
}

var accessToolName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.:-]{0,254}$`)

func validateToolAccess(policy ToolPolicy) error {
	if len(policy.ToolAllowedUsers) > 256 || len(policy.DirectOnlyTools) > 256 {
		return errors.New("tool access policy exceeds limits")
	}
	for name, users := range policy.ToolAllowedUsers {
		if !accessToolName.MatchString(name) || len(users) > 256 {
			return errors.New("invalid tool user allowlist")
		}
		seen := map[string]bool{}
		for _, user := range users {
			if user == "" || len(user) > 512 || strings.TrimSpace(user) != user || strings.IndexFunc(user, unicode.IsControl) >= 0 || seen[user] {
				return errors.New("invalid or duplicate tool user identity")
			}
			seen[user] = true
		}
	}
	seen := map[string]bool{}
	for _, name := range policy.DirectOnlyTools {
		if !accessToolName.MatchString(name) || seen[name] {
			return errors.New("invalid or duplicate direct-only tool")
		}
		seen[name] = true
	}
	return nil
}

// Return a fresh list for this invocation. Cached revision configuration must
// never be mutated by another user's or another channel audience's request.
func scopeToolAccess(policy ToolPolicy, caller Caller) []string {
	result := make([]string, 0, len(policy.AllowedTools))
	direct := stringSet(policy.DirectOnlyTools)
	for _, name := range policy.AllowedTools {
		name = strings.TrimSpace(name)
		if contains(direct, name) && caller.ChatType != "direct" {
			continue
		}
		if users, restricted := policy.ToolAllowedUsers[name]; restricted {
			found := false
			for _, user := range users {
				if caller.UserID != "" && user == caller.UserID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		result = append(result, name)
	}
	return result
}
