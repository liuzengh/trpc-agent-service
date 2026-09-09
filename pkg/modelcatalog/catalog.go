// Package modelcatalog contains the operator-owned model limits used for hard
// budget admission. Unlike the framework registry, this catalog has no runtime
// mutation interface and therefore cannot be weakened by tenant or plugin code.
package modelcatalog

// Revision identifies the exact limit set embedded in an immutable Agent
// version. Changing any context-window value requires a new revision.
const Revision = "openai-2026-08-25.2"

// Profile is an immutable operator-approved model limit.
type Profile struct {
	Provider        string
	ModelID         string
	ContextWindow   int
	MaxOutputTokens int
	Revision        string
}

// This intentionally narrow allowlist is pinned from the official model pages:
// https://platform.openai.com/docs/models/gpt-4
// https://platform.openai.com/docs/models/gpt-4o-mini
// Adding an ID or changing a limit is an operator-reviewed code change and
// requires a Revision bump.
var openAIProfiles = map[string]Profile{
	"gpt-4":       {ContextWindow: 8_192, MaxOutputTokens: 8_192},
	"gpt-4o-mini": {ContextWindow: 128_000, MaxOutputTokens: 16_384},
}

// Resolve accepts exact model IDs only. Versioned aliases require their own
// reviewed catalog entry; arbitrary suffixes cannot inherit a hard limit.
func Resolve(provider, modelName string) (Profile, bool) {
	// Model IDs are opaque identifiers. Keep matching case- and
	// whitespace-sensitive so unreviewed aliases cannot inherit another
	// entry's limits and catalog revision.
	if provider != "openai" {
		return Profile{}, false
	}
	limits, ok := openAIProfiles[modelName]
	if !ok {
		return Profile{}, false
	}
	return Profile{
		Provider:        "openai",
		ModelID:         modelName,
		ContextWindow:   limits.ContextWindow,
		MaxOutputTokens: limits.MaxOutputTokens,
		Revision:        Revision,
	}, true
}
