package domain

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	MaxTextBytes               = 65536
	MaxWeComTextBytes          = 20480
	MaxTelegramPartRunes       = 4096
	MaxExactInteger      int64 = 9007199254740991
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func opaque(v string, max int) bool {
	if v == "" || len(v) > max || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
func decimal(v string, allowNegative bool) bool {
	n, e := strconv.ParseInt(v, 10, 64)
	return e == nil && strconv.FormatInt(n, 10) == v && (n > 0 || (allowNegative && n < 0))
}
func textValid(v string) bool {
	return v != "" && len(v) <= MaxTextBytes && utf8.ValidString(v) && !strings.ContainsRune(v, 0) && strings.TrimSpace(v) != ""
}

func (t Target) Validate() error {
	if !identifier.MatchString(t.TenantID) || !identifier.MatchString(t.AccountID) || !digestPattern.MatchString(t.ManifestDigest) || !opaque(t.ConversationID, 256) || !opaque(t.SourceEventID, 256) || !validTime(t.ReceivedAt) {
		return ErrInvalid
	}
	switch t.Provider {
	case "telegram":
		if !decimal(t.ConversationID, true) || (t.ThreadID != "" && !decimal(t.ThreadID, false)) || (t.SourceMessageID != "" && !decimal(t.SourceMessageID, false)) || t.CallbackRequestID != "" || t.Origin != nil {
			return ErrInvalid
		}
	case "wecom":
		if t.ThreadID != "" || t.SourceMessageID != "" || !opaque(t.CallbackRequestID, 256) || t.Origin == nil || !identifier.MatchString(t.Origin.InstanceID) || t.Origin.Epoch < 1 || t.Origin.Revision < 1 || t.Origin.SocketGeneration < 1 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

// PlanText fixes the complete send plan before the Final barrier is committed.
// Telegram parts preserve Unicode code points. WeCom's P0 protocol allows only
// one attempted Final per callback, so oversize text is never silently split.
func PlanText(target Target, text string) ([]string, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	if !textValid(text) {
		return nil, ErrInvalid
	}
	if target.Provider == "wecom" {
		if len(text) > MaxWeComTextBytes {
			return nil, ErrUnsupported
		}
		return []string{text}, nil
	}
	runes := []rune(text)
	parts := make([]string, 0, (len(runes)+MaxTelegramPartRunes-1)/MaxTelegramPartRunes)
	for len(runes) > 0 {
		end := min(len(runes), MaxTelegramPartRunes)
		parts = append(parts, string(runes[:end]))
		runes = runes[end:]
	}
	return parts, nil
}
