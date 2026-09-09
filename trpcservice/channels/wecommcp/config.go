package wecommcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

const ChannelType = "wecom_mcp"

// BindingConfig is an explicit subscription scope, never a discover-all rule.
// Fingerprint deduplication merges identical messages from the same sender in
// the same second. Requiring the mode acknowledges this source limitation.
type BindingConfig struct {
	AllowedChatIDs []string `json:"allowed_chat_ids"`
	AllowedUserIDs []string `json:"allowed_user_ids"`
	MentionPrefix  string   `json:"mention_prefix"`
	// whitespace is strict; prefix supports the observed UI format where the
	// body is directly adjacent to a display name. No entity metadata is given.
	MentionStyle  string                  `json:"mention_style,omitempty"`
	Timezone      string                  `json:"timezone"`
	StartAt       string                  `json:"start_at"`
	DedupeMode    string                  `json:"dedupe_mode"`
	MessagePolicy *channels.MessagePolicy `json:"message_policy,omitempty"`
}

func ParseBinding(binding controlplane.ChannelBinding) (BindingConfig, error) {
	var cfg BindingConfig
	decoder := json.NewDecoder(bytes.NewReader(binding.Config))
	decoder.DisallowUnknownFields()
	if binding.ChannelType != ChannelType || binding.TenantID == "" || binding.ID == "" ||
		!strings.HasPrefix(binding.SecretRef, "env://") || decoder.Decode(&cfg) != nil || decoder.Decode(new(any)) != io.EOF {
		return cfg, errors.New("invalid WeCom MCP binding")
	}
	if cfg.MentionStyle == "" {
		cfg.MentionStyle = "whitespace"
	}
	if _, err := channels.ParseMessagePolicy(binding.Config); err != nil {
		return cfg, err
	}
	if !validIDs(cfg.AllowedChatIDs, 20) || !validIDs(cfg.AllowedUserIDs, 100) ||
		cfg.DedupeMode != "fingerprint-v1" || !strings.HasPrefix(cfg.MentionPrefix, "@") ||
		len(cfg.MentionPrefix) < 2 || len(cfg.MentionPrefix) > 128 || cfg.MentionPrefix != strings.TrimSpace(cfg.MentionPrefix) || strings.IndexFunc(cfg.MentionPrefix, unicode.IsControl) >= 0 || (cfg.MentionStyle != "whitespace" && cfg.MentionStyle != "prefix") {
		return cfg, errors.New("WeCom MCP requires explicit groups, human senders, mention prefix and fingerprint-v1 mode")
	}
	if cfg.Timezone != "Asia/Shanghai" && cfg.Timezone != "UTC" {
		return cfg, errors.New("WeCom MCP timezone must be explicitly Asia/Shanghai or UTC")
	}
	start, err := time.Parse(time.RFC3339, cfg.StartAt)
	if err != nil || start.IsZero() || start.Nanosecond() != 0 {
		return cfg, errors.New("WeCom MCP start_at must be a whole-second RFC3339 timestamp")
	}
	return cfg, nil
}

func validIDs(values []string, limit int) bool {
	if len(values) == 0 || len(values) > limit {
		return false
	}
	seen := map[string]bool{}
	for _, value := range values {
		if value == "" || strings.Contains(value, "*") || len(value) > 512 || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

func (c BindingConfig) Location() *time.Location {
	if c.Timezone == "UTC" {
		return time.UTC
	}
	// Avoid relying on host tzdata; this adapter deliberately supports only
	// contemporary UTC+8/UTC windows, not historical time-zone conversion.
	return time.FixedZone("Asia/Shanghai", 8*60*60)
}

func (c BindingConfig) Start() time.Time {
	stamp, _ := time.Parse(time.RFC3339, c.StartAt)
	return stamp.UTC()
}

func ConfigFingerprint(binding controlplane.ChannelBinding, cfg BindingConfig) string {
	// Delivery freshness is not source identity. Keep legacy fingerprints
	// stable and let binding-version checks fence concurrent policy edits.
	cfg.MessagePolicy = nil
	data, _ := json.Marshal(struct {
		App, Account, Ref string
		Config            BindingConfig
	}{binding.AppID, binding.AccountID, binding.SecretRef, cfg})
	return endpointHash(string(data))
}
