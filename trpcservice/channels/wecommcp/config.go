package wecommcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
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
	MentionStyle    string                  `json:"mention_style,omitempty"`
	Timezone        string                  `json:"timezone"`
	StartAt         string                  `json:"start_at"`
	DedupeMode      string                  `json:"dedupe_mode"`
	MessagePolicy   *channels.MessagePolicy `json:"message_policy,omitempty"`
	SetupMarker     string                  `json:"setup_marker,omitempty"`
	ManagedIdentity bool                    `json:"managed_identity,omitempty"`
	GroupGrants     map[string]GroupGrant   `json:"group_grants,omitempty"`
}

// GroupGrant binds a human's consent to one group. Times also prevent a newly
// granted member/group from authorizing older backlog implicitly.
type GroupGrant struct {
	StartAt string        `json:"start_at"`
	Members []MemberGrant `json:"members"`
}
type MemberGrant struct {
	ID    string `json:"id"`
	Name  string `json:"name,omitempty"`
	Since string `json:"since"`
}

func ParseBinding(binding controlplane.ChannelBinding) (BindingConfig, error) {
	var cfg BindingConfig
	decoder := json.NewDecoder(bytes.NewReader(binding.Config))
	decoder.DisallowUnknownFields()
	if binding.ChannelType != ChannelType || binding.TenantID == "" || binding.ID == "" ||
		(!strings.HasPrefix(binding.SecretRef, "env://") && !strings.HasPrefix(binding.SecretRef, "managed://")) || decoder.Decode(&cfg) != nil || decoder.Decode(new(any)) != io.EOF {
		return cfg, errors.New("invalid WeCom MCP binding")
	}
	if cfg.MentionStyle == "" {
		cfg.MentionStyle = "whitespace"
	}
	if _, err := channels.ParseMessagePolicy(binding.Config); err != nil {
		return cfg, err
	}
	if !validIDs(cfg.AllowedChatIDs, 20) || (!cfg.ManagedIdentity && !validIDs(cfg.AllowedUserIDs, 100)) ||
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
	if cfg.ManagedIdentity {
		// A single-group old connection has an unambiguous existing grant.
		// Multi-group old connections cannot be reconstructed from two flat
		// lists: deny until each group is explicitly enrolled again.
		if cfg.GroupGrants == nil {
			cfg.GroupGrants = map[string]GroupGrant{}
			for _, chat := range cfg.AllowedChatIDs {
				g := GroupGrant{StartAt: cfg.StartAt, Members: []MemberGrant{}}
				if len(cfg.AllowedChatIDs) == 1 && validIDs(cfg.AllowedUserIDs, 100) {
					for _, id := range cfg.AllowedUserIDs {
						g.Members = append(g.Members, MemberGrant{ID: id, Since: cfg.StartAt})
					}
				}
				cfg.GroupGrants[chat] = g
			}
		}
		if len(cfg.GroupGrants) != len(cfg.AllowedChatIDs) {
			return cfg, errors.New("invalid per-group grants")
		}
		for _, chat := range cfg.AllowedChatIDs {
			g, ok := cfg.GroupGrants[chat]
			from, e := time.Parse(time.RFC3339, g.StartAt)
			if !ok || e != nil || from.Before(start) || from.Nanosecond() != 0 || len(g.Members) > 100 {
				return cfg, errors.New("invalid group grant boundary")
			}
			ids := []string{}
			for _, m := range g.Members {
				since, e := time.Parse(time.RFC3339, m.Since)
				if e != nil || since.Before(from) || since.Nanosecond() != 0 || len(m.Name) > 512 {
					return cfg, errors.New("invalid group member grant")
				}
				ids = append(ids, m.ID)
			}
			if len(ids) > 0 && !validIDs(ids, 100) {
				return cfg, errors.New("invalid group members")
			}
		}
		cfg.SyncMembers()
	} else if len(cfg.GroupGrants) > 0 {
		return cfg, errors.New("per-group grants require managed identity")
	}
	return cfg, nil
}

func (c *BindingConfig) SyncMembers() {
	ids := []string{}
	for _, g := range c.GroupGrants {
		for _, m := range g.Members {
			if !slices.Contains(ids, m.ID) {
				ids = append(ids, m.ID)
			}
		}
	}
	slices.Sort(ids)
	c.AllowedUserIDs = ids
}
func (c BindingConfig) AllowsUser(chat, user string) bool {
	if !c.ManagedIdentity {
		return slices.Contains(c.AllowedUserIDs, user)
	}
	if !slices.Contains(c.AllowedChatIDs, chat) {
		return false
	}
	for _, m := range c.GroupGrants[chat].Members {
		if m.ID == user {
			return true
		}
	}
	return false
}
func (c BindingConfig) AllowsMessage(chat, user string, at time.Time) bool {
	if !c.AllowsUser(chat, user) || at.Before(c.StartFor(chat)) {
		return false
	}
	if !c.ManagedIdentity {
		return true
	}
	for _, m := range c.GroupGrants[chat].Members {
		if m.ID == user {
			since, e := time.Parse(time.RFC3339, m.Since)
			return e == nil && !at.Before(since)
		}
	}
	return false
}
func (c BindingConfig) StartFor(chat string) time.Time {
	if c.ManagedIdentity {
		if g, ok := c.GroupGrants[chat]; ok {
			since, e := time.Parse(time.RFC3339, g.StartAt)
			if e == nil {
				return since
			}
		}
	}
	return c.Start()
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
	cfg.SetupMarker = ""
	// Browser-managed subscriptions keep each group's checkpoint across explicit
	// membership additions. Binding-version checks still fence authorization changes.
	if cfg.ManagedIdentity {
		cfg.AllowedChatIDs = nil
		cfg.AllowedUserIDs = nil
		cfg.GroupGrants = nil
	}
	data, _ := json.Marshal(struct {
		App, Account, Ref string
		Config            BindingConfig
	}{binding.AppID, binding.AccountID, binding.SecretRef, cfg})
	return endpointHash(string(data))
}
