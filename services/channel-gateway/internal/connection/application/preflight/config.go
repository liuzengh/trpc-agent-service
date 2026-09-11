package preflight

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/netip"
	"net/url"
	"strings"

	"github.com/gowebpki/jcs"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

const (
	OriginValid     = "PUBLIC_ORIGIN_STATIC_VALID"
	OriginInvalid   = "PUBLIC_ORIGIN_INVALID"
	OriginNotPublic = "PUBLIC_ORIGIN_NOT_PUBLIC"
)

// NewConfig classifies the origin without DNS or network access. Invalid input
// is discarded rather than retained in errors, snapshots, or their digests.
func NewConfig(scope, epoch, rawOrigin string) (ConfigSnapshot, error) {
	if !accountcatalog.ValidID(scope) || !accountcatalog.ValidEpoch(epoch) {
		return ConfigSnapshot{}, ErrInvalid
	}
	origin, status := normalizeOrigin(rawOrigin)
	cfg := ConfigSnapshot{ScopeID: scope, SourceEpoch: epoch, PublicOrigin: origin, OriginStatus: status}
	var err error
	cfg.Digest, err = configDigest(cfg)
	return cfg, err
}

// ValidateConfig requires an already canonical, internally consistent snapshot.
// It does not turn an arbitrary claimed digest into a trusted configuration.
func ValidateConfig(cfg ConfigSnapshot) error {
	if cfg.Policy == "wecom_long_connection_v1" {
		digest, err := wire.PreflightConfigDigestForPolicy(cfg.Policy, cfg.ScopeID, cfg.SourceEpoch, cfg.PublicOrigin, cfg.OriginStatus)
		if err != nil || digest != cfg.Digest {
			return ErrInvalid
		}
		return nil
	}
	if cfg.Policy != "" {
		return ErrInvalid
	}

	if !accountcatalog.ValidID(cfg.ScopeID) || !accountcatalog.ValidEpoch(cfg.SourceEpoch) {
		return ErrInvalid
	}
	switch cfg.OriginStatus {
	case OriginValid:
		if cfg.PublicOrigin == nil {
			return ErrInvalid
		}
		origin, status := normalizeOrigin(*cfg.PublicOrigin)
		if status != OriginValid || origin == nil || *origin != *cfg.PublicOrigin {
			return ErrInvalid
		}
	case OriginInvalid, OriginNotPublic:
		if cfg.PublicOrigin != nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	digest, err := configDigest(cfg)
	if err != nil || cfg.Digest != digest {
		return ErrInvalid
	}
	return nil
}

func configDigest(cfg ConfigSnapshot) (string, error) {
	body, err := json.Marshal(struct {
		SchemaVersion     int     `json:"schema_version"`
		PolicyVersion     string  `json:"policy_version"`
		ScopeID           string  `json:"scope_id"`
		SourceEpoch       string  `json:"source_epoch"`
		TelegramAPIOrigin string  `json:"telegram_api_origin"`
		PublicOrigin      *string `json:"public_origin"`
		OriginStatus      string  `json:"origin_status"`
	}{1, "telegram-preflight-v1", cfg.ScopeID, cfg.SourceEpoch, "https://api.telegram.org", cfg.PublicOrigin, cfg.OriginStatus})
	if err != nil {
		return "", ErrInvalid
	}
	canonical, err := jcs.Transform(body)
	if err != nil {
		return "", ErrInvalid
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func normalizeOrigin(raw string) (*string, string) {
	if raw == "" || len(raw) > 2048 || strings.ContainsAny(raw, "%\\?#") {
		return nil, OriginInvalid
	}
	for _, c := range raw {
		if c <= ' ' || c >= 127 {
			return nil, OriginInvalid
		}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil || u.Host == "" || u.RawPath != "" || u.ForceQuery || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, OriginInvalid
	}
	host, port := strings.ToLower(u.Hostname()), u.Port()
	if port != "" && port != "443" && port != "80" && port != "88" && port != "8443" {
		return nil, OriginInvalid
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, OriginInvalid
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if !publicLiteral(addr) {
			return nil, OriginNotPublic
		}
		host = addr.String()
		if addr.Is6() {
			host = "[" + host + "]"
		}
	} else {
		if strings.ContainsAny(host, ":[]") || !validHostname(host) {
			return nil, OriginInvalid
		}
		if !strings.Contains(host, ".") || host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".home.arpa") {
			return nil, OriginNotPublic
		}
		// Reject historical numeric address spellings instead of accepting them as
		// DNS names (e.g. 127.1, 0177.0.0.1, or 0x7f.0.0.1).
		last := host[strings.LastIndexByte(host, '.')+1:]
		if strings.Trim(last, "0123456789") == "" {
			return nil, OriginNotPublic
		}
	}
	if port != "" && port != "443" {
		if strings.HasPrefix(host, "[") {
			host = net.JoinHostPort(strings.Trim(host, "[]"), port)
		} else {
			host += ":" + port
		}
	}
	origin := "https://" + host
	return &origin, OriginValid
}

func validHostname(host string) bool {
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

// The deny set is intentionally conservative for literal addresses. Domain
// names are not resolved, so STATIC_VALID never claims public reachability.
var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

func publicLiteral(addr netip.Addr) bool {
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.Is4In6() {
		return false
	}
	if addr.Is6() && !netip.MustParsePrefix("2000::/3").Contains(addr) {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

// NewWeComConfig has no inbound/public-origin dependency or caller-controlled endpoint.
func NewWeComConfig(scope, epoch string) (ConfigSnapshot, error) {
	cfg := ConfigSnapshot{Policy: "wecom_long_connection_v1", ScopeID: scope, SourceEpoch: epoch, OriginStatus: "PUBLIC_ORIGIN_NOT_APPLICABLE"}
	var err error
	cfg.Digest, err = wire.PreflightConfigDigestForPolicy(cfg.Policy, scope, epoch, nil, cfg.OriginStatus)
	if err != nil {
		return ConfigSnapshot{}, ErrInvalid
	}
	return cfg, nil
}
