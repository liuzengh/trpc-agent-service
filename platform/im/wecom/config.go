package wecom

import (
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

func validID(s string) bool {
	if len(s) == 0 || len(s) > 1024 || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r <= 32 || r == 127 {
			return false
		}
	}
	return true
}
func normalizeConfig(cfg Config) (Config, error) {
	if !validID(cfg.BotID) || len(cfg.Secret) == 0 || len(cfg.Secret) > 4096 || strings.TrimSpace(cfg.Secret) != cfg.Secret || strings.ContainsAny(cfg.Secret, "\r\n\x00") || !utf8.ValidString(cfg.Secret) {
		return cfg, ErrInvalidConfig
	}
	if cfg.URL == "" {
		cfg.URL = DefaultURL
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return cfg, ErrInvalidConfig
	}
	defaults := []struct {
		p     *time.Duration
		value time.Duration
	}{{&cfg.DialTimeout, 10 * time.Second}, {&cfg.AckTimeout, 5 * time.Second}, {&cfg.WriteTimeout, 5 * time.Second}, {&cfg.HeartbeatInterval, 30 * time.Second}, {&cfg.CloseTimeout, 5 * time.Second}, {&cfg.ReconnectBackoff, time.Second}}
	for _, d := range defaults {
		if *d.p == 0 {
			*d.p = d.value
		}
		if *d.p < time.Millisecond || *d.p > time.Hour {
			return cfg, ErrInvalidConfig
		}
	}
	sizes := []struct {
		p          *int
		value, max int
	}{{&cfg.EventBuffer, 64, 65536}, {&cfg.StateBuffer, 16, 1024}, {&cfg.MaxPending, 64, 65536}, {&cfg.MaxRequestIDs, 4096, 1000000}}
	for _, d := range sizes {
		if *d.p == 0 {
			*d.p = d.value
		}
		if *d.p < 1 || *d.p > d.max {
			return cfg, ErrInvalidConfig
		}
	}
	if cfg.ReadLimit == 0 {
		cfg.ReadLimit = 1 << 20
	}
	if cfg.ReadLimit < 256 || cfg.ReadLimit > 16<<20 || cfg.MaxReconnects < 0 || cfg.MaxReconnects > 100 {
		return cfg, ErrInvalidConfig
	}
	return cfg, nil
}
