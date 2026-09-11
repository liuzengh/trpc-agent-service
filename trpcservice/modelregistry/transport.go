package modelregistry

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

type Option func(*Store) error

// WithLoopbackAliases is a deployment-only compatibility setting for moving
// container-created model connections to a host process. It does not modify
// immutable connection URLs, system DNS, /etc/hosts, or unrelated HTTP clients.
// An alias is used only for an explicitly allowed origin, with tenant checks.
func WithLoopbackAliases(raw string) Option {
	return func(s *Store) error {
		if strings.TrimSpace(raw) == "" {
			return nil
		}
		var aliases map[string]string
		if json.Unmarshal([]byte(raw), &aliases) != nil || len(aliases) > 16 {
			return errors.New("invalid model loopback aliases")
		}
		if len(aliases) == 0 {
			return nil
		}
		names := regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,252}$`)
		for name, address := range aliases {
			ip := net.ParseIP(address)
			if !names.MatchString(name) || net.ParseIP(name) != nil || ip == nil || !ip.IsLoopback() {
				return errors.New("model aliases require DNS names and loopback IP addresses")
			}
		}
		base, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return errors.New("model aliases require the standard HTTP transport")
		}
		transport := base.Clone()
		proxy, dial := transport.Proxy, transport.DialContext
		if dial == nil {
			dial = (&net.Dialer{}).DialContext
		}
		transport.Proxy = func(r *http.Request) (*url.URL, error) {
			if aliases[strings.ToLower(r.URL.Hostname())] != "" {
				return nil, nil
			}
			if proxy == nil {
				return nil, nil
			}
			return proxy(r)
		}
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if replacement := aliases[strings.ToLower(host)]; replacement != "" {
				address = net.JoinHostPort(replacement, port)
			}
			return dial(ctx, network, address)
		}
		s.transport = transport
		return nil
	}
}
