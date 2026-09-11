package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
)

type channelNATSConfig struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
	CAFile   string `json:"ca_file,omitempty"`
}

func (c channelNATSConfig) options() ([]nats.Option, error) {
	u, err := url.Parse(c.URL)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Hostname() == "" || (u.Scheme != "nats" && u.Scheme != "tls") || c.User == "" || c.Password == "" {
		return nil, errors.New("channel NATS configuration is invalid")
	}
	if u.Scheme == "nats" {
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return nil, errors.New("channel remote NATS requires TLS")
		}
	}
	options := []nats.Option{nats.Name("control-channel-route-v1"), nats.UserInfo(c.User, c.Password), nats.CustomInboxPrefix("_INBOX.control"), nats.Timeout(3 * time.Second), nats.ReconnectWait(time.Second), nats.MaxReconnects(-1)}
	if u.Scheme == "tls" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if c.CAFile != "" {
			if !filepath.IsAbs(c.CAFile) {
				return nil, errors.New("channel NATS CA path must be absolute")
			}
			raw, err := os.ReadFile(c.CAFile)
			if err != nil {
				return nil, errors.New("channel NATS CA is unavailable")
			}
			tc.RootCAs = x509.NewCertPool()
			if !tc.RootCAs.AppendCertsFromPEM(raw) {
				return nil, errors.New("channel NATS CA is invalid")
			}
		}
		options = append(options, nats.Secure(tc))
	}
	return options, nil
}
func (c *ChannelConfig) connectNATS() (*nats.Conn, error) {
	options, err := c.nats.options()
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(c.nats.URL, options...)
	if err != nil {
		return nil, errors.New("channel NATS connection is unavailable")
	}
	return nc, nil
}
