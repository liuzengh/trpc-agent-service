package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
)

func certificate(certPath, keyPath, caPath string) (tls.Certificate, *x509.CertPool, error) {
	info, err := os.Stat(keyPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return tls.Certificate{}, nil, errors.New("Worker TLS private key must be owner-readable only")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, nil, errors.New("Worker TLS certificate invalid")
	}
	raw, err := os.ReadFile(caPath)
	if err != nil {
		return tls.Certificate{}, nil, errors.New("Worker CA unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return tls.Certificate{}, nil, errors.New("Worker CA invalid")
	}
	return cert, pool, nil
}
func (c Config) controlClient() (*http.Client, *http.Transport, error) {
	cert, ca, err := certificate(c.ControlTLS.CertFile, c.ControlTLS.KeyFile, c.ControlTLS.CAFile)
	if err != nil {
		return nil, nil, err
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca, Certificates: []tls.Certificate{cert}}, MaxIdleConns: c.Limits.MaxActiveAttempts + 8, MaxIdleConnsPerHost: c.Limits.MaxActiveAttempts + 4, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: c.Timing.RequestTimeout.Value()}
	return &http.Client{Transport: tr, Timeout: c.Timing.RequestTimeout.Value(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, tr, nil
}
func (c Config) proofTLS() (*tls.Config, error) {
	cert, ca, err := certificate(c.ProofTLS.CertFile, c.ProofTLS.KeyFile, c.ProofTLS.ClientCAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: ca, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}

type natsConfig struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
	CAFile   string `json:"ca_file,omitempty"`
}

func (c Config) connectNATS() (*nats.Conn, error) {
	var conf natsConfig
	if err := readJSONFile(c.NATSFile, true, &conf); err != nil {
		return nil, err
	}
	defer func() { conf.Password = "" }()
	u, err := url.Parse(conf.URL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || conf.User == "" || conf.Password == "" || (u.Scheme != "nats" && u.Scheme != "tls") {
		return nil, errors.New("Worker NATS configuration invalid")
	}
	options := []nats.Option{nats.Name("agent-worker-v1"), nats.UserInfo(conf.User, conf.Password), nats.CustomInboxPrefix("_INBOX.worker"), nats.Timeout(c.Timing.OperationTimeout.Value()), nats.ReconnectWait(time.Second), nats.MaxReconnects(-1)}
	if u.Scheme == "nats" {
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return nil, errors.New("remote Worker NATS requires TLS")
		}
	} else {
		tc := &tls.Config{MinVersion: tls.VersionTLS13}
		if conf.CAFile != "" {
			if !filepath.IsAbs(conf.CAFile) {
				return nil, errors.New("Worker NATS CA path must be absolute")
			}
			raw, err := os.ReadFile(conf.CAFile)
			if err != nil {
				return nil, errors.New("Worker NATS CA unavailable")
			}
			tc.RootCAs = x509.NewCertPool()
			if !tc.RootCAs.AppendCertsFromPEM(raw) {
				return nil, errors.New("Worker NATS CA invalid")
			}
		}
		options = append(options, nats.Secure(tc))
	}
	nc, err := nats.Connect(conf.URL, options...)
	if err != nil {
		return nil, errors.New("Worker NATS unavailable")
	}
	return nc, nil
}
