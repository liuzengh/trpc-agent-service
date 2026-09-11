package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"os"
	"strings"

	httpadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/controlhttp"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type ControlConfig struct{ URL, CAFile, CertificateFile, KeyFile, ScopeID, SourceEpoch, PublicOrigin string }

func (c1 ControlConfig) validate(instance string) error {
	if !c.ValidID(instance) || !c.ValidID(c1.ScopeID) || !c.ValidEpoch(c1.SourceEpoch) || c1.CAFile == "" || c1.CertificateFile == "" || c1.KeyFile == "" {
		return errors.New("Control identity and mTLS file references are required")
	}
	for index, raw := range []string{c1.URL, c1.PublicOrigin} {
		if index == 1 && raw == "" {
			continue
		}
		u, e := url.Parse(raw)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
			return errors.New("Control and public origin must be HTTPS origins")
		}
	}
	return nil
}
func (c1 ControlConfig) client(instance string) (*httpadapter.Client, error) {
	options, err := c1.clientOptions(instance)
	if err != nil {
		return nil, err
	}
	return httpadapter.New(options)
}
func (c1 ControlConfig) clientOptions(instance string) (httpadapter.Options, error) {
	ca, e := os.ReadFile(c1.CAFile)
	if e != nil {
		return httpadapter.Options{}, errors.New("read Control CA failed")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return httpadapter.Options{}, errors.New("invalid Control CA")
	}
	cert, e := tls.LoadX509KeyPair(c1.CertificateFile, c1.KeyFile)
	if e != nil {
		return httpadapter.Options{}, errors.New("read Control client identity failed")
	}
	return httpadapter.Options{BaseURL: strings.TrimSuffix(c1.URL, "/"), ScopeID: c1.ScopeID, SourceEpoch: c1.SourceEpoch, InstanceID: instance, RootCAs: pool, Certificate: cert}, nil
}
