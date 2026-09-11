package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"os"

	workerhttp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/workerhttp"
)

// WorkerConfig names the independently authenticated immutable completion query.
// It is not the Control credential resolver or a live Attempt authorization API.
type WorkerConfig struct{ URL, CAFile, CertificateFile, KeyFile string }

func (c WorkerConfig) validate() error {
	u, e := url.Parse(c.URL)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") || c.CAFile == "" || c.CertificateFile == "" || c.KeyFile == "" {
		return errors.New("Worker HTTPS proof origin and mTLS file references are required")
	}
	return nil
}
func (c WorkerConfig) client() (*workerhttp.Client, error) {
	if e := c.validate(); e != nil {
		return nil, e
	}
	ca, e := os.ReadFile(c.CAFile)
	if e != nil {
		return nil, errors.New("read Worker proof CA failed")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid Worker proof CA")
	}
	cert, e := tls.LoadX509KeyPair(c.CertificateFile, c.KeyFile)
	if e != nil {
		return nil, errors.New("read Worker proof client identity failed")
	}
	return workerhttp.New(workerhttp.Options{BaseURL: c.URL, RootCAs: roots, Certificate: cert})
}
