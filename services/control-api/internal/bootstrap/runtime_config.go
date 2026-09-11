package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/gin-gonic/gin"
	artifactclient "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/workerartifact"
	knowledgeclient "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/workerknowledge"
	migrationclient "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/adapter/outbound/workermigration"
	managementhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runmanagement/adapter/outbound/workerhttp"
	profilehttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/inbound/runtimehttp"
	executionhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/executionhttp"
	"github.com/nats-io/nats.go"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// RuntimeConfig is release-owned configuration. Nil leaves Worker runtime
// surfaces unregistered. URI SAN mappings never trust request identity headers.
type RuntimeConfig struct {
	InternalAddress   string                  `json:"internal_address"`
	TLSCertFile       string                  `json:"tls_cert_file"`
	TLSKeyFile        string                  `json:"tls_key_file"`
	ClientCAFile      string                  `json:"client_ca_file"`
	ExecutionURL      string                  `json:"execution_url"`
	ExecutionCAFile   string                  `json:"execution_ca_file"`
	ExecutionCertFile string                  `json:"execution_cert_file"`
	ExecutionKeyFile  string                  `json:"execution_key_file"`
	ManifestNATSFile  string                  `json:"manifest_nats_file"`
	Workers           []RuntimeWorkerIdentity `json:"workers"`
	nats              channelNATSConfig
}
type RuntimeWorkerIdentity struct {
	PrincipalURI string `json:"principal_uri"`
	WorkerID     string `json:"worker_id"`
}

func loadRuntimeConfig(path string) (*RuntimeConfig, error) {
	if path == "" {
		return nil, nil
	}
	var c RuntimeConfig
	if err := readConfigFile(path, false, &c); err != nil {
		return nil, err
	}
	if c.InternalAddress == "" || len(c.Workers) == 0 || len(c.Workers) > 64 {
		return nil, errors.New("runtime listener and Worker identities are required")
	}
	principals := map[string]bool{}
	workers := map[string]bool{}
	for _, w := range c.Workers {
		u, err := url.Parse(w.PrincipalURI)
		if err != nil || u.Scheme != "spiffe" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || w.WorkerID == "" || len(w.WorkerID) > 256 || principals[w.PrincipalURI] || workers[w.WorkerID] {
			return nil, errors.New("runtime Worker identity mapping is invalid")
		}
		principals[w.PrincipalURI] = true
		workers[w.WorkerID] = true
	}
	if _, err := c.serverTLS(); err != nil {
		return nil, err
	}
	if _, err := c.executionVerifier(); err != nil {
		return nil, err
	}
	if err := readConfigFile(c.ManifestNATSFile, true, &c.nats); err != nil {
		return nil, err
	}
	if _, err := c.nats.options(); err != nil {
		return nil, err
	}
	return &c, nil
}
func runtimeTLS(certPath, keyPath, caPath string) (*tls.Config, error) {
	for _, p := range []string{certPath, keyPath, caPath} {
		if !filepath.IsAbs(p) {
			return nil, errors.New("runtime TLS paths must be absolute")
		}
	}
	info, err := os.Stat(keyPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("runtime private key must be owner-readable only")
	}
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, errors.New("runtime certificate is invalid")
	}
	raw, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.New("runtime CA is unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("runtime CA is invalid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: pool, ClientCAs: pool}, nil
}
func (c *RuntimeConfig) serverTLS() (*tls.Config, error) {
	tc, err := runtimeTLS(c.TLSCertFile, c.TLSKeyFile, c.ClientCAFile)
	if err != nil {
		return nil, err
	}
	tc.ClientAuth = tls.RequireAndVerifyClientCert
	return tc, nil
}
func (c *RuntimeConfig) executionVerifier() (*executionhttp.Verifier, error) {
	tc, err := runtimeTLS(c.ExecutionCertFile, c.ExecutionKeyFile, c.ExecutionCAFile)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tc, MaxIdleConns: 16, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second}, Timeout: 5 * time.Second}
	return executionhttp.New(client, c.ExecutionURL)
}
func (c *RuntimeConfig) managementClient() (*managementhttp.Client, error) {
	tc, err := runtimeTLS(c.ExecutionCertFile, c.ExecutionKeyFile, c.ExecutionCAFile)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tc, MaxIdleConns: 16, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second}, Timeout: 5 * time.Second}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return managementhttp.New(client, c.ExecutionURL)
}
func (c *RuntimeConfig) connectNATS() (*nats.Conn, error) {
	opts, err := c.nats.options()
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(c.nats.URL, opts...)
	if err != nil {
		return nil, errors.New("manifest NATS connection unavailable")
	}
	return nc, nil
}
func (c *RuntimeConfig) authenticateWorker() gin.HandlerFunc {
	return func(g *gin.Context) {
		g.Header("Cache-Control", "no-store")
		r := g.Request
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) < 1 {
			g.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		cert := r.TLS.PeerCertificates[0]
		if len(cert.URIs) != 1 {
			g.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		principal := cert.URIs[0].String()
		for _, w := range c.Workers {
			if w.PrincipalURI == principal {
				g.Request = r.WithContext(profilehttp.WithWorkerIdentity(r.Context(), w.WorkerID))
				g.Next()
				return
			}
		}
		g.AbortWithStatus(http.StatusForbidden)
	}
}

func (c *RuntimeConfig) artifactClient() (*artifactclient.Client, error) {
	tc, err := runtimeTLS(c.ExecutionCertFile, c.ExecutionKeyFile, c.ExecutionCAFile)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tc, MaxIdleConns: 8, IdleConnTimeout: 30 * time.Second}, Timeout: 30 * time.Second}
	return artifactclient.New(client, c.ExecutionURL)
}

func (c *RuntimeConfig) knowledgeClient() (*knowledgeclient.Client, error) {
	tc, err := runtimeTLS(c.ExecutionCertFile, c.ExecutionKeyFile, c.ExecutionCAFile)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tc, MaxIdleConns: 8, IdleConnTimeout: 30 * time.Second}, Timeout: 60 * time.Second}
	return knowledgeclient.New(client, c.ExecutionURL)
}

func (c *RuntimeConfig) backendMigrationClient() (*migrationclient.Client, error) {
	tc, err := runtimeTLS(c.ExecutionCertFile, c.ExecutionKeyFile, c.ExecutionCAFile)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tc, MaxIdleConns: 2, IdleConnTimeout: 30 * time.Second}, Timeout: 65 * time.Second}
	return migrationclient.New(client, c.ExecutionURL)
}
