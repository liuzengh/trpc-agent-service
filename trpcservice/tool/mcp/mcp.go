// Package mcp adapts a reviewed, tenant-scoped MCP endpoint into the service
// Tool Catalog. It deliberately uses the public trpc-agent-go/tool/mcp client
// rather than exposing a dynamic MCP ToolSet directly to an Agent.
package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	upstreammcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
	tmcp "trpc.group/trpc-go/trpc-mcp-go"
)

const (
	maxTimeout     = time.Minute
	maxResultBytes = 256 << 10
)

// Config is a code-reviewed, immutable MCP endpoint definition. It is not a
// tenant-provided request payload: a caller must turn it into one exact Tool
// Catalog registration and bind that registration to a published revision.
//
// Only HTTPS SSE and streamable HTTP are admitted. Stdio can launch arbitrary
// local processes and model-selected URLs would weaken the tenant boundary, so
// neither is part of this adapter.
type Config struct {
	Transport                 string
	ServerURL                 string
	RemoteToolName            string
	ExpectedDeclarationDigest string
	Timeout                   time.Duration
	SecretHeader              string
	SecretPrefix              string

	// clientOptions is only an in-package test seam. Production registrations
	// use the fixed options assembled by newToolSet.
	clientOptions []tmcp.ClientOption
}

// NewRegistration returns an active, exact-version Tool Catalog registration.
// LocalID must match RemoteToolName so a published ToolRef names the actual
// remote operation; no generic mcp_call escape hatch exists.
func NewRegistration(tenantID, localID string, version int64, config Config, secretRef secrets.SecretRef) (servicetool.Registration, error) {
	if !validText(tenantID) || !validToolID(localID) || version < 1 {
		return servicetool.Registration{}, runtime.ErrInvariantViolation
	}
	if err := config.validate(localID, secretRef); err != nil {
		return servicetool.Registration{}, err
	}
	config = config.clone()
	bindingDigest, err := BindingDigest(config, secretRef)
	if err != nil {
		return servicetool.Registration{}, err
	}
	return servicetool.Registration{
		TenantID: tenantID, ID: localID, Version: version, ContentDigest: bindingDigest, Status: servicetool.StatusActive, SecretRef: secretRef,
		Build: func(ctx context.Context, request servicetool.BuildRequest) (agenttool.CallableTool, error) {
			value := callable{config: config, request: request, localID: localID}
			declaration, err := value.discover(ctx)
			if err != nil {
				return nil, err
			}
			value.declaration = declaration
			return value, nil
		},
	}, nil
}

func (c Config) validate(localID string, secretRef secrets.SecretRef) error {
	if c.Transport != "streamable" && c.Transport != "streamable_http" && c.Transport != "sse" {
		return runtime.ErrCapabilityUnsupported
	}
	parsed, err := url.Parse(c.ServerURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		strings.TrimSpace(c.ServerURL) != c.ServerURL {
		return runtime.ErrInvariantViolation
	}
	if ip := net.ParseIP(parsed.Hostname()); ip != nil && forbiddenAddress(ip) {
		return runtime.ErrCapabilityUnsupported
	}
	if !validToolID(c.RemoteToolName) || c.RemoteToolName != localID || !validDigest(c.ExpectedDeclarationDigest) || c.Timeout <= 0 || c.Timeout > maxTimeout {
		return runtime.ErrInvariantViolation
	}
	if secretRef.Ref == "" && secretRef.Version == 0 {
		if c.SecretHeader != "" || c.SecretPrefix != "" {
			return runtime.ErrInvariantViolation
		}
		return nil
	}
	if secretRef.Ref == "" || secretRef.Version < 1 || (c.SecretHeader != "Authorization" && c.SecretHeader != "X-API-Key") ||
		containsControl(c.SecretPrefix) {
		return runtime.ErrInvariantViolation
	}
	if c.SecretHeader == "X-API-Key" && c.SecretPrefix != "" {
		return runtime.ErrInvariantViolation
	}
	return nil
}

// BindingDigest is the revision-pinned identity of a reviewed MCP endpoint.
// Changing endpoint, timeout, authentication binding, remote tool name, or
// expected declaration makes an old published ToolRef fail closed.
func BindingDigest(config Config, secretRef secrets.SecretRef) (string, error) {
	if err := config.validate(config.RemoteToolName, secretRef); err != nil {
		return "", err
	}
	payload := struct {
		Transport, ServerURL, RemoteToolName, ExpectedDeclarationDigest string
		TimeoutNanoseconds                                              int64
		SecretRef                                                       string
		SecretVersion                                                   int64
		SecretHeader, SecretPrefix                                      string
	}{config.Transport, config.ServerURL, config.RemoteToolName, config.ExpectedDeclarationDigest, config.Timeout.Nanoseconds(),
		secretRef.Ref, secretRef.Version, config.SecretHeader, config.SecretPrefix}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// DeclarationDigest returns the stable digest expected in Config. It is for
// control-plane validation tooling and must be calculated from a reviewed MCP
// discovery result, never model-provided content.
func DeclarationDigest(value *agenttool.Declaration) (string, error) {
	if value == nil || !validToolID(value.Name) {
		return "", runtime.ErrInvariantViolation
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func (c Config) clone() Config {
	c.clientOptions = append([]tmcp.ClientOption(nil), c.clientOptions...)
	return c
}

type upstreamToolSet interface {
	Init(context.Context) error
	Tools(context.Context) []agenttool.Tool
	Close() error
}

var newUpstreamToolSet = func(config upstreammcp.ConnectionConfig, options ...upstreammcp.ToolSetOption) upstreamToolSet {
	return upstreammcp.NewMCPToolSet(config, options...)
}

type callable struct {
	config      Config
	request     servicetool.BuildRequest
	localID     string
	declaration *agenttool.Declaration
}

func (c callable) Declaration() *agenttool.Declaration {
	if c.declaration == nil {
		return nil
	}
	copy := *c.declaration
	return &copy
}

func (c callable) Call(ctx context.Context, arguments []byte) (any, error) {
	if ctx == nil {
		return nil, runtime.ErrTenantScope
	}
	// Each call opens a fresh connection (initialize + tools/list + tools/call).
	// This is deliberately the simplest correct shape for the first version: it
	// keeps the adapter stateless and avoids sharing a ToolSet across concurrent
	// calls or leaking a stale connection after a transport failure. Reusing a
	// per-registration ToolSet requires a close/reconnect lifecycle threaded
	// through the bundle manager and is a documented follow-up, not a gap.
	set, remote, err := c.openExact(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = set.Close() }()
	result, err := remote.Call(ctx, arguments)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > maxResultBytes {
		return nil, runtime.ErrCapabilityUnsupported
	}
	return result, nil
}

func (c callable) discover(ctx context.Context) (*agenttool.Declaration, error) {
	set, remote, err := c.openExact(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = set.Close() }()
	declaration := remote.Declaration()
	if declaration == nil || declaration.Name != c.localID {
		return nil, runtime.ErrVersionMismatch
	}
	digest, err := DeclarationDigest(declaration)
	if err != nil || digest != c.config.ExpectedDeclarationDigest {
		return nil, runtime.ErrVersionMismatch
	}
	copy := *declaration
	return &copy, nil
}

func (c callable) openExact(ctx context.Context) (upstreamToolSet, agenttool.CallableTool, error) {
	if ctx == nil {
		return nil, nil, runtime.ErrTenantScope
	}
	set := newUpstreamToolSet(upstreammcp.ConnectionConfig{Transport: c.config.Transport, ServerURL: c.config.ServerURL, Timeout: c.config.Timeout},
		upstreammcp.WithToolFilterFunc(agenttool.NewIncludeToolNamesFilter(c.config.RemoteToolName)),
		upstreammcp.WithMCPOptions(tmcp.WithHTTPReqHandler(safeHTTPHandler{})),
		upstreammcp.WithMCPOptions(c.clientOptions()...))
	if err := set.Init(ctx); err != nil {
		_ = set.Close()
		return nil, nil, fmt.Errorf("initialize MCP tool %q: %w", c.localID, err)
	}
	values := set.Tools(ctx)
	if len(values) != 1 || values[0] == nil || values[0].Declaration() == nil || values[0].Declaration().Name != c.localID {
		_ = set.Close()
		return nil, nil, runtime.ErrVersionMismatch
	}
	remote, ok := values[0].(agenttool.CallableTool)
	if !ok {
		_ = set.Close()
		return nil, nil, runtime.ErrCapabilityUnsupported
	}
	return set, remote, nil
}

func (c callable) clientOptions() []tmcp.ClientOption {
	options := make([]tmcp.ClientOption, 0, 2+len(c.config.clientOptions))
	// Streamable HTTP should not create a long-lived GET SSE side channel.
	options = append(options, tmcp.WithClientGetSSEEnabled(false))
	if c.config.SecretHeader != "" {
		options = append(options, tmcp.WithHTTPBeforeRequest(func(ctx context.Context, request *http.Request) error {
			value, err := c.request.ResolveSecret(ctx)
			if err != nil {
				return err
			}
			defer wipe(value.Bytes)
			request.Header.Set(c.config.SecretHeader, c.config.SecretPrefix+string(value.Bytes))
			return nil
		}))
	}
	return append(options, c.config.clientOptions...)
}

func validText(value string) bool {
	return value != "" && strings.TrimSpace(value) == value && !containsControl(value)
}

func validToolID(value string) bool {
	if !validText(value) || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func containsControl(value string) bool { return strings.IndexFunc(value, unicode.IsControl) >= 0 }

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

var lookupIP = net.DefaultResolver.LookupIPAddr

// safeHTTPHandler validates every connection target after DNS resolution. The
// config-time literal-IP check prevents the obvious bypass; this second gate
// handles a reviewed hostname that later resolves to loopback/private space.
// Redirects are deliberately disabled because a redirect is a new endpoint
// that is not part of the revision-pinned binding.
type safeHTTPHandler struct{}

func (safeHTTPHandler) Handle(ctx context.Context, client *http.Client, request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil || request.URL.Scheme != "https" || request.URL.User != nil || request.URL.Hostname() == "" {
		return nil, runtime.ErrCapabilityUnsupported
	}
	addresses, err := lookupIP(ctx, request.URL.Hostname())
	if err != nil || len(addresses) == 0 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	for _, address := range addresses {
		if forbiddenAddress(address.IP) {
			return nil, runtime.ErrCapabilityUnsupported
		}
	}
	if client == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return copy.Do(request)
}

func forbiddenAddress(value net.IP) bool {
	return value == nil || value.IsPrivate() || value.IsLoopback() || value.IsLinkLocalUnicast() || value.IsUnspecified() ||
		value.IsLinkLocalMulticast() || value.IsMulticast()
}

func wipe(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

var _ agenttool.CallableTool = callable{}
