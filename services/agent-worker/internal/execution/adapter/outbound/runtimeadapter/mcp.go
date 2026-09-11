package runtimeadapter

import (
	"context"
	"errors"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/mcptoolset"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var mcpResourcePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var mcpNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
var mcpCapabilityPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

func validateToolPlans(p domain.Plan) error {
	seen := map[string]bool{}
	for _, t := range p.Tools {
		ep, err := url.Parse(t.ServerURL)
		if !mcpResourcePattern.MatchString(t.Resource) || seen[t.Resource] || p.MaxToolCalls < 1 || !mcpNamePattern.MatchString(t.ToolName) || !mcpNamePattern.MatchString(t.ToolsetName) || !mcpCapabilityPattern.MatchString(t.Capability) || err != nil || (ep.Scheme != "http" && ep.Scheme != "https") || ep.Hostname() == "" || ep.User != nil || ep.RawQuery != "" || ep.ForceQuery || strings.Contains(t.ServerURL, "#") {
			return application.ErrManifestInvalid
		}
		seen[t.Resource] = true
		switch t.AuthKind {
		case "none":
			if t.Credential != (domain.CredentialUse{}) {
				return application.ErrManifestInvalid
			}
		case "bearer":
			if t.Credential.CredentialID == "" || t.Credential.Purpose != "bearer_token" || t.Credential.AudienceDigest != protocol.CredentialAudienceDigest("mcp_streamable_http", t.ServerURL, t.AuthKind) {
				return application.ErrManifestInvalid
			}
		default:
			return application.ErrManifestInvalid
		}
	}
	return nil
}
func (f *Factory) prepareTools(ctx context.Context, g domain.Grant, p domain.Plan, batch map[domain.CredentialUse]string) ([]*mcptoolset.Service, error) {
	var services []*mcptoolset.Service
	for _, t := range p.Tools {
		// Use the already fixed execution deadline, not a new per-tool Worker policy.
		s, err := mcptoolset.Open(ctx, mcptoolset.Config{ServerURL: t.ServerURL, ToolsetName: t.ToolsetName, ToolName: t.ToolName, AuthKind: t.AuthKind, BearerToken: batch[t.Credential], Timeout: time.Until(*g.Run.ExecutionDeadline)})
		if err != nil {
			for _, opened := range services {
				_ = opened.Close()
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, mcptoolset.ErrNetwork) || errors.Is(err, context.DeadlineExceeded) {
				return nil, application.ErrDependency
			}
			if errors.Is(err, mcptoolset.ErrAuthentication) {
				return nil, application.ErrCredentialDenied
			}
			return nil, application.ErrRuntimeFailed
		}
		services = append(services, s)
	}
	return services, nil
}
