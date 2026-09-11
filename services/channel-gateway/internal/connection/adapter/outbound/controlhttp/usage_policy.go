package controlhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

func (cl *Client) UsagePolicy(ctx context.Context, tenant string) (governancev1.Policy, error) {
	if !c.ValidID(tenant) {
		return governancev1.Policy{}, c.ErrInvalid
	}
	raw, e := cl.request(ctx, http.MethodGet, "/internal/v1/tenants/"+url.PathEscape(tenant)+"/usage-policy", nil, 128<<10)
	if e != nil {
		return governancev1.Policy{}, e
	}
	var p governancev1.Policy
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || p.TenantID != tenant || p.Validate() != nil {
		return governancev1.Policy{}, c.ErrIntegrity
	}
	return p, nil
}
