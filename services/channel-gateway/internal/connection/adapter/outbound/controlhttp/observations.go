package controlhttp

import (
	"context"
	"encoding/json"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	"net/http"
)

type Observation = c.Observation

func (cl *Client) Report(ctx context.Context, observations []Observation) error {
	for _, o := range observations {
		if o.ScopeID != cl.scope || o.SourceEpoch != cl.epoch || o.InstanceID != cl.instance {
			return c.ErrUnauthorized
		}
	}
	body, e := json.Marshal(struct {
		Version      int           `json:"schema_version"`
		Observations []Observation `json:"observations"`
	}{1, observations})
	if e != nil || len(body) > 128<<10 {
		return c.ErrInvalid
	}
	if e = wire.Validate("observations.schema.json", body); e != nil {
		return c.ErrInvalid
	}
	_, e = cl.requestStatus(ctx, http.MethodPost, "/internal/v1/channel-account-observations", body, 1024, http.StatusNoContent)
	return e
}
