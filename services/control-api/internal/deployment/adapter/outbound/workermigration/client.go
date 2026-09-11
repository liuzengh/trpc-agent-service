package workermigration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
)

type Client struct {
	http *http.Client
	url  string
}

func New(client *http.Client, endpoint string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || client == nil || client.Timeout <= 0 || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("invalid backend migration client config")
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	u.Path = executionv1.BackendMigrationPath
	return &Client{http: &c, url: u.String()}, nil
}

func (c *Client) Execute(ctx context.Context, in executionv1.BackendMigrationRequest) (executionv1.BackendMigrationResponse, error) {
	raw, err := json.Marshal(in)
	in.Source.Password, in.Target.Password = "", ""
	if err != nil || len(raw) > 64<<10 {
		clear(raw)
		return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationInvalid
	}
	defer clear(raw)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(raw))
	if err != nil {
		return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationUnavailable
	}
	r.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(r)
	if err != nil {
		return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationUnavailable
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 64<<10+1))
	if err != nil || len(data) > 64<<10 {
		return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationUnavailable
	}
	if res.StatusCode == http.StatusConflict {
		var failure struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(data, &failure) == nil && failure.Code == "BACKEND_MIGRATION_BUSY" {
			return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationBusy
		}
		return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationFailed
	}
	if res.StatusCode != http.StatusOK {
		return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationUnavailable
	}
	var out executionv1.BackendMigrationResponse
	if json.Unmarshal(data, &out) != nil || out.MemoryScopesCopied < 0 {
		return executionv1.BackendMigrationResponse{}, application.ErrBackendMigrationUnavailable
	}
	return out, nil
}
