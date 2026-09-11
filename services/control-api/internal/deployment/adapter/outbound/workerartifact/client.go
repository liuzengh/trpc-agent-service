package workerartifact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"io"
	"net/http"
	"net/url"
)

const MaxWireBytes = executionv1.MaxArtifactRequestBytes

type Client struct {
	http *http.Client
	url  string
}

func New(client *http.Client, endpoint string) (*Client, error) {
	u, e := url.Parse(endpoint)
	if e != nil || client == nil || client.Timeout <= 0 || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("invalid artifact client config")
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	u.Path = executionv1.ArtifactPath
	return &Client{&c, u.String()}, nil
}
func (c *Client) ExecuteArtifact(ctx context.Context, input application.ArtifactBackendRequest) (application.ArtifactResult, error) {
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > MaxWireBytes {
		return application.ArtifactResult{}, application.ErrArtifactTooLarge
	}
	defer clear(raw)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(raw))
	if err != nil {
		return application.ArtifactResult{}, application.ErrArtifactUnavailable
	}
	r.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(r)
	if err != nil {
		return application.ArtifactResult{}, application.ErrArtifactUnavailable
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 400:
		return application.ArtifactResult{}, application.ErrArtifactInvalid
	case 403:
		return application.ArtifactResult{}, application.ErrArtifactForbidden
	case 404:
		return application.ArtifactResult{}, application.ErrArtifactNotFound
	case 413:
		return application.ArtifactResult{}, application.ErrArtifactTooLarge
	case 200:
	default:
		return application.ArtifactResult{}, application.ErrArtifactUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, MaxWireBytes+1))
	if err != nil || len(body) > MaxWireBytes {
		return application.ArtifactResult{}, application.ErrArtifactUnavailable
	}
	defer clear(body)
	var out application.ArtifactResult
	if json.Unmarshal(body, &out) != nil {
		return application.ArtifactResult{}, application.ErrArtifactUnavailable
	}
	return out, nil
}
