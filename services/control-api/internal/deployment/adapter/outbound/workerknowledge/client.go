package workerknowledge

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

const MaxWireBytes = executionv1.MaxKnowledgeRequestBytes

type Client struct {
	http *http.Client
	url  string
}

func New(client *http.Client, endpoint string) (*Client, error) {
	u, e := url.Parse(endpoint)
	if e != nil || client == nil || client.Timeout <= 0 || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("invalid knowledge client config")
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	u.Path = executionv1.KnowledgePath
	return &Client{&c, u.String()}, nil
}
func (c *Client) ImportKnowledge(ctx context.Context, in application.KnowledgeRequest) (application.KnowledgeResult, error) {
	raw, err := json.Marshal(in)
	if err != nil || len(raw) > MaxWireBytes {
		return application.KnowledgeResult{}, application.ErrKnowledgeTooLarge
	}
	defer clear(raw)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(raw))
	if err != nil {
		return application.KnowledgeResult{}, application.ErrKnowledgeUnavailable
	}
	r.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(r)
	if err != nil {
		return application.KnowledgeResult{}, application.ErrKnowledgeUnavailable
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 400:
		return application.KnowledgeResult{}, application.ErrKnowledgeInvalid
	case 403:
		return application.KnowledgeResult{}, application.ErrKnowledgeForbidden
	case 413:
		return application.KnowledgeResult{}, application.ErrKnowledgeTooLarge
	case 200:
	default:
		return application.KnowledgeResult{}, application.ErrKnowledgeUnavailable
	}
	raw, err = io.ReadAll(io.LimitReader(res.Body, 65537))
	if err != nil || len(raw) > 65536 {
		return application.KnowledgeResult{}, application.ErrKnowledgeUnavailable
	}
	var out application.KnowledgeResult
	if json.Unmarshal(raw, &out) != nil {
		return application.KnowledgeResult{}, application.ErrKnowledgeUnavailable
	}
	return out, nil
}
