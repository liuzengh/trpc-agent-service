package workerhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	managementv1 "github.com/liuzengh/trpc-agent-service/api/runtime/management/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runmanagement/application"
)

type Client struct {
	client *http.Client
	base   *url.URL
}

func (c *Client) post(ctx context.Context, path string, source, target any) error {
	body, err := json.Marshal(source)
	if err != nil {
		return application.ErrUnavailable
	}
	u := *c.base
	u.Path += path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return application.ErrUnavailable
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return application.ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return application.ErrNotFound
	}
	if response.StatusCode == http.StatusConflict || response.StatusCode == http.StatusBadRequest {
		return application.ErrConflict
	}
	if response.StatusCode != http.StatusOK {
		return application.ErrUnavailable
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return application.ErrUnavailable
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return application.ErrUnavailable
	}
	return nil
}

func New(client *http.Client, base string) (*Client, error) {
	u, err := url.Parse(base)
	if client == nil || err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, application.ErrUnavailable
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	return &Client{client: client, base: u}, nil
}

func (c *Client) get(ctx context.Context, path string, query url.Values, target any) error {
	u := *c.base
	u.Path += path
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return application.ErrUnavailable
	}
	req.Header.Set("Accept", "application/json")
	response, err := c.client.Do(req)
	if err != nil {
		return application.ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return application.ErrNotFound
	}
	if response.StatusCode != http.StatusOK {
		return application.ErrUnavailable
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(target); err != nil {
		return application.ErrUnavailable
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		return application.ErrUnavailable
	}
	return nil
}

func pagePath(tenant, resource string) string {
	return fmt.Sprintf("/internal/v1/management/tenants/%s/%s", url.PathEscape(tenant), resource)
}

func pageQuery(offset, limit int) url.Values {
	return url.Values{"offset": []string{strconv.Itoa(offset)}, "limit": []string{strconv.Itoa(limit)}}
}

func (c *Client) ListRuns(ctx context.Context, tenant string, offset, limit int) (managementv1.RunPage, error) {
	var page managementv1.RunPage
	err := c.get(ctx, pagePath(tenant, "runs"), pageQuery(offset, limit), &page)
	return page, err
}
func (c *Client) GetRun(ctx context.Context, tenant, runID string) (managementv1.RunDetail, error) {
	var run managementv1.RunDetail
	err := c.get(ctx, "/internal/v1/management/tenants/"+url.PathEscape(tenant)+"/runs/"+url.PathEscape(runID), nil, &run)
	return run, err
}
func (c *Client) ListAudit(ctx context.Context, tenant string, offset, limit int) (managementv1.AuditPage, error) {
	var page managementv1.AuditPage
	err := c.get(ctx, pagePath(tenant, "audit-events"), pageQuery(offset, limit), &page)
	return page, err
}

func (c *Client) Usage(ctx context.Context, tenant string) (governancev1.UsageSummary, error) {
	var summary governancev1.UsageSummary
	err := c.get(ctx, "/internal/v1/management/tenants/"+url.PathEscape(tenant)+"/usage-summary", nil, &summary)
	return summary, err
}

func (c *Client) ListApprovals(ctx context.Context, tenant string, offset, limit int) (approvalv1.Page, error) {
	var page approvalv1.Page
	err := c.get(ctx, "/internal/v1/approvals/tenants/"+url.PathEscape(tenant)+"/operations", pageQuery(offset, limit), &page)
	return page, err
}

func (c *Client) DecideApproval(ctx context.Context, tenant, operation, actor string, request approvalv1.DecisionRequest) (approvalv1.DecisionResponse, error) {
	var response approvalv1.DecisionResponse
	worker := approvalv1.WorkerDecisionRequest{ActorID: actor, Action: request.Action, Reason: request.Reason, ExpectedArgumentsDigest: request.ExpectedArgumentsDigest}
	err := c.post(ctx, "/internal/v1/approvals/tenants/"+url.PathEscape(tenant)+"/operations/"+url.PathEscape(operation)+"/decision", worker, &response)
	return response, err
}
