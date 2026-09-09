package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const externalMemoryResponseLimit = 4 << 20

// ExternalMemory is the minimal tenant-scoped HTTP Memory Service adapter.
// Its fixed endpoint and bearer credential come only from published config.
type ExternalMemory struct {
	TenantID, AppID string
	Endpoint, Token string
	Client          *http.Client
}

func (service *ExternalMemory) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	var response struct {
		Entries []*memory.Entry `json:"entries"`
	}
	err := service.call(ctx, "/v1/memories/read", map[string]any{"user_id": key.UserID, "limit": limit}, &response, key.AppName)
	return response.Entries, err
}

func (service *ExternalMemory) SearchMemories(ctx context.Context, key memory.UserKey, query string, options ...memory.SearchOption) ([]*memory.Entry, error) {
	var response struct {
		Entries []*memory.Entry `json:"entries"`
	}
	err := service.call(ctx, "/v1/memories/search", map[string]any{"user_id": key.UserID, "search": memory.ResolveSearchOptions(query, options)}, &response, key.AppName)
	return response.Entries, err
}

func (service *ExternalMemory) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, options ...memory.AddOption) error {
	return service.call(ctx, "/v1/memories/add", map[string]any{"user_id": key.UserID, "memory": value, "topics": topics, "metadata": memory.ResolveAddOptions(options)}, nil, key.AppName)
}

func (service *ExternalMemory) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, options ...memory.UpdateOption) error {
	var response struct {
		MemoryID string `json:"memory_id"`
	}
	err := service.call(ctx, "/v1/memories/update", map[string]any{"user_id": key.UserID, "memory_id": key.MemoryID, "memory": value, "topics": topics, "metadata": memory.ResolveUpdateOptions(options)}, &response, key.AppName)
	if err == nil {
		if result := memory.ResolveUpdateResult(options); result != nil {
			result.MemoryID = response.MemoryID
		}
	}
	return err
}

func (service *ExternalMemory) DeleteMemory(ctx context.Context, key memory.Key) error {
	return service.call(ctx, "/v1/memories/delete", map[string]any{"user_id": key.UserID, "memory_id": key.MemoryID}, nil, key.AppName)
}

func (service *ExternalMemory) ClearMemories(ctx context.Context, key memory.UserKey) error {
	return service.call(ctx, "/v1/memories/clear", map[string]any{"user_id": key.UserID}, nil, key.AppName)
}

func (service *ExternalMemory) Tools() []tool.Tool { return nil }

func (service *ExternalMemory) EnqueueAutoMemoryJob(ctx context.Context, current *session.Session) error {
	if current == nil {
		return errors.New("storage: external memory session is required")
	}
	return service.call(ctx, "/v1/memories/auto", map[string]any{"user_id": current.UserID, "session_id": current.ID}, nil, current.AppName)
}

func (service *ExternalMemory) Close() error { return nil }

func (service *ExternalMemory) call(ctx context.Context, path string, body map[string]any, output any, appName string) error {
	if service == nil || ctx == nil || service.TenantID == "" || service.AppID == "" || service.Endpoint == "" || service.Token == "" {
		return errors.New("storage: external memory route is incomplete")
	}
	tenantID, appID, err := tenant.ParseCanonicalAppName(appName)
	if err != nil || tenantID != service.TenantID || appID != service.AppID {
		return errors.New("storage: external memory scope mismatch")
	}
	endpoint, err := url.Parse(service.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" {
		return errors.New("storage: external memory endpoint is invalid")
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + path
	body["tenant_id"], body["app_id"] = service.TenantID, service.AppID
	payload, err := json.Marshal(body)
	if err != nil {
		return errors.New("storage: external memory request is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return errors.New("storage: external memory request failed")
	}
	request.Header.Set("Authorization", "Bearer "+service.Token)
	request.Header.Set("Content-Type", "application/json")
	client := externalMemoryHTTPClient(service.Client)
	response, err := client.Do(request)
	if err != nil {
		return errors.New("storage: external memory request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return errors.New("storage: external memory service rejected request")
	}
	if output == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, externalMemoryResponseLimit))
	if err := decoder.Decode(output); err != nil {
		return errors.New("storage: external memory response is invalid")
	}
	return nil
}

// externalMemoryHTTPClient keeps the configured transport and timeout but
// prevents redirects from forwarding the tenant bearer token elsewhere.
func externalMemoryHTTPClient(configured *http.Client) *http.Client {
	if configured == nil {
		configured = &http.Client{Timeout: 5 * time.Second}
	}
	copy := *configured
	copy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &copy
}

var _ memory.Service = (*ExternalMemory)(nil)
