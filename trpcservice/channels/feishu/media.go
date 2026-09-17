package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const defaultFeishuAPIBaseURL = "https://open.feishu.cn"

type MediaAccessTokenProvider interface {
	ResolveFeishuMediaAccessToken(context.Context, channel.ReplyDestination, bool) (string, error)
}

type OfficialMediaFetcher struct {
	Tokens  MediaAccessTokenProvider
	Client  HTTPClient
	BaseURL string
}

type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

func (f OfficialMediaFetcher) Fetch(ctx context.Context, request preprocess.MediaFetchRequest) (preprocess.MediaDownload, error) {
	if f.Tokens == nil || request.Channel != "feishu" || request.TenantID == "" || request.ChannelBindingID == "" ||
		request.ExternalAccountID == "" || request.ConfigVersion < 1 || request.Media.ID == "" || request.Media.MessageID == "" ||
		(request.Media.Kind != "image" && request.Media.Kind != "file") {
		return preprocess.MediaDownload{}, runtime.ErrInvalidEnvelope
	}
	destination := channel.ReplyDestination{TenantID: request.TenantID, Channel: "feishu",
		ChannelBindingID: request.ChannelBindingID, ExternalAccountID: request.ExternalAccountID, ConfigVersion: request.ConfigVersion}
	for attempt := 0; attempt < 2; attempt++ {
		token, err := f.Tokens.ResolveFeishuMediaAccessToken(ctx, destination, attempt == 1)
		if err != nil {
			return preprocess.MediaDownload{}, err
		}
		if token == "" {
			return preprocess.MediaDownload{}, runtime.ErrVersionMismatch
		}
		download, invalidToken, err := f.fetch(ctx, token, request)
		if invalidToken && attempt == 0 {
			continue
		}
		return download, err
	}
	return preprocess.MediaDownload{}, runtime.ErrBackendUnavailable
}

func (f OfficialMediaFetcher) fetch(ctx context.Context, token string, request preprocess.MediaFetchRequest) (preprocess.MediaDownload, bool, error) {
	base := strings.TrimRight(f.BaseURL, "/")
	if base == "" {
		base = defaultFeishuAPIBaseURL
	}
	endpoint, err := url.Parse(base)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return preprocess.MediaDownload{}, false, runtime.ErrInvariantViolation
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/open-apis/im/v1/messages/" + url.PathEscape(request.Media.MessageID) +
		"/resources/" + url.PathEscape(request.Media.ID)
	query := endpoint.Query()
	query.Set("type", request.Media.Kind)
	endpoint.RawQuery = query.Encode()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return preprocess.MediaDownload{}, false, runtime.ErrInvalidEnvelope
	}
	httpRequest.Header.Set("Authorization", "Bearer "+token)
	client := f.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		if ctx.Err() != nil {
			return preprocess.MediaDownload{}, false, ctx.Err()
		}
		return preprocess.MediaDownload{}, false, runtime.ErrBackendUnavailable
	}
	if response == nil {
		return preprocess.MediaDownload{}, false, runtime.ErrBackendUnavailable
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return preprocess.MediaDownload{Body: response.Body, ContentType: response.Header.Get("Content-Type"),
			ContentEncoding: response.Header.Get("Content-Encoding"), DeclaredSize: response.ContentLength,
			ResolvedURL: endpoint.Redacted()}, false, nil
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	var provider struct {
		Code int `json:"code"`
	}
	_ = json.NewDecoder(bytes.NewReader(body)).Decode(&provider)
	invalid := response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden ||
		provider.Code == 99991663 || provider.Code == 99991664 || provider.Code == 99991671
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 {
		return preprocess.MediaDownload{}, invalid, channel.RetryableDeliveryError{Err: fmt.Errorf("feishu media status=%d code=%d", response.StatusCode, provider.Code)}
	}
	return preprocess.MediaDownload{}, invalid, channel.PermanentDeliveryError{Err: fmt.Errorf("feishu media status=%d code=%d", response.StatusCode, provider.Code), Class: "provider_rejected"}
}

var _ preprocess.MediaFetcher = OfficialMediaFetcher{}
