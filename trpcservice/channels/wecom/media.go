package wecom

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

type OfficialMediaFetcher struct {
	Tokens  AccessTokenProvider
	Client  HTTPClient
	BaseURL string
}

func (f OfficialMediaFetcher) Fetch(ctx context.Context, request preprocess.MediaFetchRequest) (preprocess.MediaDownload, error) {
	if f.Tokens == nil || request.Channel != "wecom" || request.TenantID == "" || request.ChannelBindingID == "" ||
		request.ExternalAccountID == "" || request.ConfigVersion < 1 || request.Media.ID == "" || (request.Media.Kind != "image" && request.Media.Kind != "file") {
		return preprocess.MediaDownload{}, runtime.ErrInvalidEnvelope
	}
	destination := channel.ReplyDestination{TenantID: request.TenantID, Channel: "wecom",
		ChannelBindingID: request.ChannelBindingID, ExternalAccountID: request.ExternalAccountID, ConfigVersion: request.ConfigVersion}
	for attempt := 0; attempt < 2; attempt++ {
		token, _, err := f.Tokens.ResolveWeComAccessToken(ctx, destination, attempt == 1)
		if err != nil {
			return preprocess.MediaDownload{}, err
		}
		if token == "" {
			return preprocess.MediaDownload{}, runtime.ErrVersionMismatch
		}
		download, invalidToken, err := f.fetch(ctx, token, request.Media.ID)
		if invalidToken && attempt == 0 {
			continue
		}
		return download, err
	}
	return preprocess.MediaDownload{}, runtime.ErrBackendUnavailable
}

func (f OfficialMediaFetcher) fetch(ctx context.Context, token, mediaID string) (preprocess.MediaDownload, bool, error) {
	base := strings.TrimRight(f.BaseURL, "/")
	if base == "" {
		base = defaultAPIBaseURL
	}
	endpoint, err := url.Parse(base + "/cgi-bin/media/get")
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return preprocess.MediaDownload{}, false, runtime.ErrInvariantViolation
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	query.Set("media_id", mediaID)
	endpoint.RawQuery = query.Encode()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return preprocess.MediaDownload{}, false, runtime.ErrInvalidEnvelope
	}
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
	contentType := response.Header.Get("Content-Type")
	if response.StatusCode >= 200 && response.StatusCode < 300 && contentType != "" && !strings.Contains(strings.ToLower(contentType), "json") {
		return preprocess.MediaDownload{Body: response.Body, ContentType: contentType, ContentEncoding: response.Header.Get("Content-Encoding"),
			DeclaredSize: response.ContentLength, ResolvedURL: endpoint.Scheme + "://" + endpoint.Host + endpoint.Path}, false, nil
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	var provider struct {
		ErrorCode int64 `json:"errcode"`
	}
	_ = json.NewDecoder(bytes.NewReader(body)).Decode(&provider)
	invalid := provider.ErrorCode == 40014 || provider.ErrorCode == 42001 || provider.ErrorCode == 42007 || response.StatusCode == http.StatusUnauthorized
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500 || provider.ErrorCode == -1 || provider.ErrorCode == 45009 {
		return preprocess.MediaDownload{}, invalid, channel.RetryableDeliveryError{Err: fmt.Errorf("wecom media status=%d errcode=%d", response.StatusCode, provider.ErrorCode)}
	}
	return preprocess.MediaDownload{}, invalid, channel.PermanentDeliveryError{Err: fmt.Errorf("wecom media status=%d errcode=%d", response.StatusCode, provider.ErrorCode), Class: "provider_rejected"}
}

var _ preprocess.MediaFetcher = OfficialMediaFetcher{}
