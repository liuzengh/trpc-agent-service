package feishu

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// MediaClient downloads one image or file resource belonging to the verified
// Feishu message. Redirects must be disabled by the injected HTTP client.
type MediaClient struct {
	Tokens  TokenSource
	BaseURL string
	Client  *http.Client
}

func (client *MediaClient) Download(ctx context.Context, ref gateway.MediaReference) (channels.MediaDownload, error) {
	if client == nil || client.Tokens == nil || ref.Key == "" || ref.MessageID == "" {
		return channels.MediaDownload{}, errors.New("feishu: media client is not configured")
	}
	token, err := client.Tokens.Token(ctx)
	if err != nil {
		return channels.MediaDownload{}, errors.New("feishu: media authorization failed")
	}
	base, err := url.Parse(strings.TrimRight(mediaValue(client.BaseURL, defaultAPIBaseURL), "/"))
	if err != nil {
		return channels.MediaDownload{}, errors.New("feishu: invalid media endpoint")
	}
	base.Path += "/open-apis/im/v1/messages/" + url.PathEscape(ref.MessageID) + "/resources/" + url.PathEscape(ref.Key)
	query := base.Query()
	query.Set("type", ref.Kind)
	base.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return channels.MediaDownload{}, errors.New("feishu: create media request")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.httpClient().Do(request)
	if err != nil {
		return channels.MediaDownload{}, errors.New("feishu: media request failed")
	}
	if response.StatusCode/100 != 2 {
		response.Body.Close()
		return channels.MediaDownload{}, channels.MediaError("feishu", response.StatusCode)
	}
	name := ref.Name
	if _, params, parseErr := mime.ParseMediaType(response.Header.Get("Content-Disposition")); parseErr == nil && params["filename"] != "" {
		name = params["filename"]
	}
	size, _ := strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
	return channels.MediaDownload{Body: response.Body, ContentType: response.Header.Get("Content-Type"), Name: name, Size: size}, nil
}

func (client *MediaClient) httpClient() *http.Client {
	var configured *http.Client
	if client.Client != nil {
		configured = client.Client
	} else {
		configured = (&AppTokenSource{}).client()
	}
	copy := *configured
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

func mediaValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ channels.MediaDownloader = (*MediaClient)(nil)
