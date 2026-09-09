package wecom

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

// MediaClient downloads authenticated WeCom temporary media.
type MediaClient struct {
	Tokens  TokenSource
	BaseURL string
	Client  *http.Client
}

func (client *MediaClient) Download(ctx context.Context, ref gateway.MediaReference) (channels.MediaDownload, error) {
	if client == nil || client.Tokens == nil || ref.Key == "" {
		return channels.MediaDownload{}, errors.New("wecom: media client is not configured")
	}
	token, err := client.Tokens.Token(ctx)
	if err != nil {
		return channels.MediaDownload{}, errors.New("wecom: media authorization failed")
	}
	endpoint, err := url.Parse(strings.TrimRight(firstMediaValue(client.BaseURL, defaultAPIBaseURL), "/") + "/cgi-bin/media/get")
	if err != nil {
		return channels.MediaDownload{}, errors.New("wecom: invalid media endpoint")
	}
	query := endpoint.Query()
	query.Set("access_token", token)
	query.Set("media_id", ref.Key)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return channels.MediaDownload{}, errors.New("wecom: create media request")
	}
	response, err := client.httpClient().Do(request)
	if err != nil {
		return channels.MediaDownload{}, errors.New("wecom: media request failed")
	}
	if response.StatusCode/100 != 2 {
		response.Body.Close()
		return channels.MediaDownload{}, channels.MediaError("wecom", response.StatusCode)
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
		configured = (&CredentialTokenSource{}).client()
	}
	copy := *configured
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copy
}

func firstMediaValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ channels.MediaDownloader = (*MediaClient)(nil)
