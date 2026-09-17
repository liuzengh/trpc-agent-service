package feishu_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
)

func TestOfficialMediaFetcherUsesScopedTokenAndBoundedResourceRoute(t *testing.T) {
	tokens := &feishuMediaTokens{}
	client := &mediaHTTPClientFunc{do: func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer media-token" || request.URL.Query().Get("type") != "image" ||
			request.URL.Path != "/open-apis/im/v1/messages/message-1/resources/image-key" {
			t.Fatalf("request=%s auth=%q", request.URL.Redacted(), request.Header.Get("Authorization"))
		}
		return mediaHTTPResponse(http.StatusOK, "image/png", "\x89PNG\r\n\x1a\n"), nil
	}}
	fetcher := feishu.OfficialMediaFetcher{Tokens: tokens, Client: client}
	download, err := fetcher.Fetch(context.Background(), preprocess.MediaFetchRequest{TenantID: "tenant", RequestID: "request",
		Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app", ConfigVersion: 1,
		Media: channel.MediaRef{ID: "image-key", MessageID: "message-1", Kind: "image"}})
	if err != nil {
		t.Fatal(err)
	}
	content, _ := io.ReadAll(download.Body)
	download.Body.Close()
	if string(content) != "\x89PNG\r\n\x1a\n" || download.ContentType != "image/png" || len(tokens.force) != 1 || tokens.force[0] {
		t.Fatalf("download=%#v force=%v", download, tokens.force)
	}
}

func TestOfficialMediaFetcherRefreshesInvalidTokenOnceWithoutLeakingIt(t *testing.T) {
	tokens := &feishuMediaTokens{}
	calls := 0
	client := &mediaHTTPClientFunc{do: func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return mediaHTTPResponse(http.StatusUnauthorized, "application/json", `{"code":99991663}`), nil
		}
		return mediaHTTPResponse(http.StatusOK, "text/plain", "safe"), nil
	}}
	download, err := (feishu.OfficialMediaFetcher{Tokens: tokens, Client: client}).Fetch(context.Background(), preprocess.MediaFetchRequest{
		TenantID: "tenant", RequestID: "request", Channel: "feishu", ChannelBindingID: "binding", ExternalAccountID: "app", ConfigVersion: 1,
		Media: channel.MediaRef{ID: "file-key", MessageID: "message-1", Kind: "file"}})
	if err != nil {
		t.Fatal(err)
	}
	download.Body.Close()
	if calls != 2 || len(tokens.force) != 2 || tokens.force[0] || !tokens.force[1] || strings.Contains(download.ResolvedURL, "media-token") {
		t.Fatalf("calls=%d force=%v url=%q", calls, tokens.force, download.ResolvedURL)
	}
}

type feishuMediaTokens struct{ force []bool }

func (t *feishuMediaTokens) ResolveFeishuMediaAccessToken(_ context.Context, _ channel.ReplyDestination, force bool) (string, error) {
	t.force = append(t.force, force)
	return "media-token", nil
}

type mediaHTTPClientFunc struct {
	do func(*http.Request) (*http.Response, error)
}

func (c *mediaHTTPClientFunc) Do(request *http.Request) (*http.Response, error) { return c.do(request) }

func mediaHTTPResponse(status int, contentType, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{contentType}},
		Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}
