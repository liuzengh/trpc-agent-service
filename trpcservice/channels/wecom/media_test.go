package wecom_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
)

func TestOfficialMediaFetcherRefreshesProviderTokenAndRedactsResolvedURL(t *testing.T) {
	tokens := &wecomMediaTokens{}
	calls := 0
	client := wecomHTTPClientFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		if request.URL.Query().Get("media_id") != "media-id" || request.URL.Query().Get("access_token") != "secret-token" {
			t.Fatalf("query=%v", request.URL.Query())
		}
		if calls == 1 {
			return wecomMediaResponse(http.StatusOK, "application/json", `{"errcode":40014}`), nil
		}
		return wecomMediaResponse(http.StatusOK, "application/pdf", "%PDF-1.4\n"), nil
	})
	download, err := (wecom.OfficialMediaFetcher{Tokens: tokens, Client: client}).Fetch(context.Background(), preprocess.MediaFetchRequest{
		TenantID: "tenant", RequestID: "request", Channel: "wecom", ChannelBindingID: "binding", ExternalAccountID: "corp", ConfigVersion: 1,
		Media: channel.MediaRef{ID: "media-id", MessageID: "message-1", Kind: "file"}})
	if err != nil {
		t.Fatal(err)
	}
	content, _ := io.ReadAll(download.Body)
	download.Body.Close()
	if string(content) != "%PDF-1.4\n" || calls != 2 || len(tokens.force) != 2 || tokens.force[0] || !tokens.force[1] ||
		strings.Contains(download.ResolvedURL, "secret-token") || strings.Contains(download.ResolvedURL, "media-id") {
		t.Fatalf("calls=%d force=%v download=%#v", calls, tokens.force, download)
	}
}

type wecomMediaTokens struct{ force []bool }

func (t *wecomMediaTokens) ResolveWeComAccessToken(_ context.Context, _ channel.ReplyDestination, force bool) (string, int64, error) {
	t.force = append(t.force, force)
	return "secret-token", 218, nil
}

type wecomHTTPClientFunc func(*http.Request) (*http.Response, error)

func (f wecomHTTPClientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

func wecomMediaResponse(status int, contentType, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{contentType}},
		Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}
