package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// The open platform, as this adapter uses it: four calls, one origin, no SDK HTTP
// client.
const (
	officialBaseURL = "https://open.feishu.cn"

	tokenPath       = "/open-apis/auth/v3/tenant_access_token/internal"
	botInfoPath     = "/open-apis/bot/v3/info"
	tenantQueryPath = "/open-apis/tenant/v2/tenant/query"
	replyPathFormat = "/open-apis/im/v1/messages/%s/reply"

	messageTypeText = "text"

	requestTimeout = 10 * time.Second

	maxResponseBytes = 64 * 1024
)

type openAPI struct {
	baseURL string
	client  *http.Client
}

func newOpenAPI(baseURL string) *openAPI {
	return &openAPI{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: requestTimeout,
			// Following a redirect would post an app secret or a reply to an
			// origin this package never checked.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (o *openAPI) call(
	ctx context.Context,
	method, path, token string,
	payload any,
) (status int, body []byte, err error) {
	var encoded io.Reader
	if payload != nil {
		raw, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return 0, nil, ErrPlatform
		}
		encoded = bytes.NewReader(raw)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, o.baseURL+path, encoded)
	if err != nil {
		return 0, nil, ErrPlatform
	}

	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := o.client.Do(request)
	if err != nil {
		return 0, nil, ErrPlatform
	}
	defer response.Body.Close()
	body, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	if err != nil {
		return response.StatusCode, nil, ErrPlatform
	}
	return response.StatusCode, body, nil
}

type tokenRequest struct {
	AppID     string `json:"app_id"`
	AppSecret string `json:"app_secret"`
}

// codedResponse is the envelope every open API answer shares.
type codedResponse struct {
	Code *int `json:"code"`
}

func (o *openAPI) tenantAccessToken(ctx context.Context, appID, secret string) (string, error) {
	status, body, err := o.call(ctx, http.MethodPost, tokenPath, "",
		tokenRequest{AppID: appID, AppSecret: secret})
	if err != nil {
		return "", err
	}
	var answer struct {
		codedResponse
		TenantAccessToken string `json:"tenant_access_token"`
	}
	if err := decodeOK(status, body, &answer, &answer.codedResponse); err != nil {
		return "", err
	}
	if answer.TenantAccessToken == "" {
		return "", fmt.Errorf("%w: the credential exchange returned no token", ErrPlatform)
	}
	return answer.TenantAccessToken, nil
}

func (o *openAPI) confirmBot(ctx context.Context, token string) error {
	status, body, err := o.call(ctx, http.MethodGet, botInfoPath, token, nil)
	if err != nil {
		return err
	}
	// The bot object is at the top level of this one answer, not under data.
	var answer struct {
		codedResponse
		Bot struct {
			OpenID string `json:"open_id"`
		} `json:"bot"`
	}
	if err := decodeOK(status, body, &answer, &answer.codedResponse); err != nil {
		return err
	}
	if answer.Bot.OpenID == "" {
		return fmt.Errorf("%w: the app has no bot identity", ErrPlatform)
	}
	return nil
}

func (o *openAPI) confirmTenant(ctx context.Context, token string) (string, error) {
	status, body, err := o.call(ctx, http.MethodGet, tenantQueryPath, token, nil)
	if err != nil {
		return "", err
	}
	var answer struct {
		codedResponse
		Data struct {
			Tenant struct {
				TenantKey string `json:"tenant_key"`
			} `json:"tenant"`
		} `json:"data"`
	}
	if err := decodeOK(status, body, &answer, &answer.codedResponse); err != nil {
		return "", err
	}
	if !checkExternalID(answer.Data.Tenant.TenantKey) {
		return "", fmt.Errorf("%w: the app is installed in no readable enterprise", ErrPlatform)
	}
	return answer.Data.Tenant.TenantKey, nil
}

func decodeOK(status int, body []byte, answer any, coded *codedResponse) error {
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: the platform answered with HTTP %d", ErrPlatform, status)
	}
	if err := json.Unmarshal(body, answer); err != nil {
		return fmt.Errorf("%w: the platform answered with a body this adapter cannot read",
			ErrPlatform)
	}
	if coded.Code == nil || *coded.Code != 0 {
		return fmt.Errorf("%w: the platform refused the request", ErrPlatform)
	}
	return nil
}

// textContent is the body of a text message: a JSON object, serialized, and
// carried as the string value of content.
type textContent struct {
	Text string `json:"text"`
}

type replyRequest struct {
	Content string `json:"content"`
	MsgType string `json:"msg_type"`
	UUID    string `json:"uuid"`
}

func (o *openAPI) reply(
	ctx context.Context,
	token, messageID, uuid, text string,
) channels.DeliveryReport {
	unknown := channels.DeliveryReport{Outcome: channels.DeliveryUnknown}
	content, err := json.Marshal(textContent{Text: text})
	if err != nil {
		return channels.DeliveryReport{Outcome: channels.DeliveryRejected}
	}

	path := fmt.Sprintf(replyPathFormat, url.PathEscape(messageID))
	status, body, err := o.call(ctx, http.MethodPost, path, token, replyRequest{
		Content: string(content),
		MsgType: messageTypeText,
		UUID:    uuid,
	})
	if err != nil {
		return unknown
	}
	if status >= http.StatusInternalServerError {
		return unknown
	}
	var answer struct {
		codedResponse
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil || answer.Code == nil {
		return unknown
	}
	if *answer.Code != 0 {
		return channels.DeliveryReport{Outcome: channels.DeliveryRejected}
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return unknown
	}
	if !checkExternalID(answer.Data.MessageID) {
		return unknown
	}
	return channels.DeliveryReport{
		Outcome:           channels.Delivered,
		ExternalMessageID: answer.Data.MessageID,
	}
}
