package channel

import (
	"encoding/base64"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestWeWorkAuthenticatedCallbackMatchesConfiguredApplication(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured string
		callback   string
		outer      string
		wantError  bool
	}{
		{name: "matching", configured: "1000002", callback: "1000002"},
		{name: "numeric identity", configured: "01000002", callback: "1000002"},
		{name: "different application", configured: "1000002", callback: "1000001", wantError: true},
		{name: "missing application", configured: "1000002", wantError: true},
		{name: "unsigned outer application", configured: "1000002", outer: "1000002", wantError: true},
		{name: "invalid application", configured: "1000002", callback: "invalid", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := []byte("0123456789abcdef0123456789abcdef")
			binding := &tenant.ChannelBinding{
				Type: "wework", AppID: test.configured, Token: "verification-token", Secret: "test-corporation-secret",
				EncodingAESKey: strings.TrimSuffix(base64.StdEncoding.EncodeToString(key), "="),
				Config:         map[string]string{"corp_id": "ww0123456789abcdef"},
				AccessPolicy: tenant.ChannelAccessPolicy{
					AllowDirectMessages: true, AllowedUsers: []string{"alice"},
				},
			}
			application := ""
			if test.callback != "" {
				application = "<AgentID>" + test.callback + "</AgentID>"
			}
			plaintext := []byte(`<xml><ToUserName>ww0123456789abcdef</ToUserName><FromUserName>alice</FromUserName><CreateTime>1700000000</CreateTime><MsgType>text</MsgType><Content>approved user in an application</Content><MsgId>42</MsgId>` + application + `</xml>`)
			encrypted := encryptWeWorkForTest(t, key, plaintext, binding.Config["corp_id"])
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			nonce := "nonce-123"
			query := url.Values{
				"timestamp": {timestamp}, "nonce": {nonce},
				"msg_signature": {signWeWorkForTest(binding.Token, timestamp, nonce, encrypted)},
			}
			body := fmt.Sprintf("<xml><Encrypt><![CDATA[%s]]></Encrypt><AgentID>%s</AgentID></xml>", encrypted, test.outer)
			request := httptest.NewRequest("POST", "/webhook?"+query.Encode(), strings.NewReader(body))
			adapter := NewWeWorkAdapter()
			if err := adapter.VerifySignature(request, binding); err != nil {
				t.Fatalf("test callback must have a valid signature: %v", err)
			}
			message, err := adapter.ParseInbound(request, binding)
			if test.wantError {
				if err == nil {
					t.Fatalf("accepted callback for AgentID=%s under configured AppID=%s", test.callback, binding.AppID)
				}
				return
			}
			if err != nil {
				t.Fatalf("rejected matching application: %v", err)
			}
			if err := AuthorizeInbound(binding, message); err != nil {
				t.Fatalf("rejected approved user: %v", err)
			}
		})
	}
}
