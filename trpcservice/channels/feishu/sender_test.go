package feishu

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	channeltypes "github.com/larksuite/channel-sdk-go/types"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

// fakeFeishuServer simulates Feishu tenant-token and message-delivery endpoints.
type fakeFeishuServer struct {
	appID        string
	server       *httptest.Server
	tokenCalls   atomic.Int64
	messageCalls atomic.Int64
	failMessages atomic.Int64
	imageCalls   atomic.Int64
	fileCalls    atomic.Int64
	patchCalls   atomic.Int64
	lastReceive  string
	lastMsgType  string
	lastCreate   string
	lastPatchID  string
	lastPatch    string
}

func newFakeFeishuServer(t *testing.T) *fakeFeishuServer {
	t.Helper()
	fake := &fakeFeishuServer{appID: "cli_" + strings.NewReplacer("/", "_").Replace(t.Name())}
	fake.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			fake.tokenCalls.Add(1)
			var req struct {
				AppID     string `json:"app_id"`
				AppSecret string `json:"app_secret"`
			}
			if err := json.NewDecoder(request.Body).Decode(&req); err != nil || req.AppID != fake.appID || req.AppSecret != "secret" {
				t.Fatalf("bad token request: %+v err=%v", req, err)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "msg": "ok", "tenant_access_token": "t-1", "expire": 7200})
		case "/open-apis/im/v1/messages":
			fake.messageCalls.Add(1)
			if fake.failMessages.Load() > 0 {
				fake.failMessages.Add(-1)
				writer.WriteHeader(http.StatusServiceUnavailable)
				_, _ = writer.Write([]byte(`{"code":503,"msg":"temporary failure"}`))
				return
			}
			if got := request.URL.Query().Get("receive_id_type"); got != "chat_id" {
				t.Fatalf("receive_id_type = %q, want chat_id", got)
			}
			var body struct {
				ReceiveID string `json:"receive_id"`
				MsgType   string `json:"msg_type"`
				Content   string `json:"content"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatalf("decode send body error = %v", err)
			}
			fake.lastReceive = body.ReceiveID
			fake.lastMsgType = body.MsgType
			fake.lastCreate = body.Content
			if (body.MsgType != "post" && body.MsgType != "interactive" && body.MsgType != "image" && body.MsgType != "file") || body.ReceiveID != "oc_1" {
				t.Fatalf("unexpected send body: %+v", body)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"code": 0, "msg": "ok",
				"data": map[string]any{"message_id": "om-reply-1"},
			})
		case "/open-apis/im/v1/images":
			fake.imageCalls.Add(1)
			if err := request.ParseMultipartForm(1 << 20); err != nil {
				t.Fatalf("parse image upload: %v", err)
			}
			if got := request.FormValue("image_type"); got != "message" {
				t.Fatalf("image_type = %q, want message", got)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"code": 0, "msg": "ok", "data": map[string]any{"image_key": "img-key-1"},
			})
		case "/open-apis/im/v1/files":
			fake.fileCalls.Add(1)
			if err := request.ParseMultipartForm(4 << 20); err != nil {
				t.Fatalf("parse file upload: %v", err)
			}
			if got := request.FormValue("file_type"); got != "stream" {
				t.Fatalf("file_type = %q, want stream", got)
			}
			if got := request.FormValue("file_name"); got != "report.docx" {
				t.Fatalf("file_name = %q, want report.docx", got)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"code": 0, "msg": "ok", "data": map[string]any{"file_key": "file-key-1"},
			})
		default:
			if request.Method == http.MethodPatch && strings.HasPrefix(request.URL.Path, "/open-apis/im/v1/messages/") {
				fake.patchCalls.Add(1)
				fake.lastPatchID = strings.TrimPrefix(request.URL.Path, "/open-apis/im/v1/messages/")
				var body struct {
					Content string `json:"content"`
				}
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Fatalf("decode patch body error = %v", err)
				}
				fake.lastPatch = body.Content
				_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "msg": "ok"})
				return
			}
			t.Fatalf("unexpected feishu path %q", request.URL.Path)
		}
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

func newTestFeishuSender(t *testing.T, fake *fakeFeishuServer) *Sender {
	t.Helper()
	connector, err := NewConnector(ConnectorConfig{
		AppID: fake.appID, AppSecret: "secret",
		BaseURL: fake.server.URL, HTTPClient: fake.server.Client(),
	})
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}
	sender, err := connector.Sender()
	if err != nil {
		t.Fatalf("Connector.Sender() error = %v", err)
	}
	return sender
}

func TestFeishuSenderSendsText(t *testing.T) {
	t.Parallel()
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)

	receipt, err := sender.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Feishu, ConversationID: "oc_1",
	}, channels.OutboundMessage{Text: "你好飞书"})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got, want := receipt.ExternalMessageID, "om-reply-1"; got != want {
		t.Fatalf("external message id = %q, want %q", got, want)
	}
	if fake.lastReceive != "oc_1" {
		t.Fatalf("receive id = %q, want oc_1", fake.lastReceive)
	}
	if fake.lastMsgType != "post" || !strings.Contains(fake.lastCreate, `"tag":"md"`) {
		t.Fatalf("message type/content = %q / %q, want SDK-rendered Markdown post", fake.lastMsgType, fake.lastCreate)
	}
	if fake.tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want 1", fake.tokenCalls.Load())
	}
}

func TestFeishuSenderUsesNativeImageMessageForImageArtifact(t *testing.T) {
	t.Parallel()
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)
	dir := t.TempDir()
	path := filepath.Join(dir, "result.png")
	if err := os.WriteFile(path, []byte("fake-image-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	receipt, err := sender.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Feishu, ConversationID: "oc_1",
	}, channels.OutboundMessage{Files: []channels.OutboundFile{{Path: path, Name: "result.png"}}})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if receipt.ExternalMessageID != "om-reply-1" || fake.imageCalls.Load() != 1 || fake.lastMsgType != "image" {
		t.Fatalf("receipt/image calls/msg type = %#v / %d / %q", receipt, fake.imageCalls.Load(), fake.lastMsgType)
	}
	if !strings.Contains(fake.lastCreate, "img-key-1") {
		t.Fatalf("image content = %q, want uploaded image key", fake.lastCreate)
	}
}

func TestFeishuSenderUsesNativeFileMessageForDocumentArtifact(t *testing.T) {
	t.Parallel()
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)
	dir := t.TempDir()
	path := filepath.Join(dir, "report.docx")
	if err := os.WriteFile(path, []byte("fake-docx-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	receipt, err := sender.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Feishu, ConversationID: "oc_1",
	}, channels.OutboundMessage{Files: []channels.OutboundFile{{Path: path, Name: "report.docx"}}})
	if err != nil {
		t.Fatalf("Send(document) error = %v", err)
	}
	if receipt.ExternalMessageID != "om-reply-1" || fake.fileCalls.Load() != 1 || fake.lastMsgType != "file" || !strings.Contains(fake.lastCreate, "file-key-1") {
		t.Fatalf("receipt/file calls/msg type/content = %#v / %d / %q / %q", receipt, fake.fileCalls.Load(), fake.lastMsgType, fake.lastCreate)
	}
}

func TestFeishuMediaKindUsesOnlyProviderNativeAudioVideoFormats(t *testing.T) {
	t.Parallel()
	tests := map[string]channeltypes.MediaKind{
		"voice.opus": channeltypes.MediaKindAudio,
		"clip.mp4":   channeltypes.MediaKindVideo,
		"song.mp3":   channeltypes.MediaKindFile,
		"voice.wav":  channeltypes.MediaKindFile,
		"clip.mov":   channeltypes.MediaKindFile,
	}
	for name, want := range tests {
		if got := feishuMediaKind(name); got != want {
			t.Fatalf("feishuMediaKind(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestFeishuSenderReusesTenantToken(t *testing.T) {
	t.Parallel()
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)

	for i := 0; i < 3; i++ {
		if _, err := sender.Send(context.Background(), channels.ReplyTarget{
			Channel: channels.Feishu, ConversationID: "oc_1",
		}, channels.OutboundMessage{Text: "你好"}); err != nil {
			t.Fatalf("Send() #%d error = %v", i, err)
		}
	}
	if got := fake.tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1 (SDK cache)", got)
	}
}

func TestFeishuSenderUsesChannelRetry(t *testing.T) {
	fake := newFakeFeishuServer(t)
	fake.failMessages.Store(1)
	sender := newTestFeishuSender(t, fake)

	if _, err := sender.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Feishu, ConversationID: "oc_1",
	}, channels.OutboundMessage{Text: "重试一次"}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got := fake.messageCalls.Load(); got != 2 {
		t.Fatalf("message calls = %d, want 2 with one SDK retry", got)
	}
}

func TestFeishuSenderUsesChannelMarkdownChunking(t *testing.T) {
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)

	if _, err := sender.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Feishu, ConversationID: "oc_1",
	}, channels.OutboundMessage{Text: strings.Repeat("一行内容\n", 1000)}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got := fake.messageCalls.Load(); got < 3 {
		t.Fatalf("message calls = %d, want SDK to split long Markdown into at least 3 messages", got)
	}
}

func TestFeishuProgressUpdatesOneCard(t *testing.T) {
	t.Parallel()
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)
	target := channels.ReplyTarget{Channel: channels.Feishu, ConversationID: "oc_1", ConversationScope: channels.ConversationDirect}

	receipt, err := sender.StartProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("StartProgress() error = %v", err)
	}
	if receipt.ExternalMessageID != "om-reply-1" || fake.messageCalls.Load() != 1 {
		t.Fatalf("progress receipt/calls = %#v / %d", receipt, fake.messageCalls.Load())
	}
	var initial map[string]any
	if err := json.Unmarshal([]byte(fake.lastCreate), &initial); err != nil {
		t.Fatalf("decode initial card: %v", err)
	}
	if _, exists := initial["header"]; exists {
		t.Fatalf("ordinary progress card should not have a header: %#v", initial)
	}
	if _, exists := initial["config"]; exists {
		t.Fatalf("ordinary progress card should not force wide-screen layout: %#v", initial)
	}
	elements, _ := initial["elements"].([]any)
	if len(elements) == 0 {
		if body, ok := initial["body"].(map[string]any); ok {
			elements, _ = body["elements"].([]any)
		}
	}
	if len(elements) != 1 || elements[0].(map[string]any)["tag"] != "markdown" {
		t.Fatalf("ordinary progress card should use a schema-v2 markdown element: %#v", initial)
	}
	if !strings.Contains(fake.lastCreate, "正在回复") {
		t.Fatalf("initial progress card = %q", fake.lastCreate)
	}
	if err := sender.UpdateProgress(context.Background(), target, receipt.ExternalMessageID, "回答到一半"); err != nil {
		t.Fatalf("UpdateProgress() error = %v", err)
	}
	finalReceipt, err := sender.Send(context.Background(), target, channels.OutboundMessage{
		Text: "最终回答", UpdateMessageID: receipt.ExternalMessageID,
	})
	if err != nil {
		t.Fatalf("Send(final update) error = %v", err)
	}
	if finalReceipt.ExternalMessageID != receipt.ExternalMessageID || fake.messageCalls.Load() != 1 || fake.patchCalls.Load() != 2 {
		t.Fatalf("final receipt/create/patch = %#v / %d / %d", finalReceipt, fake.messageCalls.Load(), fake.patchCalls.Load())
	}
	if fake.lastPatchID != receipt.ExternalMessageID || !strings.Contains(fake.lastPatch, "最终回答") {
		t.Fatalf("final patch id/content = %q / %q", fake.lastPatchID, fake.lastPatch)
	}
	if strings.Contains(fake.lastPatch, "正在回复") {
		t.Fatalf("final card kept progress copy: %q", fake.lastPatch)
	}
}

func TestFeishuApprovalReusesProgressCard(t *testing.T) {
	t.Parallel()
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)
	target := channels.ReplyTarget{Channel: channels.Feishu, ConversationID: "oc_1", ConversationScope: channels.ConversationDirect}

	receipt, err := sender.StartProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("StartProgress() error = %v", err)
	}
	if err := sender.UpdateProgressCard(context.Background(), target, receipt.ExternalMessageID, channels.InteractiveCard{
		Title: "确认执行操作", Body: "提交退款申请",
		Actions: []channels.CardAction{{ActionID: "approval:approve:token-1", Label: "确认执行", Style: "primary"}},
	}); err != nil {
		t.Fatalf("UpdateProgressCard() error = %v", err)
	}
	if fake.messageCalls.Load() != 1 || fake.patchCalls.Load() != 1 {
		t.Fatalf("create/patch calls = %d/%d, want one original message and one in-place patch", fake.messageCalls.Load(), fake.patchCalls.Load())
	}
	if fake.lastPatchID != receipt.ExternalMessageID || !strings.Contains(fake.lastPatch, "确认执行操作") || !strings.Contains(fake.lastPatch, "approval:approve:token-1") {
		t.Fatalf("approval patch id/content = %q / %q", fake.lastPatchID, fake.lastPatch)
	}
	finalReceipt, err := sender.Send(context.Background(), target, channels.OutboundMessage{
		Text: "退款申请已提交", UpdateMessageID: receipt.ExternalMessageID,
	})
	if err != nil {
		t.Fatalf("Send(final update) error = %v", err)
	}
	if finalReceipt.ExternalMessageID != receipt.ExternalMessageID || fake.messageCalls.Load() != 1 || fake.patchCalls.Load() != 2 {
		t.Fatalf("final receipt/create/patch = %#v / %d / %d", finalReceipt, fake.messageCalls.Load(), fake.patchCalls.Load())
	}
	if fake.lastPatchID != receipt.ExternalMessageID || !strings.Contains(fake.lastPatch, "退款申请已提交") || strings.Contains(fake.lastPatch, "确认执行操作") {
		t.Fatalf("final patch id/content = %q / %q", fake.lastPatchID, fake.lastPatch)
	}
}

func TestFeishuInteractiveCardUsesCompactActionLayout(t *testing.T) {
	t.Parallel()
	content, err := buildFeishuCard(&channels.InteractiveCard{
		Title: "确认操作",
		Body:  "确认继续执行？",
		Actions: []channels.CardAction{{
			ActionID: "approval:approve:token-1", Label: "确认", Style: "primary",
		}},
	}, "", channels.ConversationDirect)
	if err != nil {
		t.Fatalf("buildFeishuCard() error = %v", err)
	}
	var card map[string]any
	if err := json.Unmarshal([]byte(content), &card); err != nil {
		t.Fatalf("decode card: %v", err)
	}
	if card["schema"] != "2.0" {
		t.Fatalf("interactive card schema = %#v, want 2.0", card["schema"])
	}
	header, _ := card["header"].(map[string]any)
	if header["template"] != "grey" {
		t.Fatalf("header template = %#v, want neutral grey", header)
	}
	body, _ := card["body"].(map[string]any)
	elements, _ := body["elements"].([]any)
	if len(elements) != 3 || elements[1].(map[string]any)["tag"] != "hr" || elements[2].(map[string]any)["tag"] != "column_set" {
		t.Fatalf("v2 action layout = %#v", elements)
	}
	columns, _ := elements[2].(map[string]any)["columns"].([]any)
	if len(columns) != 1 {
		t.Fatalf("v2 action columns = %#v", columns)
	}
	columnElements, _ := columns[0].(map[string]any)["elements"].([]any)
	if len(columnElements) != 1 {
		t.Fatalf("v2 action column elements = %#v", columnElements)
	}
	button, _ := columnElements[0].(map[string]any)
	behaviors, _ := button["behaviors"].([]any)
	if button["tag"] != "button" || len(behaviors) != 1 {
		t.Fatalf("v2 button = %#v", button)
	}
	behavior, _ := behaviors[0].(map[string]any)
	value, _ := behavior["value"].(map[string]any)
	if behavior["type"] != "callback" || value["action_id"] != "approval:approve:token-1" || value["conversation_scope"] != "direct" {
		t.Fatalf("v2 callback behavior = %#v", behavior)
	}
}

func TestFeishuResolvedApprovalRemainsCardV2(t *testing.T) {
	t.Parallel()
	content, err := buildFeishuCard(&channels.InteractiveCard{
		Title: "已确认", Body: "已确认，正在继续执行退款申请", State: "approved",
	}, "", channels.ConversationDirect)
	if err != nil {
		t.Fatalf("buildFeishuCard() error = %v", err)
	}
	var card map[string]any
	if err := json.Unmarshal([]byte(content), &card); err != nil {
		t.Fatalf("decode card: %v", err)
	}
	if card["schema"] != "2.0" {
		t.Fatalf("resolved approval schema = %#v, want 2.0", card["schema"])
	}
	body, _ := card["body"].(map[string]any)
	elements, _ := body["elements"].([]any)
	if len(elements) != 1 || elements[0].(map[string]any)["tag"] != "markdown" {
		t.Fatalf("resolved approval elements = %#v", elements)
	}
}

func TestBuildFeishuCardCoversFallbackLinkAndActionFiltering(t *testing.T) {
	t.Parallel()
	content, err := buildFeishuCard(nil, "普通回复", channels.ConversationDirect)
	if err != nil || !strings.Contains(content, "普通回复") {
		t.Fatalf("buildFeishuCard(fallback) = %q, %v", content, err)
	}
	if _, err := buildFeishuCard(nil, "   ", channels.ConversationDirect); err == nil {
		t.Fatal("buildFeishuCard(empty) error = nil")
	}
	content, err = buildFeishuCard(&channels.InteractiveCard{
		Title: "订单信息", Body: "已找到订单",
		Actions: []channels.CardAction{
			{Label: " 查看详情 ", URL: "https://support.example.test/orders/42", Style: "primary"},
			{Label: "默认按钮", URL: "https://support.example.test/help", Style: "unexpected"},
			{Label: "   ", URL: "https://support.example.test/ignored"},
			{Label: "无动作"},
		},
	}, "", channels.ConversationGroup)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["schema"] != "2.0" {
		t.Fatalf("link-only card schema = %#v, want 2.0", payload["schema"])
	}
	body, _ := payload["body"].(map[string]any)
	elements, _ := body["elements"].([]any)
	if len(elements) != 3 {
		t.Fatalf("card elements = %#v", elements)
	}
	columnSet, _ := elements[2].(map[string]any)
	columns, _ := columnSet["columns"].([]any)
	if len(columns) != 2 {
		t.Fatalf("filtered columns = %#v", columns)
	}
	primary, _ := columns[0].(map[string]any)
	primaryElements, _ := primary["elements"].([]any)
	primaryButton, _ := primaryElements[0].(map[string]any)
	if primaryButton["type"] != "primary_filled" {
		t.Fatalf("primary button type = %#v", primaryButton["type"])
	}
	behaviors, _ := primaryButton["behaviors"].([]any)
	behavior, _ := behaviors[0].(map[string]any)
	if behavior["type"] != "open_url" || behavior["default_url"] != "https://support.example.test/orders/42" {
		t.Fatalf("primary button behavior = %#v", behavior)
	}
	defaultColumn, _ := columns[1].(map[string]any)
	defaultElements, _ := defaultColumn["elements"].([]any)
	defaultButton, _ := defaultElements[0].(map[string]any)
	if defaultButton["type"] != "default" {
		t.Fatalf("default button type = %#v", defaultButton["type"])
	}
}

func TestFeishuDeleteMessageFailsClosedWithoutUsableTargetOrClient(t *testing.T) {
	t.Parallel()
	sender := newSender(nil, nil)
	if err := sender.DeleteMessage(context.Background(), channels.ReplyTarget{Channel: channels.Telegram, ConversationID: "chat"}, "message-1"); err == nil {
		t.Fatal("DeleteMessage() accepted wrong channel")
	}
	if err := sender.DeleteMessage(context.Background(), channels.ReplyTarget{Channel: channels.Feishu}, "message-1"); err == nil {
		t.Fatal("DeleteMessage() accepted empty conversation")
	}
	if err := sender.DeleteMessage(context.Background(), channels.ReplyTarget{Channel: channels.Feishu, ConversationID: "oc_1"}, "message-1"); err == nil || !strings.Contains(err.Error(), "raw client is unavailable") {
		t.Fatalf("DeleteMessage(nil client) error = %v", err)
	}
}

func TestRedisCacheStoresAndExpiresToken(t *testing.T) {
	t.Parallel()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(server.Close)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache, err := NewRedisCache(client)
	if err != nil {
		t.Fatalf("NewRedisCache() error = %v", err)
	}

	ctx := context.Background()
	if err := cache.Set(ctx, "tenant-access-token", "token-1", time.Minute); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got, err := cache.Get(ctx, "tenant-access-token"); err != nil || got != "token-1" {
		t.Fatalf("Get() = %q, %v; want token-1, nil", got, err)
	}
	server.FastForward(time.Minute)
	if got, err := cache.Get(ctx, "tenant-access-token"); err != nil || got != "" {
		t.Fatalf("Get() after expiry = %q, %v; want empty, nil", got, err)
	}
}

func TestFeishuSenderRejectsNonFeishuTarget(t *testing.T) {
	t.Parallel()
	fake := newFakeFeishuServer(t)
	sender := newTestFeishuSender(t, fake)
	if _, err := sender.Send(context.Background(), channels.ReplyTarget{
		Channel: channels.Telegram, ConversationID: "oc_1",
	}, channels.OutboundMessage{Text: "hello"}); err == nil {
		t.Fatal("expected error for non-Feishu target")
	}
}
