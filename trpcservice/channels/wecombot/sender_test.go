package wecombot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type fakeRequester struct {
	calls     []requestCall
	responses map[string]WireFrame
	failures  map[string]int
}

type requestCall struct {
	command string
	reqID   string
	body    map[string]any
}

func (f *fakeRequester) Request(ctx context.Context, command, reqID string, body any) error {
	_, err := f.RequestFrame(ctx, command, reqID, body)
	return err
}

func (f *fakeRequester) RequestFrame(_ context.Context, command, reqID string, body any) (WireFrame, error) {
	encoded, _ := json.Marshal(body)
	var decoded map[string]any
	_ = json.Unmarshal(encoded, &decoded)
	f.calls = append(f.calls, requestCall{command: command, reqID: reqID, body: decoded})
	if f.failures[command] > 0 {
		f.failures[command]--
		return WireFrame{}, errors.New("temporary provider failure")
	}
	if response, ok := f.responses[command]; ok {
		return response, nil
	}
	return WireFrame{}, nil
}

func TestSenderStreamsAgainstInboundReplyToken(t *testing.T) {
	requester := &fakeRequester{}
	sender := newSender(requester, 16<<20)
	target := channels.ReplyTarget{
		Channel: channels.WeCom, ConversationID: "chat-1", ProviderReplyToken: "callback-req-1",
	}
	receipt, err := sender.StartProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("StartProgress() error = %v", err)
	}
	if receipt.ExternalMessageID == "" || len(requester.calls) != 1 {
		t.Fatalf("progress receipt/calls = %#v / %#v", receipt, requester.calls)
	}
	first := requester.calls[0]
	if first.command != "aibot_respond_msg" || first.reqID != "callback-req-1" {
		t.Fatalf("progress request = %#v", first)
	}
	stream, _ := first.body["stream"].(map[string]any)
	if stream["id"] != receipt.ExternalMessageID || stream["finish"] != false {
		t.Fatalf("progress stream = %#v", stream)
	}
	if _, exists := stream["content"]; exists {
		t.Fatalf("initial progress must not render placeholder content: %#v", stream)
	}
	if err := sender.UpdateProgress(context.Background(), target, receipt.ExternalMessageID, "已经完成一半"); err != nil {
		t.Fatalf("UpdateProgress() error = %v", err)
	}
	second := requester.calls[1]
	secondStream, _ := second.body["stream"].(map[string]any)
	if second.command != "aibot_respond_msg" || second.reqID != "callback-req-1" || secondStream["id"] != receipt.ExternalMessageID || secondStream["content"] != "已经完成一半" {
		t.Fatalf("updated progress = %#v", second)
	}
	if err := sender.UpdateProgress(context.Background(), target, receipt.ExternalMessageID, "已经完成一半，而且还有更多内容"); err != nil {
		t.Fatalf("UpdateProgress(throttled) error = %v", err)
	}
	if len(requester.calls) != 2 {
		t.Fatalf("rapid progress update should be coalesced, calls = %#v", requester.calls)
	}
	finalReceipt, err := sender.Send(context.Background(), target, channels.OutboundMessage{
		Text: "最终回答", UpdateMessageID: receipt.ExternalMessageID,
	})
	if err != nil {
		t.Fatalf("Send(final stream) error = %v", err)
	}
	if finalReceipt.ExternalMessageID != receipt.ExternalMessageID || len(requester.calls) != 3 {
		t.Fatalf("final receipt/calls = %#v / %#v", finalReceipt, requester.calls)
	}
	finalStream, _ := requester.calls[2].body["stream"].(map[string]any)
	if requester.calls[2].command != "aibot_respond_msg" || finalStream["id"] != receipt.ExternalMessageID || finalStream["finish"] != true || finalStream["content"] != "最终回答" {
		t.Fatalf("final stream = %#v", requester.calls[2])
	}
}

func TestSenderAddsApprovalCardToExistingProgressStream(t *testing.T) {
	requester := &fakeRequester{}
	sender := newSender(requester, 16<<20)
	sender.progressUpdateInterval = 0
	target := channels.ReplyTarget{
		Channel: channels.WeCom, ConversationID: "chat-1", ProviderReplyToken: "callback-req-1",
	}
	receipt, err := sender.StartProgress(context.Background(), target)
	if err != nil {
		t.Fatalf("StartProgress() error = %v", err)
	}
	if err := sender.UpdateProgress(context.Background(), target, receipt.ExternalMessageID, "正在准备退款申请"); err != nil {
		t.Fatalf("UpdateProgress() error = %v", err)
	}
	card := channels.InteractiveCard{
		Title: "确认执行操作", Body: "提交退款申请",
		Actions: []channels.CardAction{
			{ActionID: "approval:approve:token-1", Label: "确认执行", Style: "primary"},
			{ActionID: "approval:reject:token-1", Label: "取消", Style: "danger"},
		},
	}
	if err := sender.UpdateProgressCard(context.Background(), target, receipt.ExternalMessageID, card); err != nil {
		t.Fatalf("UpdateProgressCard() error = %v", err)
	}
	if len(requester.calls) != 3 {
		t.Fatalf("calls = %#v, want start + progress + one combined-card update", requester.calls)
	}
	call := requester.calls[2]
	if call.command != "aibot_respond_msg" || call.reqID != target.ProviderReplyToken {
		t.Fatalf("combined approval call = %#v", call)
	}
	if call.body["msgtype"] != "stream_with_template_card" {
		t.Fatalf("combined approval msgtype = %#v", call.body["msgtype"])
	}
	stream, _ := call.body["stream"].(map[string]any)
	if stream["id"] != receipt.ExternalMessageID || stream["content"] != "正在准备退款申请" || stream["finish"] != false {
		t.Fatalf("combined approval stream = %#v", stream)
	}
	template, _ := call.body["template_card"].(map[string]any)
	if template["task_id"] != "token-1" || template["card_type"] != "button_interaction" {
		t.Fatalf("combined approval template = %#v", template)
	}
	if err := sender.UpdateProgressCard(context.Background(), target, receipt.ExternalMessageID, card); err != nil {
		t.Fatalf("repeated UpdateProgressCard() error = %v", err)
	}
	if len(requester.calls) != 3 {
		t.Fatalf("repeated approval update sent another template card: %#v", requester.calls)
	}
	finalReceipt, err := sender.Send(context.Background(), target, channels.OutboundMessage{
		Text: "退款申请已提交", UpdateMessageID: receipt.ExternalMessageID,
	})
	if err != nil {
		t.Fatalf("Send(final combined stream) error = %v", err)
	}
	if finalReceipt.ExternalMessageID != receipt.ExternalMessageID || len(requester.calls) != 4 {
		t.Fatalf("final receipt/calls = %#v / %#v", finalReceipt, requester.calls)
	}
	finalCall := requester.calls[3]
	if finalCall.command != "aibot_respond_msg" || finalCall.reqID != target.ProviderReplyToken || finalCall.body["msgtype"] != "stream_with_template_card" {
		t.Fatalf("final combined call = %#v", finalCall)
	}
	finalStream, _ := finalCall.body["stream"].(map[string]any)
	if finalStream["id"] != receipt.ExternalMessageID || finalStream["content"] != "退款申请已提交" || finalStream["finish"] != true {
		t.Fatalf("final combined stream = %#v", finalStream)
	}
	if _, exists := finalCall.body["template_card"]; exists {
		t.Fatalf("final combined update must not attach the template card twice: %#v", finalCall.body)
	}
	for _, sent := range requester.calls {
		if sent.command == "aibot_send_msg" {
			t.Fatalf("approval created a second visible message: %#v", sent)
		}
	}
}

func TestCompactWeComMarkdownKeepsRepliesDenseWithoutTouchingCode(t *testing.T) {
	t.Parallel()
	input := "# 功能范围\n\n\n## 售后咨询\n\n正文\n\n```md\n# code heading\n```"
	want := "**功能范围**\n\n**售后咨询**\n\n正文\n\n```md\n# code heading\n```"
	if got := compactWeComMarkdown(input); got != want {
		t.Fatalf("compact markdown = %q, want %q", got, want)
	}
}

func TestSenderSendsInteractiveCard(t *testing.T) {
	requester := &fakeRequester{}
	sender := newSender(requester, 16<<20)
	_, err := sender.Send(context.Background(), channels.ReplyTarget{Channel: channels.WeCom, ConversationID: "chat-1"}, channels.OutboundMessage{
		Card: &channels.InteractiveCard{Title: "需要确认", Body: "是否执行操作？", Actions: []channels.CardAction{
			{ActionID: "approval:approve:token-1", Label: "批准", Style: "primary"},
			{ActionID: "approval:reject:token-1", Label: "拒绝", Style: "danger"},
		}},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if len(requester.calls) != 1 || requester.calls[0].command != "aibot_send_msg" {
		t.Fatalf("card calls = %#v", requester.calls)
	}
	card, _ := requester.calls[0].body["template_card"].(map[string]any)
	if card["card_type"] != "button_interaction" {
		t.Fatalf("template card = %#v", card)
	}
	buttons, _ := card["button_list"].([]any)
	if len(buttons) != 2 {
		t.Fatalf("button list = %#v", buttons)
	}
}

func TestTemplateCardMapsURLActionsToNativeJumpList(t *testing.T) {
	t.Parallel()
	card, err := templateCard(channels.InteractiveCard{
		Title: "订单详情", Body: "查看当前订单的完整信息。",
		Actions: []channels.CardAction{{Label: "打开订单", URL: "https://example.com/orders/TF-1001"}},
	}, "task-1")
	if err != nil {
		t.Fatalf("templateCard() error = %v", err)
	}
	if card["card_type"] != "text_notice" {
		t.Fatalf("card type = %#v, want text_notice", card["card_type"])
	}
	jumps, _ := card["jump_list"].([]map[string]any)
	if len(jumps) != 1 || jumps[0]["type"] != 1 || jumps[0]["title"] != "打开订单" || jumps[0]["url"] != "https://example.com/orders/TF-1001" {
		t.Fatalf("jump_list = %#v", card["jump_list"])
	}
	action, _ := card["card_action"].(map[string]any)
	if action["type"] != 1 || action["url"] != "https://example.com/orders/TF-1001" {
		t.Fatalf("card_action = %#v", action)
	}
}

func TestTemplateCardRejectsMixedCallbackAndURLActions(t *testing.T) {
	t.Parallel()
	_, err := templateCard(channels.InteractiveCard{
		Body: "请选择操作",
		Actions: []channels.CardAction{
			{ActionID: "approval:approve:token-1", Label: "确认"},
			{Label: "查看详情", URL: "https://example.com"},
		},
	}, "task-1")
	if err == nil {
		t.Fatal("mixed callback and URL actions must be rejected instead of silently dropping one action")
	}
}

func TestTemplateCardTurnsResolvedApprovalIntoTextNotice(t *testing.T) {
	t.Parallel()
	card, err := templateCard(channels.InteractiveCard{
		Title: "已确认", Body: "已确认，正在继续执行：为指定订单发起退款申请", State: "approved",
	}, "task-1")
	if err != nil {
		t.Fatalf("templateCard() error = %v", err)
	}
	if card["card_type"] != "text_notice" || card["task_id"] != "task-1" {
		t.Fatalf("resolved card = %#v", card)
	}
	action, _ := card["card_action"].(map[string]any)
	if action["type"] != 0 {
		t.Fatalf("resolved card_action = %#v, want native no-op action", action)
	}
	if _, exists := card["button_list"]; exists {
		t.Fatalf("resolved card must not retain approval buttons: %#v", card)
	}
	mainTitle, _ := card["main_title"].(map[string]string)
	if mainTitle["title"] != "已确认" {
		t.Fatalf("resolved card title = %#v", mainTitle)
	}
}

func TestUpdateCardReplacesApprovalButtonsWithResolvedNotice(t *testing.T) {
	t.Parallel()
	requester := &fakeRequester{}
	sender := newSender(requester, 16<<20)
	target := channels.ReplyTarget{
		Channel: channels.WeCom, ConversationID: "chat-1", ProviderReplyToken: "callback-req-2",
	}
	if err := sender.UpdateCard(context.Background(), target, "task-1", channels.InteractiveCard{
		Title: "已确认", Body: "已确认，正在继续执行：为指定订单发起退款申请", State: "approved",
	}); err != nil {
		t.Fatalf("UpdateCard() error = %v", err)
	}
	if len(requester.calls) != 1 || requester.calls[0].command != "aibot_respond_update_msg" || requester.calls[0].reqID != "callback-req-2" {
		t.Fatalf("update card call = %#v", requester.calls)
	}
	if requester.calls[0].body["response_type"] != "update_template_card" {
		t.Fatalf("update body = %#v", requester.calls[0].body)
	}
	card, _ := requester.calls[0].body["template_card"].(map[string]any)
	if card["card_type"] != "text_notice" || card["task_id"] != "task-1" {
		t.Fatalf("resolved update card = %#v", card)
	}
	action, _ := card["card_action"].(map[string]any)
	if action["type"] != float64(0) {
		t.Fatalf("resolved update card_action = %#v, want native no-op action", action)
	}
	if _, exists := card["button_list"]; exists {
		t.Fatalf("resolved update card retained buttons: %#v", card)
	}
}

func TestSenderUploadsAndSendsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.txt")
	if err := os.WriteFile(path, []byte("artifact-body"), 0o600); err != nil {
		t.Fatal(err)
	}
	requester := &fakeRequester{responses: map[string]WireFrame{
		"aibot_upload_media_init":   {Body: json.RawMessage(`{"upload_id":"upload-1"}`)},
		"aibot_upload_media_finish": {Body: json.RawMessage(`{"media_id":"media-1","type":"file"}`)},
	}}
	sender := newSender(requester, 16<<20)
	_, err := sender.Send(context.Background(), channels.ReplyTarget{Channel: channels.WeCom, ConversationID: "chat-1"}, channels.OutboundMessage{
		Files: []channels.OutboundFile{{Path: path, Name: "report.txt"}},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	commands := make([]string, 0, len(requester.calls))
	for _, call := range requester.calls {
		commands = append(commands, call.command)
	}
	want := []string{"aibot_upload_media_init", "aibot_upload_media_chunk", "aibot_upload_media_finish", "aibot_send_msg"}
	if len(commands) != len(want) {
		t.Fatalf("commands = %v, want %v", commands, want)
	}
	for i := range want {
		if commands[i] != want[i] {
			t.Fatalf("commands = %v, want %v", commands, want)
		}
	}
	media, _ := requester.calls[len(requester.calls)-1].body["file"].(map[string]any)
	if media["media_id"] != "media-1" {
		t.Fatalf("media payload = %#v", media)
	}
}

func TestSenderRetriesMediaChunkLikeOfficialSDK(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.txt")
	if err := os.WriteFile(path, []byte("artifact-body"), 0o600); err != nil {
		t.Fatal(err)
	}
	requester := &fakeRequester{
		failures: map[string]int{"aibot_upload_media_chunk": 1},
		responses: map[string]WireFrame{
			"aibot_upload_media_init":   {Body: json.RawMessage(`{"upload_id":"upload-1"}`)},
			"aibot_upload_media_finish": {Body: json.RawMessage(`{"media_id":"media-1","type":"file"}`)},
		},
	}
	sender := newSender(requester, 16<<20)
	if _, err := sender.Send(context.Background(), channels.ReplyTarget{Channel: channels.WeCom, ConversationID: "chat-1"}, channels.OutboundMessage{
		Files: []channels.OutboundFile{{Path: path, Name: "report.txt"}},
	}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	chunkCalls := 0
	for _, call := range requester.calls {
		if call.command == "aibot_upload_media_chunk" {
			chunkCalls++
		}
	}
	if chunkCalls != 2 {
		t.Fatalf("chunk calls = %d, want initial attempt plus one retry", chunkCalls)
	}
}
