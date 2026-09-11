package feishu

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// TestRenderCardCarriesButtonsAndCallbackValues covers the "Agent Event →
// 卡片消息" half of the IM requirement: the approval card must contain both the
// body text and one button per decision, each carrying the callback value the
// platform returns on click.
func TestRenderCardCarriesButtonsAndCallbackValues(t *testing.T) {
	card := channels.Card{
		Title:   "需要人工审批",
		Content: "Agent 请求执行工具：execute_code",
		Actions: []channels.CardAction{
			{Text: "批准", Value: map[string]string{"session_id": "t1:feishu:user:u1", "decision": "approve"}},
			{Text: "拒绝", Value: map[string]string{"session_id": "t1:feishu:user:u1", "decision": "deny"}},
		},
	}
	raw := renderCard(card)

	var payload struct {
		Header struct {
			Title struct{ Content string } `json:"title"`
		} `json:"header"`
		Elements []struct {
			Tag     string `json:"tag"`
			Content string `json:"content"`
			Actions []struct {
				Tag      string                   `json:"tag"`
				Text     struct{ Content string } `json:"text"`
				Value    map[string]string        `json:"value"`
				Behavior []struct {
					Type  string            `json:"type"`
					Value map[string]string `json:"value"`
				} `json:"behaviors"`
			} `json:"actions"`
		} `json:"elements"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("renderCard produced invalid JSON: %v\n%s", err, raw)
	}
	if payload.Header.Title.Content != "需要人工审批" {
		t.Errorf("card title = %q, want 需要人工审批", payload.Header.Title.Content)
	}
	if len(payload.Elements) != 2 {
		t.Fatalf("card elements = %d, want body + action row: %s", len(payload.Elements), raw)
	}
	if !strings.Contains(payload.Elements[0].Content, "execute_code") {
		t.Errorf("body lost the tool name: %q", payload.Elements[0].Content)
	}
	actions := payload.Elements[1].Actions
	if len(actions) != 2 {
		t.Fatalf("buttons = %d, want 2", len(actions))
	}
	if actions[0].Text.Content != "批准" || actions[1].Text.Content != "拒绝" {
		t.Errorf("button labels = %q/%q", actions[0].Text.Content, actions[1].Text.Content)
	}
	// The click must come back over the long connection, and the value must
	// carry the decision: without these two the button is decorative.
	for i, act := range actions {
		if len(act.Behavior) != 1 || act.Behavior[0].Type != "callback" {
			t.Errorf("button %d behaviors = %+v, want one callback behavior", i, act.Behavior)
		}
		if act.Value["session_id"] == "" || act.Value["decision"] == "" {
			t.Errorf("button %d value = %v, want session_id + decision", i, act.Value)
		}
	}
	if actions[0].Value["decision"] != "approve" || actions[1].Value["decision"] != "deny" {
		t.Errorf("decisions = %q/%q, want approve/deny", actions[0].Value["decision"], actions[1].Value["decision"])
	}
}

// TestRenderCardWithoutActionsStaysAMarkdownCard keeps the plain card path
// (no buttons) working: a card without actions must not emit an empty action row.
func TestRenderCardWithoutActionsStaysAMarkdownCard(t *testing.T) {
	raw := renderCard(channels.Card{Content: "just text"})
	if strings.Contains(raw, `"tag":"action"`) {
		t.Errorf("card without actions emitted an action row: %s", raw)
	}
	if !strings.Contains(raw, "just text") {
		t.Errorf("card body lost: %s", raw)
	}
}

// TestCardActionBecomesAnApprovalReply is the inbound half: a button click must
// turn into the same 批准/拒绝 reply the worker already classifies, carrying the
// session it belongs to and a dedup id that distinguishes the two buttons.
func TestCardActionBecomesAnApprovalReply(t *testing.T) {
	env := &cardActionEnvelope{CardAction: &cardActionPayload{
		EventID:   "evt-1",
		OpenID:    "ou_operator",
		MessageID: "om_card",
		Value: map[string]string{
			"session_id": "t1:feishu:user:u1",
			"decision":   "approve",
		},
	}}

	in := CardActionToInbound("t1", env)
	if in == nil {
		t.Fatal("card action did not produce an inbound message")
	}
	if in.Content != "批准" {
		t.Errorf("content = %q, want 批准 (the wording the approval classifier knows)", in.Content)
	}
	if in.SessionID != "t1:feishu:user:u1" || in.TenantID != "t1" {
		t.Errorf("routing lost: tenant=%q session=%q", in.TenantID, in.SessionID)
	}
	if in.UserID != "ou_operator" {
		t.Errorf("user = %q, want the operator so the audit blames the right person", in.UserID)
	}
	if in.PlatformMsgID != "evt-1" {
		t.Errorf("dedup id = %q, want the platform event id", in.PlatformMsgID)
	}

	// Deny maps to the other wording; the same card's two buttons must not share
	// a dedup key (otherwise the second click is swallowed as a duplicate).
	deny := &cardActionEnvelope{CardAction: &cardActionPayload{
		EventID: "evt-2",
		OpenID:  "ou_operator",
		Value:   map[string]string{"session_id": "t1:feishu:user:u1", "decision": "deny"},
	}}
	if got := CardActionToInbound("t1", deny); got == nil || got.Content != "拒绝" {
		t.Fatalf("deny action = %+v, want content 拒绝", got)
	}
	// Without an event id, the fallback key still separates approve from deny.
	noEvent := func(decision string) *channels.InboundMessage {
		return CardActionToInbound("t1", &cardActionEnvelope{CardAction: &cardActionPayload{
			MessageID: "om_card", Value: map[string]string{"session_id": "s", "decision": decision},
		}})
	}
	if noEvent("approve").PlatformMsgID == noEvent("deny").PlatformMsgID {
		t.Error("fallback dedup key must include the decision")
	}
}

// TestCardActionIgnoresUselessPayloads keeps the decoder from inventing turns.
func TestCardActionIgnoresUselessPayloads(t *testing.T) {
	cases := map[string]*cardActionEnvelope{
		"nil envelope":     nil,
		"no payload":       {},
		"no session":       {CardAction: &cardActionPayload{Value: map[string]string{"decision": "approve"}}},
		"unknown decision": {CardAction: &cardActionPayload{Value: map[string]string{"session_id": "s", "decision": "maybe"}}},
	}
	for name, env := range cases {
		if got := CardActionToInbound("t1", env); got != nil {
			t.Errorf("%s: got %+v, want nil", name, got)
		}
	}
}
