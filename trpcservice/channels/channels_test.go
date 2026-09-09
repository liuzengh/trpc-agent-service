package channels

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestRenderPlain(t *testing.T) {
	in := "## 标题\n**加粗** 和 `代码`，见 [文档](https://example.com)。\n- 列表项"
	want := "标题\n加粗 和 代码，见 文档 (https://example.com)。\n- 列表项"
	if got := RenderPlain(in); got != want {
		t.Fatalf("RenderPlain:\n got %q\nwant %q", got, want)
	}
	// Plain text passes through untouched.
	if got := RenderPlain("纯文本，没有标记"); got != "纯文本，没有标记" {
		t.Fatalf("plain text must pass through: %q", got)
	}
	// A '[' that never forms [text](url) survives verbatim.
	if got := RenderPlain("见 [未闭合 和 ](畸形"); got != "见 [未闭合 和 ](畸形" {
		t.Fatalf("unclosed bracket must survive: %q", got)
	}
}

func TestLooksMarkdown(t *testing.T) {
	if !LooksMarkdown("**加粗**") || !LooksMarkdown("## 标题") || !LooksMarkdown("[a](b)") {
		t.Fatal("markdown markers must be detected")
	}
	if LooksMarkdown("纯文本") {
		t.Fatal("plain text must not be detected as markdown")
	}
}

func TestScrubError(t *testing.T) {
	// A url.Error from net/http embeds the full URL incl. query secrets.
	raw := &url.Error{Op: "Get", URL: "https://api.example.com/cgi-bin/gettoken?corpid=ww1&corpsecret=SUPERSECRET", Err: errors.New("dial refused")}
	scrubbed := ScrubError(raw)
	if strings.Contains(scrubbed.Error(), "SUPERSECRET") {
		t.Fatalf("corpsecret leaked in error: %v", scrubbed)
	}
	if !strings.Contains(scrubbed.Error(), "corpsecret=%2A%2A%2A") && !strings.Contains(scrubbed.Error(), "corpsecret=***") {
		t.Fatalf("scrubbed marker missing: %v", scrubbed)
	}
	// Non-url errors pass through.
	plain := errors.New("plain")
	if ScrubError(plain) != plain {
		t.Fatal("non-url error must pass through unchanged")
	}
}

// SplitText breaks long text into ≤n-byte segments without ever cutting
// inside a UTF-8 sequence.
func TestSplitText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		n    int
		want []string
	}{
		{"short", "hello", 10, []string{"hello"}},
		{"exact fit", "hello", 5, []string{"hello"}},
		{"ascii split", "hello", 2, []string{"he", "ll", "o"}},
		{"empty", "", 4, []string{""}},
		// Each Chinese rune is 3 bytes: n=3 yields one rune per segment, and the
		// segments reassemble losslessly.
		{"cjk", "中文中文", 3, []string{"中", "文", "中", "文"}},
		{"cjk multi-rune chunks", "中文中文", 6, []string{"中文", "中文"}},
		// 4-byte emoji with n on the boundary.
		{"emoji boundary", "🚀🚀", 4, []string{"🚀", "🚀"}},
		// The cut walks back to the rune boundary behind it.
		{"emoji cut walks back", "a🚀b", 4, []string{"a", "🚀", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SplitText(tc.in, tc.n)
			if len(got) != len(tc.want) {
				t.Fatalf("SplitText(%q, %d):\n got %#v\nwant %#v", tc.in, tc.n, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("SplitText(%q, %d):\n got %#v\nwant %#v", tc.in, tc.n, got, tc.want)
				}
			}
			// The segments must reassemble the input losslessly.
			var joined strings.Builder
			for _, s := range got {
				joined.WriteString(s)
			}
			if joined.String() != tc.in {
				t.Fatalf("segments do not reassemble: %q != %q", joined.String(), tc.in)
			}
		})
	}
}

// SessionKey: direct chats key on the user, group chats key on the chat.
func TestSessionKey(t *testing.T) {
	cases := []struct {
		channel, user, chat, want string
	}{
		{"wecom", "u1", "", "dm:wecom:u1"},
		{"mock", "external-42", "", "dm:mock:external-42"},
		{"wecom", "u1", "chat-9", "group:wecom:chat-9"},
		{"wxkf", "", "chat-9", "group:wxkf:chat-9"}, // chatID wins over a missing user
	}
	for _, tc := range cases {
		if got := SessionKey(tc.channel, tc.user, tc.chat); got != tc.want {
			t.Fatalf("SessionKey(%q, %q, %q) = %q, want %q", tc.channel, tc.user, tc.chat, got, tc.want)
		}
	}
}

// HandlerFunc turns a plain function into a Handler.
func TestHandlerFunc(t *testing.T) {
	var got InboundMessage
	h := HandlerFunc(func(_ context.Context, msg InboundMessage) (OutboundMessage, error) {
		got = msg
		return OutboundMessage{Text: "pong", Channel: msg.Channel}, nil
	})
	out, err := h.Handle(context.Background(), InboundMessage{Channel: "mock", Text: "ping"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Text != "ping" || got.Channel != "mock" {
		t.Fatalf("handler did not receive the message: %+v", got)
	}
	if out.Text != "pong" {
		t.Fatalf("unexpected outbound: %+v", out)
	}

	_, err = HandlerFunc(func(context.Context, InboundMessage) (OutboundMessage, error) {
		return OutboundMessage{}, errors.New("nope")
	}).Handle(context.Background(), InboundMessage{})
	if err == nil {
		t.Fatal("the wrapped error must surface")
	}
}

func TestStaleCallback(t *testing.T) {
	now := time.Unix(1757000000, 0)
	cases := []struct {
		name   string
		create int64
		stale  bool
	}{
		{"fresh", now.Add(-time.Minute).Unix(), false},
		{"just inside the window", now.Add(-CallbackTimestampWindow).Unix(), false},
		{"five minutes and a second old", now.Add(-CallbackTimestampWindow - time.Second).Unix(), true},
		{"a day old", now.Add(-24 * time.Hour).Unix(), true},
		{"slightly in the future", now.Add(time.Minute).Unix(), false},
		{"far in the future", now.Add(time.Hour).Unix(), true},
		{"no timestamp at all", 0, false},
	}
	for _, tc := range cases {
		if got := StaleCallback(tc.create, now); got != tc.stale {
			t.Errorf("%s: StaleCallback(%d) = %v, want %v", tc.name, tc.create, got, tc.stale)
		}
	}
}
