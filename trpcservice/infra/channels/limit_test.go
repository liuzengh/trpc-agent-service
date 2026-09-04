package channels

import (
	"context"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestMaxTextLenPerChannel(t *testing.T) {
	cases := map[string]int{"wecom": 2000, "feishu": 4000, "telegram": 4000, "": 4000}
	for ch, want := range cases {
		if got := maxTextLen(ch); got != want {
			t.Errorf("maxTextLen(%q) = %d, want %d", ch, got, want)
		}
	}
}

func TestTruncateTextShortUnchanged(t *testing.T) {
	s := "你好 world"
	if got := truncateText(s, 100); got != s {
		t.Errorf("short text truncated: %q", got)
	}
}

func TestTruncateTextRuneSafeAndSuffix(t *testing.T) {
	// 5000 CJK runes exceed the wecom ceiling; truncation must not split a
	// multi-byte rune and must append an ellipsis.
	long := strings.Repeat("你", 5000)
	got := truncateText(long, maxTextLen("wecom"))
	if !utf8.ValidString(got) {
		t.Fatal("truncated text is not valid UTF-8")
	}
	n := utf8.RuneCountInString(got)
	if n > maxTextLen("wecom") {
		t.Errorf("truncated length %d exceeds ceiling", n)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncated text lacks ellipsis suffix: …%q", got[len(got)-12:])
	}
	if got == long {
		t.Error("long text was not truncated")
	}
}

// staticLimiter lets a test decide allow/deny and error behaviour.
type staticLimiter struct {
	allow bool
	err   error
}

func (s *staticLimiter) Allow(context.Context, string) (bool, error) {
	return s.allow, s.err
}

// TestGatewayRateLimitDropsDeniedInbound verifies the limiter gate sits before
// bus.PublishInbound: a denied message never enters the pipeline, and the
// pump stays alive for the next allowed message.
func TestGatewayRateLimitDropsDeniedInbound(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	g := NewGateway(b, nil)
	g.SetRateLimiter(&staticLimiter{allow: false})
	a := &fakeAdapter{name: "wecom", inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)}

	g.Attach(ctx, a, Attach{AgentID: "agent-1"})
	a.inbound <- &InboundMessage{
		PlatformMsgID: "drop-1", TenantID: "t1", SessionID: "s1",
		UserID: "u1", ChatType: ChatTypeSingle, ChatID: "c", Content: "spam",
	}
	select {
	case m := <-b.published:
		t.Fatalf("rate-limited message was published: %+v", m)
	case <-time.After(300 * time.Millisecond):
		// expected: denied message dropped
	}

	// The same pump must keep working once the limiter allows.
	g.SetRateLimiter(&staticLimiter{allow: true})
	a.inbound <- &InboundMessage{
		PlatformMsgID: "ok-2", TenantID: "t1", SessionID: "s1",
		UserID: "u1", ChatType: ChatTypeSingle, ChatID: "c", Content: "hi",
	}
	select {
	case m := <-b.published:
		if m.ID != "wecom:ok-2" {
			t.Errorf("published = %+v, want wecom:ok-2", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("allowed message never published after limiter flip")
	}
}

// TestGatewayTruncatesOverLimitOutbound verifies the text ceiling is applied
// on the reply path before the adapter sees the message.
func TestGatewayTruncatesOverLimitOutbound(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	g := NewGateway(b, nil)
	a := &fakeAdapter{name: "wecom", inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)}
	g.Attach(ctx, a, Attach{AgentID: "agent-1"})

	a.inbound <- &InboundMessage{
		PlatformMsgID: "p1", TenantID: "t1", SessionID: "s1",
		UserID: "u1", ChatType: ChatTypeSingle, ChatID: "chat-9", Content: "hi",
	}
	select {
	case <-b.published:
	case <-time.After(2 * time.Second):
		t.Fatal("inbound not published")
	}

	reply := model.NewAssistantMessage(strings.Repeat("长", 3000)) // > 2000
	b.outbound = []*bus.Message{{
		ID: "r1", TenantID: "t1", SessionID: "s1", Channel: "wecom", Content: &reply,
	}}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = g.Run(runCtx) }()

	select {
	case sent := <-a.sent:
		n := utf8.RuneCountInString(sent.Text())
		if n > wecomTextMax {
			t.Errorf("adapter received %d runes, ceiling %d", n, wecomTextMax)
		}
		if !strings.HasSuffix(sent.Text(), "…") {
			t.Error("truncated reply lacks ellipsis")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("outbound reply never dispatched")
	}
}
