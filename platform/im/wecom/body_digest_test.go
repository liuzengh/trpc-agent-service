package wecom_test

import (
	"context"
	"github.com/coder/websocket"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	"testing"
)

func TestBodyDigestPreservesUnknownPayloadWithoutTransportIdentity(t *testing.T) {
	events := make(chan wecom.Event, 3)
	client := fixture(t, wecom.Config{}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		frames := []string{
			`{"cmd":"aibot_msg_callback","headers":{"req_id":"r1"},"body":{"msgid":"m1","aibotid":"test-bot","chattype":"single","from":{"userid":"u"},"msgtype":"image","image":{"url":"first"}}}`,
			`{"headers":{"req_id":"r2"},"cmd":"aibot_msg_callback","body":{"image":{"url":"first"},"msgtype":"image","from":{"userid":"u"},"chattype":"single","aibotid":"test-bot","msgid":"m1"}}`,
			`{"cmd":"aibot_msg_callback","headers":{"req_id":"r3"},"body":{"msgid":"m1","aibotid":"test-bot","chattype":"single","from":{"userid":"u"},"msgtype":"image","image":{"url":"different"}}}`,
		}
		for _, f := range frames {
			_ = conn.Write(ctx, websocket.MessageText, []byte(f))
		}
		_, _, _ = conn.Read(ctx)
	})
	runClient(client, collect(events))
	a, b, c := waitValue(t, events), waitValue(t, events), waitValue(t, events)
	if len(a.BodyDigest) != 64 || a.BodyDigest != b.BodyDigest || a.BodyDigest == c.BodyDigest {
		t.Fatalf("body fingerprint lost identity: %q %q %q", a.BodyDigest, b.BodyDigest, c.BodyDigest)
	}
	if a.Text != "" || b.Text != "" || c.Text != "" {
		t.Fatal("unsupported data became prompt")
	}
}
