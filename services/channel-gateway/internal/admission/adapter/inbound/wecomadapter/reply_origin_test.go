package wecomadapter_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	adapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/wecomadapter"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

func TestReplyOriginPreservesSocketAndNeverEntersWire(t *testing.T) {
	dst := &capture{}
	h, err := adapter.NewHandler("account-1", "bot-1", fence(), dst)
	if err != nil {
		t.Fatal(err)
	}
	e := event()
	e.Generation = 17
	if err = h.Handle(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	in := dst.inputs[0]
	want := domain.ReplyOrigin{InstanceID: "instance-1", Epoch: 7, Revision: 3, SocketGeneration: 17}
	if in.ReplyOrigin == nil || *in.ReplyOrigin != want {
		t.Fatalf("origin=%+v", in.ReplyOrigin)
	}
	raw, _ := json.Marshal(in)
	for _, word := range []string{"reply_origin", "socket_generation", "instance-1"} {
		if strings.Contains(string(raw), word) {
			t.Fatal("origin entered input wire")
		}
	}
	bad := in
	copied := *in.ReplyOrigin
	copied.Epoch++
	bad.ReplyOrigin = &copied
	if bad.Validate() == nil {
		t.Fatal("origin and authorization fence mismatch accepted")
	}
	bad = in
	copied = *in.ReplyOrigin
	copied.SocketGeneration = 0
	bad.ReplyOrigin = &copied
	if bad.Validate() == nil {
		t.Fatal("zero socket accepted")
	}
	old := in
	old.ReplyOrigin = nil
	if old.Validate() != nil {
		t.Fatal("legacy unknown origin rejected")
	}
}
