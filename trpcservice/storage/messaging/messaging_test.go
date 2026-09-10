package messaging_test

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestNormalizeContentTypeIsFailClosed(t *testing.T) {
	for _, test := range []struct {
		input, want string
	}{
		{"", messaging.ContentTypeText},
		{" TEXT/PLAIN ", messaging.ContentTypeText},
		{messaging.ContentTypeCard, messaging.ContentTypeCard},
		{"image/png", "image/png"},
	} {
		got, err := messaging.NormalizeContentType(test.input)
		if err != nil || got != test.want {
			t.Fatalf("input=%q got=%q err=%v", test.input, got, err)
		}
	}
	if _, err := messaging.NormalizeContentType("application/json"); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("unsupported type error=%v", err)
	}
}

func TestStableReplyIDUsesOnlyLogicalCoordinate(t *testing.T) {
	in := messaging.ReplyCoordinate{TenantID: "tenant", RequestID: "request", InputSeq: 2, Stage: "terminal", Ordinal: 0}
	first, err := messaging.StableReplyID(in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := messaging.StableReplyID(in)
	if err != nil || first != second || len(first) < 4 || first[:3] != "r1_" {
		t.Fatalf("first=%q second=%q err=%v", first, second, err)
	}
	in.Ordinal++
	third, err := messaging.StableReplyID(in)
	if err != nil || third == first {
		t.Fatalf("different coordinate id=%q err=%v", third, err)
	}
}
