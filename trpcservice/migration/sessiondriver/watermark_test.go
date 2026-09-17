package sessiondriver

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestSessionWatermarkCanonicalRoundTrip(t *testing.T) {
	encoded, err := EncodeSessionWatermark("app-a", "session-a", false)
	if err != nil {
		t.Fatal(err)
	}
	appID, sessionID, empty, err := DecodeSessionWatermark(encoded)
	if err != nil || appID != "app-a" || sessionID != "session-a" || empty {
		t.Fatalf("decoded=%q/%q empty=%t err=%v", appID, sessionID, empty, err)
	}
	if _, _, _, err := DecodeSessionWatermark("cursor:untrusted"); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("untrusted watermark=%v", err)
	}
}
