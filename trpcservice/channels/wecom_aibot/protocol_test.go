package wecom_aibot

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestFrameCodecRejectsMalformedAndUnknown(t *testing.T) {
	if _, err := decodeFrame(nil); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("nil frame error = %v", err)
	}
	if _, err := decodeFrame([]byte(`{"headers":{"req_id":"x"},"unknown":1}`)); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("unknown field accepted: %v", err)
	}
	if _, err := decodeFrame([]byte(`{"headers":{}}`)); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("missing req id accepted: %v", err)
	}
}

func TestFrameCodecRoundTrip(t *testing.T) {
	body, err := json.Marshal(Message{MsgID: "m1", MsgType: "text"})
	if err != nil {
		t.Fatal(err)
	}
	want := Frame{Cmd: cmdCallback, Headers: Headers{ReqID: "req-1"}, Body: body}
	encoded, err := encodeFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cmd != want.Cmd || got.Headers.ReqID != want.Headers.ReqID {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}
