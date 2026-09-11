package controleventsv1

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestManifestEventCodec(t *testing.T) {
	raw, err := os.ReadFile("examples/valid/runtime-manifest-published.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeRuntimeManifestPublishedEvent(raw); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		[]byte(strings.Replace(string(raw), `"event_id":`, `"event_id":"duplicate","event_id":`, 1)),
		[]byte(strings.Replace(string(raw), `"tenant_id": "tnt_fixture_tenant"`, `"tenant_id": "wrong_tenant"`, 1)),
		append(raw, []byte("{}")...),
	} {
		if _, err = DecodeRuntimeManifestPublishedEvent(bad); !errors.Is(err, ErrInvalidManifestEvent) {
			t.Fatalf("bad event accepted: %v", err)
		}
	}
	var e RuntimeManifestPublishedEvent
	json.Unmarshal(raw, &e)
	e.Manifest.ContentDigest = "sha256:" + strings.Repeat("0", 64)
	bad, _ := json.Marshal(e)
	if _, err = DecodeRuntimeManifestPublishedEvent(bad); !errors.Is(err, ErrInvalidManifestEvent) {
		t.Fatalf("bad digest accepted %v", err)
	}
	digest, err := ManifestEventDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	json.Unmarshal(raw, &v)
	compact, _ := json.Marshal(v)
	same, err := ManifestEventDigest(compact)
	if err != nil || digest != same {
		t.Fatalf("canonical digest differs")
	}
}
