package wecommcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRejectedMediaIdentitySurvivesRotatingRetrievalID(t *testing.T) {
	b := fixtureBinding()
	cfg, _ := ParseBinding(b)
	from := cfg.Start()
	store := NewMemoryStore()
	key := PollKey{b.TenantID, b.ID, endpointHash("group-1")}
	var previous string
	for _, token := range []string{"private-retrieval-A", "private-retrieval-B"} {
		media := map[string]any{"userid": "human-1", "user_name": token, "send_time": "2026-09-06 00:00:20", "msg_type": "image",
			"image": map[string]any{"media_id": token, "url": "https://example.invalid/file?token=" + token}}
		raw, _ := json.Marshal(fixturePage([]any{media}, false, ""))
		batch, _, _, _, err := decodePageBatch(raw, b, cfg, "group-1", from, from.Add(time.Minute))
		if err != nil || len(batch.Messages) != 0 || len(batch.Rejected) != 1 {
			t.Fatal("media was not isolated")
		}
		r := batch.Rejected[0]
		if r.Reason != "unsupported_type" || !strings.HasPrefix(r.Fingerprint, "mcp_reject2_") {
			t.Fatal("missing stable media rejection identity")
		}
		fresh, err := store.RecordRejection(context.Background(), key, r)
		if err != nil || fresh != (previous == "") {
			t.Fatal("rotated credential created a second quarantine fact")
		}
		if previous != "" && previous != r.Fingerprint {
			t.Fatal("media fingerprint depends on retrieval credential")
		}
		previous = r.Fingerprint
		encoded, _ := json.Marshal(r)
		if strings.Contains(string(encoded), "private-retrieval") || strings.Contains(string(encoded), "example.invalid") {
			t.Fatal("quarantine leaked media retrieval data")
		}
	}
	rows, err := store.ListRejections(context.Background(), b.TenantID, b.ID, 100)
	if err != nil || len(rows) != 1 {
		t.Fatal("repeated media read should have one quarantine aggregate")
	}
}

func TestRejectedMediaIdentitySeparatesEnvelopeAndScope(t *testing.T) {
	seen := map[string]bool{}
	for _, change := range []string{"baseline", "sender", "second", "type", "tenant", "binding", "chat"} {
		b := fixtureBinding()
		cfg, _ := ParseBinding(b)
		chat := "group-1"
		m := map[string]any{"userid": "human-1", "send_time": "2026-09-06 00:00:20", "msg_type": "image", "image": map[string]string{"media_id": "rotating"}}
		switch change {
		case "sender":
			m["userid"] = "human-2"
		case "second":
			m["send_time"] = "2026-09-06 00:00:21"
		case "type":
			m["msg_type"] = "file"
		case "tenant":
			b.TenantID = "another-tenant"
		case "binding":
			b.ID = "another-binding"
		case "chat":
			chat = "another-group"
		}
		raw, _ := json.Marshal(fixturePage([]any{m}, false, ""))
		batch, _, _, _, err := decodePageBatch(raw, b, cfg, chat, cfg.Start(), cfg.Start().Add(time.Minute))
		if err != nil || len(batch.Rejected) != 1 {
			t.Fatalf("case %s not rejected", change)
		}
		id := batch.Rejected[0].Fingerprint
		if seen[id] {
			t.Fatalf("case %s crossed an envelope/scope boundary", change)
		}
		seen[id] = true
	}
}
