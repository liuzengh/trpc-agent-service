package application

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	wire "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func TestFinalIntentEncodingKeepsLegacyWireAndUsesExistingEnvelopeBoundary(t *testing.T) {
	r := domain.Run{Request: domain.Requested{RunID: "run", AdmissionID: "admission"}, ReplyDeadline: time.Date(2026, 9, 9, 1, 2, 3, 456, time.UTC)}
	legacy := wire.ReplyIntent{SchemaVersion: 1, IntentID: domain.StableID("fin", "run"), AdmissionID: "admission", RunID: "run", Kind: "final", Sequence: 1, Deadline: r.ReplyDeadline.Format(time.RFC3339Nano), Content: wire.FinalTextContent{Type: "text", Text: "Final\n逐字"}, Execution: wire.ReplyExecution{AttemptID: "attempt", CompletionID: domain.StableID("cmp", "run"), Generation: 2}}
	want, e := codec.EncodeReplyIntent(legacy)
	if e != nil {
		t.Fatal(e)
	}
	wantDigest, e := codec.ReplyIntentDigest(legacy)
	if e != nil {
		t.Fatal(e)
	}
	for _, items := range [][]domain.Attachment{nil, {}} {
		got, digest, e := EncodeFinalIntent(r, "attempt", 2, legacy.Content.Text, items)
		if e != nil || !bytes.Equal(got, want) || digest != wantDigest {
			t.Fatal("legacy Final changed", e)
		}
	}
	items := make([]domain.Attachment, 5000)
	for i := range items {
		items[i] = domain.Attachment{Name: fmt.Sprintf("file-%04d-%s.txt", i, strings.Repeat("x", 100)), Version: 0, MimeType: "text/plain", SizeBytes: 1, SHA256: strings.Repeat("a", 64)}
	}
	// There is deliberately no new private count limit: 512 small references fit.
	if _, _, e = EncodeFinalIntent(r, "attempt", 2, "Final", items[:512]); e != nil {
		t.Fatal("introduced attachment count cap", e)
	}
	if e = domain.ValidateAttachments(items); e != nil {
		t.Fatal("metadata invalid before envelope boundary", e)
	}
	if _, _, e = EncodeFinalIntent(r, "attempt", 2, "Final", items); !errors.Is(e, codec.ErrInvalidReplyIntent) {
		t.Fatal("oversized wire envelope accepted", e)
	}
}
