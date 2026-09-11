package domain_test

import (
	"testing"
	"time"

	contract "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

func wire(in domain.Intent) dto.ReplyIntent {
	return dto.ReplyIntent{SchemaVersion: 1, IntentID: in.ID, AdmissionID: in.AdmissionID, RunID: in.RunID, Execution: dto.ReplyExecution{AttemptID: in.AttemptID, Generation: in.ExecutionGeneration, CompletionID: in.CompletionID}, Sequence: in.Sequence, Kind: "final", Content: dto.FinalTextContent{Type: "text", Text: in.Text}, Deadline: in.Deadline.UTC().Format(time.RFC3339Nano)}
}
func TestIntentDigestMatchesWireCanonicalDocument(t *testing.T) {
	for _, text := range []string{"hello", "你好😀<&>", "a\nb\t\u2028c", "e\u0301", "\\ud800"} {
		in := intent()
		in.Text = text
		in.Deadline = in.Deadline.In(time.FixedZone("east", 8*3600))
		domainDigest, err := domain.IntentDigest(in)
		if err != nil {
			t.Fatal(err)
		}
		wireDigest, err := contract.ReplyIntentDigest(wire(in))
		if err != nil || domainDigest != wireDigest {
			t.Fatalf("digest divergence: %s %s %v", domainDigest, wireDigest, err)
		}
	}
	in := intent()
	digest, err := domain.IntentDigest(in)
	if err != nil || digest != "sha256:612e2d7727311ac0ac71166173a28a1780534a31a653280e5307aed0390ac483" {
		t.Fatalf("literal contract digest=%s err=%v", digest, err)
	}
	mutations := map[string]func(*domain.Intent){"intent": func(i *domain.Intent) { i.ID += "2" }, "admission": func(i *domain.Intent) { i.AdmissionID += "2" }, "run": func(i *domain.Intent) { i.RunID += "2" }, "attempt": func(i *domain.Intent) { i.AttemptID += "2" }, "completion": func(i *domain.Intent) { i.CompletionID += "2" }, "generation": func(i *domain.Intent) { i.ExecutionGeneration++ }, "sequence": func(i *domain.Intent) { i.Sequence++ }, "text": func(i *domain.Intent) { i.Text += "2" }, "deadline": func(i *domain.Intent) { i.Deadline = i.Deadline.Add(time.Nanosecond) }}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := in
			mutate(&changed)
			d, err := domain.IntentDigest(changed)
			if err != nil || d == digest {
				t.Fatalf("unbound field: %v", err)
			}
			w, err := contract.ReplyIntentDigest(wire(changed))
			if err != nil || d != w {
				t.Fatalf("wire divergence: %v", err)
			}
		})
	}
}
