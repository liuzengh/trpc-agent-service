package executionv1_test

import (
	"bytes"
	"errors"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	contract "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
)

func TestReplyIntentCanonicalRoundTripAndStableDigest(t *testing.T) {
	raw, err := os.ReadFile("fixtures/reply-intent-final.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	event, err := contract.DecodeReplyIntent(raw)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := contract.EncodeReplyIntent(event)
	if err != nil {
		t.Fatal(err)
	}
	again, err := contract.DecodeReplyIntent(canonical)
	if err != nil {
		t.Fatal(err)
	}
	recoded, err := contract.EncodeReplyIntent(again)
	if err != nil || !bytes.Equal(canonical, recoded) {
		t.Fatalf("unstable canonical JSON err=%v", err)
	}
	digest, err := contract.ReplyIntentDigest(event)
	if err != nil || digest != "sha256:612e2d7727311ac0ac71166173a28a1780534a31a653280e5307aed0390ac483" {
		t.Fatalf("digest=%s err=%v", digest, err)
	}
}

func TestReplyIntentRejectsNoncanonicalTime(t *testing.T) {
	raw, err := os.ReadFile("fixtures/reply-intent-final.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"0000-01-01T00:00:00Z", "0001-01-01T00:00:00Z", "2026-09-05T09:30:00+08:00", "2026-09-05T01:30:00.000Z", "2026-09-05T01:30:60Z"} {
		candidate := bytes.Replace(raw, []byte("2026-09-05T01:30:00Z"), []byte(value), 1)
		if _, err = contract.DecodeReplyIntent(candidate); err == nil {
			t.Errorf("accepted invalid/noncanonical deadline %q", value)
		}
	}
}

func TestReplyIntentInvalidFixtures(t *testing.T) {
	files, err := filepath.Glob("fixtures/reply-intent/invalid/*.json")
	if err != nil || len(files) != 8 {
		t.Fatalf("invalid fixtures: %v %v", files, err)
	}
	for _, name := range files {
		t.Run(filepath.Base(name), func(t *testing.T) {
			raw, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = contract.DecodeReplyIntent(raw); !errors.Is(err, contract.ErrInvalidReplyIntent) {
				t.Fatalf("accepted invalid fixture: %v", err)
			}
		})
	}
}

func TestReplyIntentRejectsMalformedJSONAndLossyNumbers(t *testing.T) {
	raw, err := os.ReadFile("fixtures/reply-intent-final.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty": nil, "null": []byte("null"), "array": []byte("[]"), "over-budget": bytes.Repeat([]byte(" "), contract.MaxReplyIntentBytes+1),
		"trailing":          append(bytes.Clone(raw), []byte("{}")...),
		"duplicate":         bytes.Replace(raw, []byte(`"intent_id": "intent-1"`), []byte(`"intent_id":"intent-1","intent_id":"intent-1"`), 1),
		"escaped-duplicate": bytes.Replace(raw, []byte(`"intent_id": "intent-1"`), []byte(`"intent_id":"intent-1","intent_i\u0064":"intent-1"`), 1),
		"invalid-utf8":      bytes.Replace(raw, []byte("hello"), []byte{0xff}, 1),
		"high-surrogate":    bytes.Replace(raw, []byte("hello"), []byte(`\ud800`), 1),
		"low-surrogate":     bytes.Replace(raw, []byte("hello"), []byte(`\udc00`), 1),
		"wrong-surrogate":   bytes.Replace(raw, []byte("hello"), []byte(`\ud800\ud800`), 1),
	}
	for _, number := range []string{"0", "-1", "1.00000000000000001", "9007199254740992", "9007199254740993", "1e1000", "1e-1000"} {
		cases[number] = bytes.Replace(raw, []byte(`"generation": 1`), []byte(`"generation": `+number), 1)
	}
	for name, candidate := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := contract.DecodeReplyIntent(candidate); !errors.Is(err, contract.ErrInvalidReplyIntent) {
				t.Fatalf("accepted malformed JSON: %v", err)
			}
		})
	}
	for _, value := range []string{`\ud83d\ude00`, `\\ud800`} {
		if _, err := contract.DecodeReplyIntent(bytes.Replace(raw, []byte("hello"), []byte(value), 1)); err != nil {
			t.Fatalf("valid escaped Unicode rejected: %v", err)
		}
	}
	for _, number := range []string{"1e0", "1.0", "9007199254740991"} {
		if _, err := contract.DecodeReplyIntent(bytes.Replace(raw, []byte(`"generation": 1`), []byte(`"generation": `+number), 1)); err != nil {
			t.Fatalf("exact integer rejected: %v", err)
		}
	}
}

func TestReplyIntentEncoderStrictBounds(t *testing.T) {
	raw, err := os.ReadFile("fixtures/reply-intent-final.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	seed, err := contract.DecodeReplyIntent(raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*dto.ReplyIntent){
		"kind": func(e *dto.ReplyIntent) { e.Kind = "progress" }, "content-type": func(e *dto.ReplyIntent) { e.Content.Type = "markdown" },
		"missing-completion": func(e *dto.ReplyIntent) { e.Execution.CompletionID = "" }, "invalid-id": func(e *dto.ReplyIntent) { e.IntentID = "a b" },
		"unsafe-sequence": func(e *dto.ReplyIntent) { e.Sequence = 9007199254740992 }, "empty-text": func(e *dto.ReplyIntent) { e.Content.Text = "" },
		"whitespace-text": func(e *dto.ReplyIntent) { e.Content.Text = " \t\n" }, "NUL": func(e *dto.ReplyIntent) { e.Content.Text = "a\x00b" },
		"UTF8": func(e *dto.ReplyIntent) { e.Content.Text = string([]byte{0xff}) }, "text-byte-budget": func(e *dto.ReplyIntent) { e.Content.Text = strings.Repeat("😀", 16385) },
	} {
		t.Run(name, func(t *testing.T) {
			event := seed
			mutate(&event)
			if _, err := contract.EncodeReplyIntent(event); !errors.Is(err, contract.ErrInvalidReplyIntent) {
				t.Fatalf("invalid encode: %v", err)
			}
		})
	}
	seed.Content.Text = strings.Repeat("😀", 16384)
	if _, err := contract.EncodeReplyIntent(seed); err != nil {
		t.Fatalf("exact byte boundary: %v", err)
	}
}

func TestGeneratedReplyIntentDTOIsCurrent(t *testing.T) {
	cmd := exec.Command("go", "run", "./generate", "-schema", "reply-intent.schema.json", "-out", "../../../../gen/events/execution/v1/reply_intent.go", "-check")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generator: %v\n%s", err, out)
	}
}

func TestReplyIntentConcurrentCodec(t *testing.T) {
	raw, err := os.ReadFile("fixtures/reply-intent-final.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				event, err := contract.DecodeReplyIntent(raw)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err = contract.ReplyIntentDigest(event); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestReplyAttachmentFixtureZeroVersion(t *testing.T) {
	raw, err := os.ReadFile("fixtures/reply-intent-attachment.valid.json")
	if err != nil {
		t.Fatal(err)
	}
	in, err := contract.DecodeReplyIntent(raw)
	if err != nil || len(in.Content.Attachments) != 1 || in.Content.Attachments[0].Version != 0 {
		t.Fatal(in, err)
	}
	encoded, err := contract.EncodeReplyIntent(in)
	if err != nil || !bytes.Contains(encoded, []byte(`"version":0`)) {
		t.Fatal(string(encoded), err)
	}
}
