package executionv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
)

func fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("fixtures/valid/telegram-text.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func eventFixture(t *testing.T) dto.RunRequested {
	t.Helper()
	event, err := DecodeRunRequested(fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestRunRequestedFixtures(t *testing.T) {
	for _, kind := range []string{"valid", "invalid"} {
		files, err := filepath.Glob("fixtures/" + kind + "/*.json")
		if err != nil || len(files) == 0 {
			t.Fatalf("missing %s fixtures: %v", kind, err)
		}
		for _, name := range files {
			t.Run(filepath.Base(name), func(t *testing.T) {
				raw, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				event, err := DecodeRunRequested(raw)
				if kind == "invalid" {
					if !errors.Is(err, ErrInvalidRunRequested) {
						t.Fatalf("invalid fixture returned %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := EncodeRunRequested(event)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := DecodeRunRequested(encoded)
				if err != nil || !reflect.DeepEqual(event, decoded) {
					t.Fatalf("lossy round trip: %v", err)
				}
				repeated, err := EncodeRunRequested(decoded)
				if err != nil || !bytes.Equal(encoded, repeated) {
					t.Fatal("encoding is not repeatable")
				}
			})
		}
	}
}

func TestSchemaIntegerNormalizationIsLossless(t *testing.T) {
	raw := fixture(t)
	raw = bytes.Replace(raw, []byte(`"schema_version": 1`), []byte(`"schema_version": 1e0`), 1)
	raw = bytes.Replace(raw, []byte(`"generation": 1`), []byte(`"generation": 1.0`), 1)
	event, err := DecodeRunRequested(raw)
	if err != nil || event.SchemaVersion != 1 || event.Route.Generation != 1 {
		t.Fatalf("valid schema integers: %#v %v", event, err)
	}
	for _, number := range []string{"9007199254740992", "9007199254740993", "1.00000000000000001", "1e1000"} {
		candidate := bytes.Replace(fixture(t), []byte(`"generation": 1`), []byte(`"generation": `+number), 1)
		if _, err := DecodeRunRequested(candidate); !errors.Is(err, ErrInvalidRunRequested) {
			t.Errorf("accepted nonexact or unsafe integer %s", number)
		}
	}
}

func TestProviderIDsStayStringsAboveFloatPrecision(t *testing.T) {
	event := eventFixture(t)
	event.Input.SenderID = "9007199254740993"
	raw, err := EncodeRunRequested(event)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := DecodeRunRequested(raw)
	if err != nil || actual.Input.SenderID != event.Input.SenderID {
		t.Fatalf("identifier rounded: %v", err)
	}
}

func TestEncodeRejectsInvalidValues(t *testing.T) {
	cases := map[string]func(*dto.RunRequested){
		"kind":           func(e *dto.RunRequested) { e.Input.Kind = "ignore" },
		"route identity": func(e *dto.RunRequested) { e.Route.AccountID = "another-account" },
		"nil reply":      func(e *dto.RunRequested) { e.Input.ReplyContext = nil },
		"wrong chat":     func(e *dto.RunRequested) { e.Input.ReplyContext.ChatID = "43" },
		"missing sender": func(e *dto.RunRequested) { e.Input.SenderID = "" },
		"zero time":      func(e *dto.RunRequested) { e.Input.ReceivedAt = "0001-01-01T00:00:00Z" },
		"UTF8":           func(e *dto.RunRequested) { e.Input.Text = string([]byte{0xff}) },
		"UTF8 nested":    func(e *dto.RunRequested) { e.Input.ReplyContext.ChatID = string([]byte{0xff}) },
		"NUL":            func(e *dto.RunRequested) { e.Input.Text = "a\x00b" },
		"text bytes":     func(e *dto.RunRequested) { e.Input.Text = strings.Repeat("😀", 16385) },
		"leading zeros":  func(e *dto.RunRequested) { e.Input.Key.EventID = "001" },
		"zero chat":      func(e *dto.RunRequested) { e.Input.ConversationID = "0"; e.Input.ReplyContext.ChatID = "0" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			e := eventFixture(t)
			change(&e)
			if _, err := EncodeRunRequested(e); !errors.Is(err, ErrInvalidRunRequested) {
				t.Fatalf("accepted invalid event: %v", err)
			}
		})
	}
	e := eventFixture(t)
	e.Input.Text = strings.Repeat("😀", 16384)
	if _, err := EncodeRunRequested(e); err != nil {
		t.Fatalf("byte boundary rejected: %v", err)
	}
}

func TestRejectMalformedJSONWithoutRepair(t *testing.T) {
	raw, err := EncodeRunRequested(eventFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	var asMap map[string]any
	if err = json.Unmarshal(raw, &asMap); err != nil {
		t.Fatal(err)
	}
	for name, candidate := range map[string][]byte{
		"empty": nil, "oversize": bytes.Repeat([]byte(" "), MaxRunRequestedBytes+1), "null": []byte("null"), "array": []byte("[]"),
		"invalid UTF8":      bytes.Replace(raw, []byte("你好"), []byte{0xff}, 1),
		"high surrogate":    bytes.Replace(raw, []byte("你好"), []byte(`\ud800`), 1),
		"low surrogate":     bytes.Replace(raw, []byte("你好"), []byte(`\udc00`), 1),
		"wrong pair":        bytes.Replace(raw, []byte("你好"), []byte(`\ud800\ud800`), 1),
		"escaped duplicate": bytes.Replace(raw, []byte(`"schema_version":1`), []byte(`"schema_version":1,"schema_versio\u006e":1`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRunRequested(candidate); !errors.Is(err, ErrInvalidRunRequested) {
				t.Fatalf("accepted malformed JSON: %v", err)
			}
		})
	}
	for _, escaped := range []string{`\ud83d\ude00`, `\\ud800`} {
		candidate := bytes.Replace(raw, []byte("你好"), []byte(escaped), 1)
		if _, err := DecodeRunRequested(candidate); err != nil {
			t.Fatalf("valid escape rejected %s: %v", escaped, err)
		}
	}
}

func TestReplyContextNarrowMapping(t *testing.T) {
	reply, err := DecodeReplyContext("telegram", []byte(`{"chat_id":"42","source_message_id":"99"}`))
	if err != nil || reply.ChatID != "42" || reply.SourceMessageID != "99" {
		t.Fatalf("mapping: %#v %v", reply, err)
	}
	for _, raw := range []string{`null`, `{}`, `{"chat_id":"42","callback_query_id":"callback"}`, `{"chat_id":"42","chat_id":"42"}`, `{"chat_id":"42","token":"secret"}`, `{"chat_id":42}`, `{"chat_id":"42","message_thread_id":null}`, `{"Chat_ID":"42"}`} {
		if _, err := DecodeReplyContext("telegram", []byte(raw)); !errors.Is(err, ErrInvalidRunRequested) {
			t.Fatalf("accepted arbitrary context: %s", raw)
		}
	}
	if _, err := DecodeReplyContext("unknown", []byte(`{"chat_id":"42"}`)); !errors.Is(err, ErrInvalidRunRequested) {
		t.Fatal("unknown provider accepted")
	}
}

func TestReplyContextUnicodeLengthUsesSchemaBoundary(t *testing.T) {
	for _, size := range []int{256, 257} {
		value := strings.Repeat("界", size)
		raw, err := json.Marshal(map[string]string{
			"chat_type": "group", "chatid_or_userid": value,
			"received_at": "2026-09-05T01:02:03Z",
		})
		if err != nil {
			t.Fatal(err)
		}
		reply, err := DecodeReplyContext("wecom", raw)
		if size == 256 {
			if err != nil || reply.ChatIDOrUserID != value {
				t.Fatalf("schema-valid Unicode identifier rejected: %v", err)
			}
		} else if !errors.Is(err, ErrInvalidRunRequested) {
			t.Fatal("overlong Unicode identifier accepted")
		}
	}
}

func TestConcurrentCodec(t *testing.T) {
	raw := fixture(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				e, err := DecodeRunRequested(raw)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err = EncodeRunRequested(e); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestGeneratedDTOsMatchSchema(t *testing.T) {
	command := exec.Command("go", "run", "./generate", "-check")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generator drift: %v\n%s", err, output)
	}
}

func TestSchemaCompiles(t *testing.T) {
	if _, err := compiledSchema(); err != nil {
		t.Fatal(err)
	}
}
