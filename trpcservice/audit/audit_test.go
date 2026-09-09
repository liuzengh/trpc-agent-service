package audit

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoggerWritesJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	l.Log(Record{Event: EventInbound, TenantID: "demo", Channel: "webchat", Decision: DecisionAllow, TraceID: "abc123"})
	l.Log(Record{Event: EventGuardrailBlock, TenantID: "demo", Stage: "input", Rule: "keyword", Decision: DecisionBlock})
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var recs []Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		if r.Time.IsZero() {
			t.Fatalf("record without ts: %+v", r)
		}
		recs = append(recs, r)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].TraceID != "abc123" || recs[0].Event != EventInbound {
		t.Fatalf("first record = %+v", recs[0])
	}
	if recs[1].Stage != "input" || recs[1].Decision != DecisionBlock {
		t.Fatalf("second record = %+v", recs[1])
	}
}

func TestNilLoggerIsNoop(t *testing.T) {
	var l *Logger
	l.Log(Record{Event: EventReply}) // must not panic
	if err := l.Close(); err != nil {
		t.Fatalf("nil close: %v", err)
	}
	if _, err := New(""); err != nil {
		t.Fatalf("empty path must yield nil logger: %v", err)
	}
}

func TestLoggerAppendMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	for i := 0; i < 2; i++ {
		l, err := New(path)
		if err != nil {
			t.Fatal(err)
		}
		l.Log(Record{Event: EventAdmin, Detail: "boot"})
		l.Close()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n := countLines(data); n != 2 {
		t.Fatalf("reopen must append, got %d lines", n)
	}
}

// countLines counts JSONL lines in the raw file content.
func countLines(b []byte) int {
	n := 0
	for _, c := range b {
		if c == '\n' {
			n++
		}
	}
	return n
}
