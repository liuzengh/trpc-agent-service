package audit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
}

// An empty path must still produce a trail, on stdout. That is the container
// shape: the filesystem is ephemeral and usually read-only, while the cluster's
// log pipeline is the durable sink, so a K8s manifest legitimately leaves
// audit.file out.
//
// This test exists because the old one did not catch the bug it was next to.
// New("") used to return a nil *Logger — a no-op — and TestNilLoggerIsNoop
// asserted only that err was nil, so "the whole governance trail silently
// disappears" passed as green. Asserting non-nil is not enough either: the
// point is that a record actually reaches the log, which is what the captured
// handler below checks.
func TestEmptyPathIsLogOnlyNotSilent(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	l, err := New("")
	if err != nil {
		t.Fatalf("new with empty path: %v", err)
	}
	if l == nil {
		t.Fatal("empty path must yield a log-only logger, not nil: a nil *Logger " +
			"is a no-op by design and would drop every record")
	}
	l.Log(Record{Event: EventInbound, TenantID: "demo", Channel: "webchat",
		Decision: DecisionAllow, TraceID: "abc123"})
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		`"msg":"audit"`,
		`"event":"inbound"`,
		`"tenant":"demo"`,
		`"trace_id":"abc123"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("log-only audit is missing %s; got %q", want, out)
		}
	}
}

// The file-backed logger must ALSO log: that is the documented "echoes them
// through the structured log" behaviour, and it is the half of Log that keeps
// working when the file write is skipped. Guarding it here is what stops the
// two states above from being merged again by a well-meaning simplification.
func TestFileBackedLoggerEchoesToTheLog(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	l.Log(Record{Event: EventAdmin, TenantID: "*", Decision: DecisionOK, Detail: "settings"})
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !strings.Contains(buf.String(), `"msg":"audit"`) {
		t.Fatalf("file-backed logger stopped echoing to slog; got %q", buf.String())
	}
	if n := countLines(mustRead(t, path)); n != 1 {
		t.Fatalf("file got %d lines, want 1", n)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
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

// TestRotationDefaultsBackupGeneration pins the documented default: rotation
// on with no MaxBackups keeps 3 generations, not "no backups".
func TestRotationDefaultsBackupGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := NewWithOptions(Options{Path: path, MaxSizeMB: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if l.maxBackups != 3 {
		t.Fatalf("maxBackups = %d, want 3 when rotation is on and no bound was given", l.maxBackups)
	}
}

// TestRotationRollsAtThresholdAndBoundsBackups drives the size-based rotation
// for real: the option is in whole megabytes, so the test writes whole
// megabytes, crosses the cap twice, and then holds the contract to three
// properties — the live file stays under the cap, the generations exist in
// order, and every record survives across the live file plus the backups
// (rotation must not lose a line).
func TestRotationRollsAtThresholdAndBoundsBackups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	l, err := NewWithOptions(Options{Path: path, MaxSizeMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatal(err)
	}

	// ~4.1KB per record: 640 records ≈ 2.6MB, crossing a 1MB cap twice.
	detail := strings.Repeat("x", 4096)
	const records = 640
	for i := 0; i < records; i++ {
		l.Log(Record{Event: EventInbound, Detail: detail})
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	live, err := os.Stat(path)
	if err != nil {
		t.Fatalf("live file: %v", err)
	}
	if live.Size() >= 1<<20+8192 {
		t.Fatalf("live file is %d bytes, want under the 1MB cap plus one record", live.Size())
	}
	for _, gen := range []string{path + ".1", path + ".2"} {
		if _, err := os.Stat(gen); err != nil {
			t.Fatalf("generation %s: %v", gen, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("path.3 exists (err=%v): MaxBackups=2 must keep exactly two generations", err)
	}

	total := countLines(mustRead(t, path)) +
		countLines(mustRead(t, path+".1")) +
		countLines(mustRead(t, path+".2"))
	if total != records {
		t.Fatalf("records across live+backups = %d, want %d: rotation lost lines", total, records)
	}
}
