package telemetry

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type auditSinkFunc func([]byte) (int, error)

func (f auditSinkFunc) Write(data []byte) (int, error) { return f(data) }

func TestDurableAuditSinkIgnoresMirrorFailureAfterPersisting(t *testing.T) {
	var stored bytes.Buffer
	mirror := auditSinkFunc(func([]byte) (int, error) { return 0, errors.New("stderr unavailable") })
	sink := NewDurableAuditSink(&stored, mirror)
	data := []byte("audit-record\n")
	n, err := sink.Write(data)
	if err != nil || n != len(data) || !bytes.Equal(stored.Bytes(), data) {
		t.Fatalf("n=%d err=%v stored=%q", n, err, stored.String())
	}
}

func TestDurableAuditSinkSurfacesPersistenceFailureAndStillMirrors(t *testing.T) {
	want := errors.New("database unavailable")
	var mirrored bytes.Buffer
	sink := NewDurableAuditSink(auditSinkFunc(func([]byte) (int, error) { return 0, want }), &mirrored)
	data := []byte("audit-record\n")
	if _, err := sink.Write(data); !errors.Is(err, want) || !bytes.Equal(mirrored.Bytes(), data) {
		t.Fatalf("err=%v mirrored=%q", err, mirrored.String())
	}
}

func TestDurableAuditSinkRejectsShortOrMissingDurableWriter(t *testing.T) {
	for _, writer := range []io.Writer{nil, auditSinkFunc(func([]byte) (int, error) { return 1, nil })} {
		if _, err := NewDurableAuditSink(writer, nil).Write([]byte("record")); err == nil {
			t.Fatal("missing or incomplete durable write accepted")
		}
	}
}
