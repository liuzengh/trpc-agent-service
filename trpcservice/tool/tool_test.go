package tool

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

type writer struct{ events []audit.Event }

func (w *writer) Append(_ context.Context, e audit.Event) (audit.AppendResult, error) {
	w.events = append(w.events, e)
	return audit.AppendResult{Event: e}, nil
}

func TestPolicyDecisionAuditsWithoutPayload(t *testing.T) {
	w := &writer{}
	p := Policy{Recorder: audit.NewRecorder(w, "tenant"), Allowed: map[string]Decision{"search": Allow, "admin": ApprovalRequired}}
	if _, err := p.Decide(context.Background(), "req", "trace", "search"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Decide(context.Background(), "req2", "trace2", "admin"); err != ErrApprovalRequired {
		t.Fatalf("approval err = %v", err)
	}
	if len(w.events) != 2 || w.events[0].EventType != audit.EventToolAllowed || w.events[1].EventType != audit.EventToolApprovalRequired {
		t.Fatalf("events = %+v", w.events)
	}
}

func TestPolicyDeniesByDefault(t *testing.T) {
	p := Policy{}
	if _, err := p.Decide(context.Background(), "req", "trace", "unknown"); err != ErrDenied {
		t.Fatalf("err = %v", err)
	}
}

func TestPolicyRejectsInvalidNamesAndAuditFailures(t *testing.T) {
	p := Policy{}
	for _, name := range []string{"", strings.Repeat("x", 257)} {
		if _, err := p.Decide(context.Background(), "req", "trace", name); !errors.Is(err, audit.ErrInvalid) {
			t.Fatalf("name %q err=%v", name, err)
		}
	}
	w := &failingWriter{}
	p = Policy{Recorder: audit.NewRecorder(w, "t"), Allowed: map[string]Decision{"search": Allow}}
	if _, err := p.Decide(context.Background(), "req", "trace", "search"); !errors.Is(err, audit.ErrWriteFailed) {
		t.Fatalf("err=%v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Append(context.Context, audit.Event) (audit.AppendResult, error) {
	return audit.AppendResult{}, errors.New("down")
}
