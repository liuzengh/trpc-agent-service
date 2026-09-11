package eventadapter_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/eventadapter"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

const validFinal = `{"schema_version":1,"intent_id":"final-1","admission_id":"admission-1","run_id":"run-1","execution":{"attempt_id":"attempt-1","generation":7,"completion_id":"completion-1"},"sequence":3,"kind":"final","content":{"type":"text","text":"原始回答\n"},"deadline":"2026-09-05T12:00:00Z"}`

type acceptFunc func(context.Context, domain.Intent) (domain.Receipt, error)

func (f acceptFunc) AcceptReplyIntent(ctx context.Context, i domain.Intent) (domain.Receipt, error) {
	return f(ctx, i)
}

func TestHandleTransfersExactValidatedFinal(t *testing.T) {
	expected := domain.Intent{ID: "final-1", AdmissionID: "admission-1", RunID: "run-1", AttemptID: "attempt-1", CompletionID: "completion-1", ExecutionGeneration: 7, Sequence: 3, Text: "原始回答\n", Deadline: time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)}
	receipt := domain.Receipt{IntentID: "final-1", RunID: "run-1", PartCount: 2}
	calls := 0
	ctx := context.Background()
	h, err := eventadapter.NewHandler(acceptFunc(func(gotCtx context.Context, got domain.Intent) (domain.Receipt, error) {
		calls++
		if gotCtx != ctx || !reflect.DeepEqual(got, expected) {
			t.Fatalf("input was not transferred exactly: %#v", got)
		}
		return receipt, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Handle(ctx, []byte(validFinal))
	if err != nil || got != receipt || calls != 1 {
		t.Fatalf("receipt=%#v error=%v calls=%d", got, err, calls)
	}
}

func TestInvalidWireNeverCrossesApplicationSeam(t *testing.T) {
	calls := 0
	h, err := eventadapter.NewHandler(acceptFunc(func(context.Context, domain.Intent) (domain.Receipt, error) {
		calls++
		return domain.Receipt{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"empty": "", "truncated": "{", "null": "null",
		"caller target":        strings.Replace(validFinal, `"schema_version":1`, `"chat_id":"elsewhere","schema_version":1`, 1),
		"duplicate identity":   strings.Replace(validFinal, `"intent_id":"final-1"`, `"intent_id":"final-1","intent_id":"final-2"`, 1),
		"unsupported progress": strings.Replace(validFinal, `"kind":"final"`, `"kind":"progress"`, 1),
		"imprecise generation": strings.Replace(validFinal, `"generation":7`, `"generation":9007199254740993`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := h.Handle(context.Background(), []byte(raw))
			if !errors.Is(err, domain.ErrInvalid) || got != (domain.Receipt{}) || calls != 0 {
				t.Fatalf("invalid wire crossed seam: receipt=%#v error=%v calls=%d", got, err, calls)
			}
		})
	}
	if _, err := h.Handle(nil, []byte(validFinal)); !errors.Is(err, domain.ErrInvalid) || calls != 0 {
		t.Fatalf("nil context: %v calls=%d", err, calls)
	}
}

func TestApplicationFailureIsNotAReceipt(t *testing.T) {
	for _, want := range []error{domain.ErrUnauthorized, domain.ErrConflict, domain.ErrUnavailable, context.DeadlineExceeded} {
		h, err := eventadapter.NewHandler(acceptFunc(func(context.Context, domain.Intent) (domain.Receipt, error) {
			return domain.Receipt{}, want
		}))
		if err != nil {
			t.Fatal(err)
		}
		got, err := h.Handle(context.Background(), []byte(validFinal))
		if got != (domain.Receipt{}) || !errors.Is(err, want) {
			t.Fatalf("failure became receipt: got=%#v error=%v want=%v", got, err, want)
		}
	}
	if _, err := eventadapter.NewHandler(nil); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("missing application accepted: %v", err)
	}
}
