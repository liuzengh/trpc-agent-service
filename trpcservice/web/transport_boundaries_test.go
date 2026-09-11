package web

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestSSEWriterEmitsProtocolFramesAndHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	writer := newSSEWriter(recorder)
	writer.begin()
	writer.delta("partial")
	writer.done("complete")
	writer.doneMessageWithID("card-1", "", &channels.InteractiveCard{Title: "订单信息", Body: "已找到订单"})
	writer.error("failed")
	writer.keepAlive()

	body := recorder.Body.String()
	for _, expected := range []string{
		"retry: 1000\n\n",
		"\"type\":\"delta\"",
		"\"content\":\"partial\"",
		"\"type\":\"done\"",
		"\"reply\":\"complete\"",
		"\"title\":\"订单信息\"",
		"\"body\":\"已找到订单\"",
		"\"type\":\"error\"",
		": keepalive\n\n",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("SSE body %q does not contain %q", body, expected)
		}
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := recorder.Header().Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q", got)
	}
}

func TestHTTPTransportWritesServerAndMarshalErrors(t *testing.T) {
	recorder := httptest.NewRecorder()
	serverError(recorder, "load data", errors.New("database unavailable"))
	if recorder.Code != 500 || !strings.Contains(recorder.Body.String(), "load data") {
		t.Fatalf("serverError response = %d: %s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	writeJSON(recorder, 200, map[string]any{"unsupported": func() {}})
	if recorder.Code != 500 {
		t.Fatalf("marshal failure status = %d, want 500", recorder.Code)
	}
}

func TestParseOptionalTimeAcceptsSupportedForms(t *testing.T) {
	t.Parallel()
	if value, err := parseOptionalTime(""); err != nil || value != nil {
		t.Fatalf("empty parseOptionalTime() = %v, %v", value, err)
	}
	rfc, err := parseOptionalTime("2026-09-11T10:20:30Z")
	if err != nil || !rfc.Equal(time.Date(2026, 9, 11, 10, 20, 30, 0, time.UTC)) {
		t.Fatalf("RFC3339 parseOptionalTime() = %v, %v", rfc, err)
	}
	date, err := parseOptionalTime("2026-09-11")
	if err != nil || !date.Equal(time.Date(2026, 9, 11, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("date parseOptionalTime() = %v, %v", date, err)
	}
	if _, err := parseOptionalTime("11/09/2026"); err == nil {
		t.Fatal("parseOptionalTime accepted unsupported format")
	}
}

func TestFilterMemoryEntriesOrdersEventTimeAndDropsInvalidEntries(t *testing.T) {
	t.Parallel()
	early := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	late := early.Add(time.Hour)
	entries := []*agentmemory.Entry{
		nil,
		{Memory: nil},
		{Memory: &agentmemory.Memory{Memory: "late", Kind: agentmemory.KindFact, EventTime: &late}},
		{Memory: &agentmemory.Memory{Memory: "episode", Kind: agentmemory.KindEpisode, EventTime: &early}},
		{Memory: &agentmemory.Memory{Memory: "early", Kind: agentmemory.KindFact, EventTime: &early}},
		{Memory: &agentmemory.Memory{Memory: "undated", Kind: agentmemory.KindFact}},
	}
	filtered := filterMemoryEntries(entries, agentmemory.KindFact, nil, nil, true)
	if len(filtered) != 3 || filtered[0].Memory.Memory != "early" || filtered[1].Memory.Memory != "late" || filtered[2].Memory.Memory != "undated" {
		t.Fatalf("filtered memories = %#v", filtered)
	}
	after := early.Add(time.Minute)
	filtered = filterMemoryEntries(entries, "", &after, nil, false)
	if len(filtered) != 1 || filtered[0].Memory.Memory != "late" {
		t.Fatalf("time-filtered memories = %#v", filtered)
	}
}
