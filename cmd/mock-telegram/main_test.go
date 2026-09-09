package main

import (
	"bytes"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRepeatQueuesTwoUpdates(t *testing.T) {
	s := &server{sendCode: 200}
	r := httptest.NewRequest("POST", "/control/repeat", bytes.NewBufferString(`{"update_id":1}`))
	w := httptest.NewRecorder()
	s.repeat(w, r)
	if w.Code != 204 || len(s.updates) != 2 {
		t.Fatalf("code=%d updates=%d", w.Code, len(s.updates))
	}
}

func TestEmptyGetUpdatesWaitsInsteadOfBusyPolling(t *testing.T) {
	s := &server{sendCode: 200}
	r := httptest.NewRequest("GET", "/bot-token/getUpdates", nil)
	w := httptest.NewRecorder()
	started := time.Now()
	s.api(w, r)
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond {
		t.Fatalf("empty poll returned too quickly: %s", elapsed)
	}
	if w.Code != 200 || w.Body.String() != "{\"ok\":true,\"result\":[]}\n" {
		t.Fatalf("code=%d body=%q", w.Code, w.Body.String())
	}
}
