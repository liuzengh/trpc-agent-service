package main

import (
	"net/http/httptest"
	"testing"
)

func TestMockCompletion(t *testing.T) {
	s := &server{status: 200}
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	s.completion(w, r)
	if w.Code != 200 || s.requests != 1 {
		t.Fatalf("code=%d requests=%d", w.Code, s.requests)
	}
}
