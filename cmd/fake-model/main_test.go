package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// safeBuffer collects handler logs, which arrive from concurrent request
// goroutines.
type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

const chatBody = `{"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"say hi"}]}`

// startFake spins up a fake-model with a captured log and registers teardown.
func startFake(t *testing.T, mode string, delay float64) (*httptest.Server, *safeBuffer) {
	t.Helper()
	logs := &safeBuffer{}
	srv := httptest.NewServer(newServer(mode, delay, logs).Handler())
	t.Cleanup(srv.Close)
	return srv, logs
}

// setMode switches the fault mode over HTTP, the way a drill script does.
func setMode(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+modePath, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", modePath, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s %s = %d: %s", modePath, body, resp.StatusCode, out)
	}
	return resp
}

// getState reads the control plane the way a drill script does.
func getState(t *testing.T, url string) stateResponse {
	t.Helper()
	resp, err := http.Get(url + modePath)
	if err != nil {
		t.Fatalf("GET %s: %v", modePath, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d", modePath, resp.StatusCode)
	}
	var st stateResponse
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	return st
}

// events splits an SSE body into its data payloads, in arrival order.
func events(t *testing.T, body io.Reader) []string {
	t.Helper()
	var out []string
	sc := bufio.NewScanner(body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			out = append(out, strings.TrimPrefix(line, "data: "))
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan stream: %v", err)
	}
	return out
}

func post(t *testing.T, url, path, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

// TestCompletionMatchesThePythonOriginal is the parity gate: scripts/fake_model.py
// stays in the repo for keyless smoke tests, and a drill must not be able to
// tell which of the two is answering. Same chunks, same split of 内部资料, same
// usage block, same terminator.
func TestCompletionMatchesThePythonOriginal(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)

	// The path is deliberately not the canonical one: base_url is
	// operator-supplied and may or may not carry the /v1 prefix.
	resp := post(t, srv.URL, "/chat/completions", chatBody)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q, want text/event-stream", ct)
	}

	evs := events(t, resp.Body)
	if len(evs) != len(chunks)+2 {
		t.Fatalf("got %d events, want %d (chunks + finish + [DONE]): %v", len(evs), len(chunks)+2, evs)
	}
	if evs[len(evs)-1] != "[DONE]" {
		t.Fatalf("last event = %q, want [DONE]", evs[len(evs)-1])
	}

	var text strings.Builder
	for i, want := range chunks {
		var c chunkEvent
		if err := json.Unmarshal([]byte(evs[i]), &c); err != nil {
			t.Fatalf("event %d is not a chunk: %v (%s)", i, err, evs[i])
		}
		if c.ID != completionID || c.Object != "chat.completion.chunk" || c.Model != modelName {
			t.Fatalf("event %d envelope = %+v", i, c)
		}
		if len(c.Choices) != 1 || c.Choices[0].Index != 0 {
			t.Fatalf("event %d choices = %+v", i, c.Choices)
		}
		if got := c.Choices[0].Delta.Content; got != want {
			t.Fatalf("chunk %d = %q, want %q", i, got, want)
		}
		if c.Choices[0].FinishReason != nil {
			t.Fatalf("chunk %d must not carry a finish_reason, got %q", i, *c.Choices[0].FinishReason)
		}
		if !strings.Contains(evs[i], `"finish_reason":null`) {
			t.Fatalf("chunk %d must render finish_reason as null, not omit it: %s", i, evs[i])
		}
		text.WriteString(c.Choices[0].Delta.Content)
	}

	// The output guardrail drill depends on the keyword never appearing whole
	// inside a single chunk: only a tail-window checker can catch it. So it
	// must be absent from every chunk yet present in the reassembled reply.
	for i := range chunks {
		if strings.Contains(evs[i], "内部资料") {
			t.Fatalf("chunk %d leaks the whole keyword: %s", i, evs[i])
		}
	}
	if got := text.String(); got != "Hello world, 内部资料 leaked. (done)" {
		t.Fatalf("reassembled reply = %q", got)
	}

	var last chunkEvent
	if err := json.Unmarshal([]byte(evs[len(evs)-2]), &last); err != nil {
		t.Fatalf("finish event is not a chunk: %v (%s)", err, evs[len(evs)-2])
	}
	if last.Choices[0].FinishReason == nil || *last.Choices[0].FinishReason != "stop" {
		t.Fatalf("finish_reason = %v, want stop", last.Choices[0].FinishReason)
	}
	if last.Choices[0].Delta.Content != "" {
		t.Fatalf("finish chunk must carry no content, got %q", last.Choices[0].Delta.Content)
	}
	want := map[string]int{"prompt_tokens": promptTokens, "completion_tokens": completionTokens,
		"total_tokens": promptTokens + completionTokens}
	for k, v := range want {
		if last.Usage[k] != v {
			t.Fatalf("usage[%s] = %d, want %d (%v)", k, last.Usage[k], v, last.Usage)
		}
	}

	// The counter is what the dedup drill (D3) reads to prove a duplicate
	// msg_id reached the model exactly once.
	if st := getState(t, srv.URL); st.Requests != 1 {
		t.Fatalf("requests = %d, want 1", st.Requests)
	}
}

// TestFaultModes covers every injection the drill matrix uses, switched at
// runtime over HTTP: no restart, no re-create of the container.
func TestFaultModes(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)

	t.Run("error500", func(t *testing.T) {
		setMode(t, srv.URL, `{"mode":"error500"}`).Body.Close()
		resp := post(t, srv.URL, "/v1/chat/completions", chatBody)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", resp.StatusCode)
		}
		out, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(out), `"type":"server_error"`) {
			t.Fatalf("body must be an OpenAI-shaped error: %s", out)
		}
	})

	t.Run("limit429", func(t *testing.T) {
		setMode(t, srv.URL, `{"mode":"limit429"}`).Body.Close()
		resp := post(t, srv.URL, "/v1/chat/completions", chatBody)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429", resp.StatusCode)
		}
		if got := resp.Header.Get("Retry-After"); got != retryAfter {
			t.Fatalf("Retry-After = %q, want %q", got, retryAfter)
		}
		out, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(out), `"type":"rate_limit_error"`) {
			t.Fatalf("body must be a rate limit error: %s", out)
		}
	})

	t.Run("empty", func(t *testing.T) {
		setMode(t, srv.URL, `{"mode":"empty"}`).Body.Close()
		resp := post(t, srv.URL, "/v1/chat/completions", chatBody)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (a degenerate stream is not an HTTP error)", resp.StatusCode)
		}
		evs := events(t, resp.Body)
		if len(evs) != 1 || evs[0] != "[DONE]" {
			t.Fatalf("events = %v, want only [DONE]", evs)
		}
	})

	t.Run("slow paces the chunks", func(t *testing.T) {
		setMode(t, srv.URL, `{"mode":"slow","delay":0.15}`).Body.Close()
		resp := post(t, srv.URL, "/v1/chat/completions", chatBody)
		defer resp.Body.Close()

		start := time.Now()
		sc := bufio.NewScanner(resp.Body)
		if !sc.Scan() {
			t.Fatalf("no first event: %v", sc.Err())
		}
		first := time.Since(start)
		// Streaming, not buffered: the first chunk must arrive before the whole
		// reply could possibly have been produced.
		if first > 100*time.Millisecond {
			t.Fatalf("first chunk took %v, want it streamed at once", first)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		total := time.Since(start)
		if want := time.Duration(len(chunks)-1) * 150 * time.Millisecond; total < want {
			t.Fatalf("whole stream took %v, want at least %v (the gap between chunks)", total, want)
		}
		rest := strings.TrimSpace(string(body))
		if !strings.HasSuffix(rest, "data: [DONE]") {
			t.Fatalf("slow stream must still finish with [DONE]: %q", rest)
		}
	})

	t.Run("back to ok", func(t *testing.T) {
		setMode(t, srv.URL, `{"mode":"ok","reset":true}`).Body.Close()
		resp := post(t, srv.URL, "/v1/chat/completions", chatBody)
		defer resp.Body.Close()
		if evs := events(t, resp.Body); len(evs) != len(chunks)+2 {
			t.Fatalf("events = %d, want %d", len(evs), len(chunks)+2)
		}
		// reset zeroes the counter, so this call is the only one counted.
		if st := getState(t, srv.URL); st.Requests != 1 || st.Mode != modeOK {
			t.Fatalf("state = %+v, want one request in mode ok", st)
		}
	})
}

// TestTimeoutModeObservesCancellation is the guard for spec fact #13: a hanging
// upstream must notice the caller leaving. Server.Close waits for outstanding
// requests, so a handler that never saw the disconnect would block here for the
// whole delay — which is exactly how the bug first showed up.
func TestTimeoutModeObservesCancellation(t *testing.T) {
	srv, logs := startFake(t, modeOK, 0)
	setMode(t, srv.URL, `{"mode":"timeout","delay":5}`).Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		srv.URL+"/v1/chat/completions", strings.NewReader(chatBody))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		t.Fatal("the caller must not get a reply: the model hangs past its budget")
	}

	start := time.Now()
	srv.Close()
	if el := time.Since(start); el > time.Second {
		t.Fatalf("Close took %v: the handler never noticed the disconnect and slept its full delay", el)
	}
	if !strings.Contains(logs.String(), "caller gave up") {
		t.Fatalf("the fake model must log the cancellation a drill asserts on:\n%s", logs.String())
	}
}

// TestModeEndpointValidation covers the control plane's own error paths: a drill
// that fat-fingers a mode must be told, not silently left injecting nothing.
func TestModeEndpointValidation(t *testing.T) {
	srv, _ := startFake(t, modeSlow, 0.25)

	st := getState(t, srv.URL)
	if st.Mode != modeSlow || st.Delay != 0.25 {
		t.Fatalf("state = %+v, want slow/0.25 as started", st)
	}

	cases := []struct {
		name, body string
		want       int
	}{
		{"unknown mode", `{"mode":"chaos"}`, http.StatusBadRequest},
		{"negative delay", `{"mode":"timeout","delay":-1}`, http.StatusBadRequest},
		{"malformed json", `{"mode":`, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, err := http.Post(srv.URL+modePath, "application/json", strings.NewReader(c.body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != c.want {
				out, _ := io.ReadAll(resp.Body)
				t.Fatalf("%s = %d, want %d: %s", c.body, resp.StatusCode, c.want, out)
			}
		})
	}

	// A rejected switch must leave the running mode alone.
	if st := getState(t, srv.URL); st.Mode != modeSlow || st.Delay != 0.25 {
		t.Fatalf("state after rejected switches = %+v, want it unchanged", st)
	}

	// Omitting delay falls back to the mode's documented default.
	resp := setMode(t, srv.URL, `{"mode":"timeout"}`)
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.Mode != modeTimeout || st.Delay != defaultDelay(modeTimeout) {
		t.Fatalf("state = %+v, want timeout/%v", st, defaultDelay(modeTimeout))
	}

	for _, method := range []string{http.MethodDelete, http.MethodPut} {
		req, err := http.NewRequest(method, srv.URL+modePath, nil)
		if err != nil {
			t.Fatal(err)
		}
		rw, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		rw.Body.Close()
		if rw.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s = %d, want 405", method, modePath, rw.StatusCode)
		}
	}
}

// TestNonCompletionRoutes pins the two routes a Compose healthcheck and a drill
// script rely on, and that a GET to the model path is refused rather than
// answered with a stream nobody asked for.
func TestNonCompletionRoutes(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)

	resp, err := http.Get(srv.URL + healthPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health = %d, want 200", resp.StatusCode)
	}
	var st stateResponse
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.Mode != modeOK {
		t.Fatalf("health mode = %q, want %q", st.Mode, modeOK)
	}

	resp = post(t, srv.URL, "/v1/chat/completions", chatBody)
	resp.Body.Close()
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	rw, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Body.Close()
	if rw.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET on the model path = %d, want 405", rw.StatusCode)
	}
}

// TestLastRequestOf pins both halves of the extraction: the text a drill greps
// for in the log, and the message count the node-recovery drill (D6) reads back
// off /__mode. The count is what distinguishes "the session survived in Redis"
// from "the restarted node actually sent it upstream", so a body that does not
// parse has to report 0 rather than a stale or guessed number.
func TestLastRequestOf(t *testing.T) {
	cases := []struct {
		name, body, want string
		msgs             int
	}{
		{"last of several", chatBody, "say hi", 2},
		{"single message", `{"messages":[{"role":"user","content":"only"}]}`, "only", 1},
		{"empty", "", "", 0},
		{"malformed", `{"messages":`, "", 0},
		{"no messages", `{"model":"m"}`, "", 0},
		{"multimodal", `{"messages":[{"content":[{"type":"text","text":"part"}]}]}`,
			`[{"type":"text","text":"part"}]`, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := lastRequestOf(strings.NewReader(c.body))
			if got.text != c.want || got.messages != c.msgs {
				t.Fatalf("lastRequestOf(%q) = %+v, want text %q and %d messages",
					c.body, got, c.want, c.msgs)
			}
		})
	}
}

// TestStateReportsLastMessageCount is the contract D6 reads: after a completion
// carrying N messages, GET /__mode answers last_messages=N. Without it the drill
// can only observe Redis, which never involves the process under test.
func TestStateReportsLastMessageCount(t *testing.T) {
	srv, _ := startFake(t, modeOK, 0)

	if st := getState(t, srv.URL); st.LastMessages != 0 {
		t.Fatalf("last_messages before any call = %d, want 0", st.LastMessages)
	}
	for _, body := range []string{
		`{"messages":[{"role":"user","content":"first"}]}`,
		`{"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"hi"},{"role":"user","content":"second"}]}`,
	} {
		post(t, srv.URL, "/v1/chat/completions", body).Body.Close()
	}
	st := getState(t, srv.URL)
	if st.LastMessages != 3 {
		t.Fatalf("last_messages = %d, want 3 (the second request's count)", st.LastMessages)
	}
	if st.Requests != 2 {
		t.Fatalf("requests = %d, want 2", st.Requests)
	}
}
