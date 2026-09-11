// Command fake-model is an OpenAI-compatible streaming model that never calls
// a model. It exists so the whole platform loop (IM callback -> guardrails ->
// model -> streaming reply) can be exercised offline, and so the fault drills
// in docs/spec-deployment-fault-drill.md §2.6 can inject a misbehaving upstream
// on demand — no API key, no network, no python image (which the build host
// cannot pull: spec fact #1).
//
// It mirrors scripts/fake_model.py chunk for chunk, and adds runtime fault
// injection so a drill can retune the upstream without restarting a container:
//
//	GET  /__mode                              mode, delay, request count, and how
//	                                          many messages the last request carried
//	POST /__mode {"mode":"timeout","delay":30}  switch mode
//	POST /__mode {"mode":"ok","reset":true}     switch and zero the request count
//
// last_messages exists so the node-recovery drill (D6) can assert something Redis
// cannot. "The session lives in Redis" is checkable by reading Redis, but that
// says nothing about the process that just restarted: only the upstream can
// confirm the new node rebuilt the conversation and actually sent the old turns
// along. The count is the cheapest such witness — it grows as a session
// accumulates turns, and a node that came back with amnesia would send one.
//
//	fake-model                       # listens on 127.0.0.1:9009, mode ok
//	fake-model -addr :9009 -mode slow -delay 0.4
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Fault modes. Each one reproduces a failure the platform has to survive; the
// drill matrix in spec §2.6 names the drill that uses it.
const (
	modeOK       = "ok"       // baseline: scripted reply, streamed
	modeTimeout  = "timeout"  // hang past the caller's budget (D1)
	modeError500 = "error500" // upstream 5xx (D2)
	modeLimit429 = "limit429" // upstream rate limit (D2, risk #2)
	modeSlow     = "slow"     // slow but inside the budget: streaming + latency (D7)
	modeEmpty    = "empty"    // degenerate stream: 200 and nothing but [DONE]
	// modeTools streams one function call, then answers with text once the
	// payload carries the tool result — the P3 tool-loop drill (see tool.go).
	modeTools = "tools"
)

// Scripted reply, identical to scripts/fake_model.py. 内部资料 is split across
// two chunks on purpose: only a tail-window stream checker can catch it, which
// is what the output guardrail drill asserts.
var chunks = []string{"Hello ", "world, 内部", "资料 leaked.", " (done)"}

// Token counts reported in the final chunk, matching the python original so a
// drill can assert on them without knowing which fake model is running.
const (
	modelName        = "fake-model"
	completionID     = "chatcmpl-fake"
	promptTokens     = 11
	completionTokens = 7
	modePath         = "/__mode"
	healthPath       = "/healthz"
	retryAfter       = "2"
)

// defaultDelay is what a mode uses when the switch request omits one: the ok
// gap reproduces python's 50ms, and 30s of hanging is comfortably longer than
// any message_timeout a drill would set.
func defaultDelay(mode string) float64 {
	switch mode {
	case modeOK, modeTools:
		return 0.05
	case modeTimeout:
		return 30
	case modeSlow:
		return 0.4
	default:
		return 0
	}
}

func knownMode(m string) bool {
	switch m {
	case modeOK, modeTimeout, modeError500, modeLimit429, modeSlow, modeEmpty, modeTools:
		return true
	}
	return false
}

// settings is the immutable snapshot a request reads, so switching modes never
// races an in-flight stream. ToolName/ToolArgs are the scripted call the
// tools mode emits.
type settings struct {
	Mode     string  `json:"mode"`
	Delay    float64 `json:"delay"`
	ToolName string  `json:"tool,omitempty"`
	ToolArgs string  `json:"tool_args,omitempty"`
}

func (s settings) gap() time.Duration {
	return time.Duration(s.Delay * float64(time.Second))
}

type server struct {
	cfg      atomic.Pointer[settings]
	requests atomic.Int64
	lastMsgs atomic.Int64 // messages in the most recent completion request
	embeds   atomic.Int64 // embeddings calls served (see embedding.go)
	embed    embedState   // the embedding fault switch (see embedding.go)
	kf       *kfState     // the 微信客服 stub (see kf.go)
	tool     *toolState   // the tool-target stub (see tool.go)
	log      *log.Logger
}

func newServer(mode string, delay float64, out io.Writer) *server {
	s := &server{log: log.New(out, "[fake-model] ", log.LstdFlags|log.Lmsgprefix)}
	s.kf = &kfState{log: s.log}
	s.tool = &toolState{mode: "ok"}
	s.cfg.Store(&settings{Mode: mode, Delay: delay})
	return s
}

// Handler routes the control plane and the completion endpoint.
func (s *server) Handler() http.Handler {
	mux := http.NewServeMux()
	// The KF stub is more specific than "/" so ServeMux routes it first
	// regardless of registration order; the completion catch-all below stays
	// as permissive as it was for every other path.
	//
	// The two control paths are registered individually: they are not under
	// /cgi-bin/ (they are this binary's own surface, not a WeCom API shape),
	// and without an explicit route the catch-all would answer them as a chat
	// completion — measured, not assumed: the script silently never took
	// effect and send_msg looked recorded while nothing was.
	mux.HandleFunc("/cgi-bin/", s.handleKF)
	mux.HandleFunc(kfScriptPath, s.handleKF)
	mux.HandleFunc(kfSentPath, s.handleKF)
	mux.HandleFunc("/__tool/", s.handleTool)
	mux.HandleFunc(embedModePath, s.handleEmbedMode)
	mux.HandleFunc("/v1/embeddings", s.handleEmbeddings)
	mux.HandleFunc(modePath, s.handleMode)
	mux.HandleFunc(healthPath, s.handleHealth)
	// Every other path is a completion, whatever its shape: base_url is
	// operator-supplied and may or may not carry the /v1 prefix, and the python
	// original this mirrors is equally permissive. A drill that typos the path
	// still gets a model, not a 404 it has to debug.
	mux.HandleFunc("/", s.handleCompletion)
	return mux
}

type modeRequest struct {
	Mode     string   `json:"mode"`
	Delay    *float64 `json:"delay"`
	Reset    bool     `json:"reset"`
	ToolName string   `json:"tool"`
	ToolArgs string   `json:"tool_args"`
}

// stateResponse is what GET /__mode answers. requests counts completion calls,
// which is how the dedup drill (D3) proves a duplicate msg_id reached the model
// exactly once.
type stateResponse struct {
	Mode     string  `json:"mode"`
	Delay    float64 `json:"delay"`
	Requests int64   `json:"requests"`
	// LastMessages is the message count of the most recent completion request;
	// see the package comment for why D6 needs it. Zero until the first call, and
	// zero after a body that did not parse.
	LastMessages int64 `json:"last_messages"`
	// Embeddings counts /v1/embeddings calls, so a drill can assert that the
	// index job actually embedded through the configured endpoint.
	Embeddings int64 `json:"embeddings"`
	// EmbedMode reports the embedding fault switch ("ok" unless injected).
	EmbedMode string `json:"embed_mode"`
}

func (s *server) handleMode(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.state())
	case http.MethodPost:
		s.switchMode(w, r)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "use GET to read the mode and POST to change it")
	}
}

func (s *server) state() stateResponse {
	set := s.cfg.Load()
	return stateResponse{Mode: set.Mode, Delay: set.Delay, Requests: s.requests.Load(),
		LastMessages: s.lastMsgs.Load(), Embeddings: s.embeds.Load(), EmbedMode: s.embed.get()}
}

func (s *server) switchMode(w http.ResponseWriter, r *http.Request) {
	var p modeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&p); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("decode body: %v", err))
		return
	}
	if !knownMode(p.Mode) {
		writeErr(w, http.StatusBadRequest,
			fmt.Sprintf("unknown mode %q: want one of ok, timeout, error500, limit429, slow, empty, tools", p.Mode))
		return
	}
	delay := defaultDelay(p.Mode)
	if p.Delay != nil {
		if *p.Delay < 0 {
			writeErr(w, http.StatusBadRequest, "delay must not be negative")
			return
		}
		delay = *p.Delay
	}
	toolName := p.ToolName
	if toolName == "" {
		toolName = defaultToolName
	}
	toolArgs := p.ToolArgs
	if toolArgs == "" {
		toolArgs = `{"text":"ping-from-tool"}`
	}
	s.cfg.Store(&settings{Mode: p.Mode, Delay: delay, ToolName: toolName, ToolArgs: toolArgs})
	if p.Reset {
		s.requests.Store(0)
		s.embeds.Store(0)
	}
	s.log.Printf("mode=%s delay=%gs reset=%t", p.Mode, delay, p.Reset)
	writeJSON(w, http.StatusOK, s.state())
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.state())
}

func (s *server) handleCompletion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed,
			"POST a chat completion request, or use "+modePath+" / "+healthPath)
		return
	}

	// Drain the body before doing anything else. net/http only starts watching
	// for a client disconnect once the request body has been read (spec fact
	// #13), so a handler that hangs without draining never learns the caller
	// gave up — and the timeout drill would then measure this process's sleep
	// instead of the platform's budget.
	last := lastRequestOf(r.Body)
	set := s.cfg.Load()
	n := s.requests.Add(1)
	s.lastMsgs.Store(int64(last.messages))
	s.log.Printf("request #%d mode=%s delay=%gs messages=%d text=%q",
		n, set.Mode, set.Delay, last.messages, last.text)

	switch set.Mode {
	case modeError500:
		writeErr(w, http.StatusInternalServerError, "injected upstream failure")
		return
	case modeLimit429:
		w.Header().Set("Retry-After", retryAfter)
		writeErr(w, http.StatusTooManyRequests, "injected rate limit")
		return
	}

	if set.Mode == modeTimeout {
		// The point of the mode is to outlive the caller, so returning the
		// moment it goes away is the success path: it proves the platform's
		// cancellation really reaches the upstream connection.
		started := time.Now()
		select {
		case <-time.After(set.gap()):
			s.log.Printf("request #%d: slept the full %v and nobody cancelled", n, set.gap())
		case <-r.Context().Done():
			s.log.Printf("request #%d: caller gave up after %v (%v)", n,
				time.Since(started).Round(time.Millisecond), r.Context().Err())
			return
		}
		// delay is the hang, not the chunk gap: a caller that never cancelled
		// still gets a normally paced reply.
		set = &settings{Mode: set.Mode, Delay: defaultDelay(modeOK)}
	}

	// A request that asked for stream:false gets the non-streaming shape
	// (the summary job consumes a whole answer, not a stream). The scripted
	// text is the joined chunks, so a drill asserts the same content either
	// way; tool scripting does not apply to this shape.
	if !last.wantsStream {
		writeJSON(w, http.StatusOK, map[string]any{
			"id":      completionID,
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   modelName,
			"choices": []map[string]any{{
				"index":         0,
				"message":       map[string]string{"role": "assistant", "content": strings.Join(chunks, "")},
				"finish_reason": "stop",
			}},
			"usage": map[string]int{
				"prompt_tokens":     promptTokens,
				"completion_tokens": completionTokens,
				"total_tokens":      promptTokens + completionTokens,
			},
		})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	if set.Mode == modeEmpty {
		// Degenerate stream: a 200 with no content and no finish_reason. The
		// platform must still close the reply instead of waiting forever.
		writeSSE(w, flusher, "data: [DONE]\n\n")
		return
	}

	// Requests in tools mode split on whether the payload already carries the
	// tool result: the first round scripts a function call, the second (with a
	// role=tool message) answers with text.
	if set.Mode == modeTools && !last.hasToolRole {
		writeToolCallStream(w, flusher, set.ToolName, set.ToolArgs)
		return
	}
	toolChunks := chunks
	if set.Mode == modeTools {
		// The second round echoes the tool result it was given: that makes
		// "the model saw the retrieved passage" an assertion on the delivered
		// reply instead of a guess about what the platform sent.
		round := "tool round complete"
		if last.toolContent != "" {
			round += ": " + truncateForEcho(last.toolContent, 400)
		}
		toolChunks = []string{round}
	}

	for _, text := range toolChunks {
		if !writeSSE(w, flusher, event(chunkPayload(delta{Content: text}, "", nil))) {
			return
		}
		if !sleep(r.Context(), set.gap()) {
			s.log.Printf("request #%d: caller left mid-stream after %d chunks", n, len(toolChunks))
			return
		}
	}
	usage := map[string]int{
		"prompt_tokens":     promptTokens,
		"completion_tokens": completionTokens,
		"total_tokens":      promptTokens + completionTokens,
	}
	writeSSE(w, flusher, event(chunkPayload(delta{}, "stop", usage)))
	writeSSE(w, flusher, "data: [DONE]\n\n")
}

// sleep waits d unless the caller goes away first; false means it left.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// writeSSE pushes one event and reports whether the caller is still there.
func writeSSE(w http.ResponseWriter, f http.Flusher, payload string) bool {
	if _, err := io.WriteString(w, payload); err != nil {
		return false
	}
	f.Flush()
	return true
}

func event(payload string) string { return "data: " + payload + "\n\n" }

// truncateForEcho cuts a tool result to a prompt-sized echo, on a rune
// boundary.
func truncateForEcho(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

type delta struct {
	Content string `json:"content,omitempty"`
}

type choice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type chunkEvent struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []choice       `json:"choices"`
	Usage   map[string]int `json:"usage,omitempty"`
}

// chunkPayload renders one chat.completion.chunk. finish_reason stays present
// and null until the last chunk, exactly like the python original.
func chunkPayload(d delta, finish string, usage map[string]int) string {
	var reason *string
	if finish != "" {
		reason = &finish
	}
	body, err := json.Marshal(chunkEvent{
		ID: completionID, Object: "chat.completion.chunk",
		Created: time.Now().Unix(), Model: modelName,
		Choices: []choice{{Index: 0, Delta: d, FinishReason: reason}},
		Usage:   usage,
	})
	if err != nil {
		return "{}" // unreachable for these types; never fail a drill on it
	}
	return string(body)
}

// lastRequest is what the log line and the state endpoint report about the most
// recent completion request.
type lastRequest struct {
	text        string // content of the final message
	messages    int    // how many messages the caller sent; 0 if the body did not parse
	hasToolRole bool   // the payload already carries a tool result (second round)
	wantsStream bool   // stream defaults to true; summary jobs ask for false
	toolContent string // content of the last tool-role message, echoed in tools mode
}

// lastRequestOf extracts the final message and the message count. It always
// drains the body, past the 1 MiB it parses: an undrained request body is what
// stops net/http from noticing a disconnect (spec fact #13).
func lastRequestOf(body io.Reader) lastRequest {
	data, err := io.ReadAll(io.LimitReader(body, 1<<20))
	_, _ = io.Copy(io.Discard, body)
	if err != nil {
		return lastRequest{}
	}
	var req struct {
		Stream   *bool `json:"stream"`
		Messages []struct {
			Content json.RawMessage `json:"content"`
			Role    string          `json:"role"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(data, &req); err != nil || len(req.Messages) == 0 {
		return lastRequest{}
	}
	out := lastRequest{messages: len(req.Messages), wantsStream: true}
	if req.Stream != nil {
		out.wantsStream = *req.Stream
	}
	for _, m := range req.Messages {
		if m.Role == "tool" {
			out.hasToolRole = true
			var content string
			if err := json.Unmarshal(m.Content, &content); err == nil {
				out.toolContent = content
			}
		}
	}
	last := req.Messages[len(req.Messages)-1].Content
	var text string
	if err := json.Unmarshal(last, &text); err != nil {
		// Multimodal content is an array of parts; a drill never sends one, so
		// rendering it raw is enough to keep the log line honest.
		text = strings.TrimSpace(string(last))
	}
	out.text = text
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{
		"message": modelName + ": " + msg,
		"type":    errorType(code),
		"code":    code,
	}})
}

func errorType(code int) string {
	if code == http.StatusTooManyRequests {
		return "rate_limit_error"
	}
	return "server_error"
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9009", "address to listen on")
	mode := flag.String("mode", modeOK, "initial fault mode: ok, timeout, error500, limit429, slow, empty")
	delay := flag.Float64("delay", -1, "initial delay in seconds; omit for the mode's default")
	flag.Parse()

	if !knownMode(*mode) {
		fmt.Fprintf(os.Stderr, "fake-model: unknown -mode %q\n", *mode)
		os.Exit(2)
	}
	d := defaultDelay(*mode)
	if *delay >= 0 {
		d = *delay
	}

	s := newServer(*mode, d, os.Stdout)
	s.log.Printf("listening on http://%s (mode=%s delay=%gs)", *addr, *mode, d)
	srv := &http.Server{Addr: *addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		s.log.Printf("serve: %v", err)
		os.Exit(1)
	}
}
