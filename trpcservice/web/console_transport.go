package web

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type sseWriter struct {
	writer  http.ResponseWriter
	flusher http.Flusher
}

func newSSEWriter(writer http.ResponseWriter) *sseWriter {
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("X-Accel-Buffering", "no")
	flusher, _ := writer.(http.Flusher)
	return &sseWriter{writer: writer, flusher: flusher}
}

func (s *sseWriter) begin() {
	fmt.Fprint(s.writer, "retry: 1000\n\n")
	s.flush()
}

func (s *sseWriter) delta(content string) {
	s.deltaWithID("", content)
}

func (s *sseWriter) deltaWithID(eventID, content string) {
	s.emit(eventID, map[string]any{"type": "delta", "content": content})
}

func (s *sseWriter) done(reply string) {
	s.doneWithID("", reply)
}

func (s *sseWriter) doneWithID(eventID, reply string) {
	s.doneMessageWithID(eventID, reply, nil)
}

func (s *sseWriter) doneMessageWithID(eventID, reply string, card *channels.InteractiveCard, artifacts ...channels.OutboundArtifact) {
	payload := map[string]any{"type": "done", "reply": reply}
	if card != nil {
		payload["card"] = card
	}
	if len(artifacts) > 0 {
		payload["artifacts"] = artifacts
	}
	s.emit(eventID, payload)
}

func (s *sseWriter) cardWithID(eventID string, card *channels.InteractiveCard) {
	if card == nil {
		return
	}
	s.emit(eventID, map[string]any{"type": "card", "card": card})
}

func (s *sseWriter) error(message string) {
	s.errorWithID("", "", message)
}

func (s *sseWriter) errorWithID(eventID, code, message string) {
	payload := map[string]any{"type": "error", "message": message}
	if code != "" {
		payload["code"] = code
	}
	s.emit(eventID, payload)
}

func (s *sseWriter) keepAlive() {
	fmt.Fprint(s.writer, ": keepalive\n\n")
	s.flush()
}

func (s *sseWriter) emit(eventID string, payload any) {
	encoded, _ := json.Marshal(payload)
	if eventID != "" {
		fmt.Fprintf(s.writer, "id: %s\n", eventID)
	}
	fmt.Fprintf(s.writer, "data: %s\n\n", encoded)
	s.flush()
}

func (s *sseWriter) flush() {
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

func decodeJSONBody(request *http.Request, target any) error {
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = writer.Write(encoded)
}

func badRequest(writer http.ResponseWriter, message string) {
	writeJSON(writer, http.StatusBadRequest, map[string]any{"error": message})
}

func notFound(writer http.ResponseWriter, message string) {
	writeJSON(writer, http.StatusNotFound, map[string]any{"error": message})
}

func conflict(writer http.ResponseWriter, message string) {
	writeJSON(writer, http.StatusConflict, map[string]any{"error": message})
}

func serverError(writer http.ResponseWriter, operation string, err error) {
	writeJSON(writer, http.StatusInternalServerError, map[string]any{"error": fmt.Sprintf("%s: %v", operation, err)})
}

func methodNotAllowed(writer http.ResponseWriter, allowed string) {
	writer.Header().Set("Allow", allowed)
	writeJSON(writer, http.StatusMethodNotAllowed, map[string]any{"error": fmt.Sprintf("method not allowed, use %s", allowed)})
}
