package wecombot

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		c.mu.RLock()
		status := c.status
		c.mu.RUnlock()
		code := http.StatusServiceUnavailable
		if status == "subscribed" {
			code = http.StatusOK
		}
		writeStatus(w, code, status)
		return
	}
	if r.URL.Path != "/reply" {
		writeStatus(w, 404, "not_found")
		return
	}
	if r.Method != http.MethodPost {
		writeStatus(w, 405, "method_not_allowed")
		return
	}
	values := r.Header.Values(bridgeHeader)
	want := sha256.Sum256([]byte(c.config.BridgeToken))
	if len(values) != 1 {
		writeStatus(w, 401, "unauthorized")
		return
	}
	got := sha256.Sum256([]byte(values[0]))
	if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		writeStatus(w, 401, "unauthorized")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 6*maxReplyBytes+4096))
	if err != nil {
		writeStatus(w, 413, "rejected")
		return
	}
	var request struct {
		BotID          string `json:"bot_id"`
		RequestID      string `json:"request_id"`
		ConversationID string `json:"conversation_id"`
		Content        string `json:"content"`
		DeliveryID     string `json:"delivery_id"`
	}
	if !utf8.Valid(raw) || json.Unmarshal(raw, &request) != nil || request.BotID != c.config.BotID || !validIdentity(request.RequestID, 256) || !validIdentity(request.ConversationID, 256) || !validIdentity(request.DeliveryID, 256) || strings.TrimSpace(request.Content) == "" || strings.ContainsRune(request.Content, 0) || len(request.Content) > maxReplyBytes {
		writeStatus(w, 400, "rejected")
		return
	}
	c.mu.RLock()
	conn, store := c.active, c.spool
	c.mu.RUnlock()
	if conn == nil || store == nil {
		writeStatus(w, 503, "not_sent")
		return
	}
	conn.mu.Lock()
	ready := conn.ready
	conn.mu.Unlock()
	if !ready {
		writeStatus(w, 503, "not_sent")
		return
	}
	canonical, _ := json.Marshal(request)
	status, err := store.beginDelivery(request.DeliveryID, dataHash(canonical))
	if err != nil {
		writeStatus(w, 503, "not_sent")
		return
	}
	switch status {
	case "sent":
		writeStatus(w, 200, "sent")
		return
	case "rejected":
		writeStatus(w, 422, "rejected")
		return
	case "new":
	default:
		writeStatus(w, 409, "unknown")
		return
	}
	body := map[string]any{"msgtype": "stream", "stream": map[string]any{"id": spoolKey("reply", request.DeliveryID), "finish": true, "content": request.Content}}
	code, sent, exchangeErr := conn.exchange(r.Context(), request.RequestID, command("aibot_respond_msg", request.RequestID, body), c.ackTimeout, true)
	if !sent {
		if store.finishDelivery(request.DeliveryID, "not_sent") != nil {
			writeStatus(w, 409, "unknown")
			return
		}
		writeStatus(w, 503, "not_sent")
		return
	}
	if exchangeErr != nil {
		writeStatus(w, 502, "unknown")
		return
	}
	status = "sent"
	httpCode := 200
	if code == 45009 {
		// The provider explicitly rejected admission because of its quota.
		// Keep the same delivery identity retryable; a missing ACK never takes
		// this branch and remains unknown.
		status = "not_sent"
		httpCode = http.StatusTooManyRequests
		w.Header().Set("Retry-After", "60")
	} else if code != 0 {
		status = "rejected"
		httpCode = 422
	}
	if store.finishDelivery(request.DeliveryID, status) != nil {
		writeStatus(w, 502, "unknown")
		return
	}
	c.emit("reply", status, code)
	writeStatus(w, httpCode, status)
}
func writeStatus(w http.ResponseWriter, code int, status string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}
