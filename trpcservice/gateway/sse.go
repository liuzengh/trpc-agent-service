package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type SSEHandler struct {
	Events         SharedEventStore
	Principals     PrincipalResolver
	Cursors        *CursorCodec
	PollInterval   time.Duration
	ReplayLimit    int
	MaxSubscribers int64
	active         atomic.Int64
}

func (h *SSEHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h == nil || h.Events == nil || h.Principals == nil || h.Cursors == nil {
		writeControlError(writer, runtime.ErrCapabilityUnsupported)
		return
	}
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	requestID, ok := parseEventsPath(request.URL.Path)
	if !ok {
		http.NotFound(writer, request)
		return
	}
	principal, err := h.Principals.Resolve(request)
	if err != nil || !principal.Authenticated || principal.TenantID == "" || principal.SubjectID == "" {
		writeControlError(writer, ErrUnauthenticated)
		return
	}
	if !principal.CanRead {
		writeControlError(writer, ErrForbidden)
		return
	}
	maximum := h.MaxSubscribers
	if maximum <= 0 {
		maximum = 128
	}
	if h.active.Add(1) > maximum {
		h.active.Add(-1)
		http.Error(writer, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
		return
	}
	defer h.active.Add(-1)
	key := ExecutionKey{TenantID: principal.TenantID, RequestID: requestID}
	after, err := h.decodeCursor(request, key)
	if err != nil {
		writeControlError(writer, err)
		return
	}
	limit := normalizeReplayLimit(h.ReplayLimit)
	page, err := h.Events.Replay(request.Context(), key, after, limit)
	if err != nil {
		writeControlError(writer, err)
		return
	}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeControlError(writer, runtime.ErrCapabilityUnsupported)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(http.StatusOK)
	last, writeErr := h.writePage(writer, flusher, key, after, page)
	if writeErr != nil || page.Terminal {
		return
	}
	interval := h.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case <-ticker.C:
			page, err = h.Events.Replay(request.Context(), key, last, limit)
			if err != nil {
				return
			}
			last, writeErr = h.writePage(writer, flusher, key, last, page)
			if writeErr != nil || page.Terminal {
				return
			}
		}
	}
}

func (h *SSEHandler) decodeCursor(request *http.Request, key ExecutionKey) (uint64, error) {
	value := request.URL.Query().Get("cursor")
	if value == "" {
		value = request.Header.Get("Last-Event-ID")
	}
	if value == "" {
		return 0, nil
	}
	return h.Cursors.Decode(value, key)
}

func (h *SSEHandler) writePage(writer http.ResponseWriter, flusher http.Flusher, key ExecutionKey, after uint64, page EventPage) (uint64, error) {
	last := after
	for _, event := range page.Events {
		if event.TenantID != key.TenantID || event.RequestID != key.RequestID || event.Sequence <= last || !validSSEEventType(event.Type) || !json.Valid(event.Data) {
			return last, runtime.ErrInvariantViolation
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, event.Data); err != nil {
			return last, runtime.ErrInvariantViolation
		}
		cursor, err := h.Cursors.Encode(key, event.Sequence)
		if err != nil {
			return last, err
		}
		if _, err := fmt.Fprintf(writer, "id: %s\nevent: %s\ndata: %s\n\n", cursor, event.Type, compact.Bytes()); err != nil {
			return last, err
		}
		flusher.Flush()
		last = event.Sequence
	}
	if page.LastSequence < last || (page.Terminal && last < page.LastSequence) {
		return last, runtime.ErrInvariantViolation
	}
	return last, nil
}

func validSSEEventType(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, current := range value {
		if (current < 'a' || current > 'z') && (current < '0' || current > '9') && current != '.' && current != '_' && current != '-' {
			return false
		}
	}
	return true
}

func parseEventsPath(path string) (string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return func() (string, bool) {
		if len(parts) == 4 && parts[0] == "v1" && parts[1] == "agent-runs" && parts[3] == "events" && parts[2] != "" {
			return parts[2], true
		}
		return "", false
	}()
}
