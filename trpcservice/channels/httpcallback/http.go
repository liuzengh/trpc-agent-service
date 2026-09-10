// Package httpcallback exposes provider callbacks without moving protocol,
// tenant resolution, or durable ACK decisions into the HTTP layer.
package httpcallback

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

const defaultMaxBodyBytes int64 = 1 << 20

type Acceptor interface {
	Accept(context.Context, channel.CallbackRequest) ([]ingress.AcceptedEvent, error)
}

type ChallengeVerifier interface {
	Verify(context.Context, channel.CallbackRequest) (channel.HTTPResponse, error)
}

type Endpoint struct {
	Adapter    channel.HTTPAdapter
	Pipeline   Acceptor
	Challenges ChallengeVerifier
	MaxBody    int64
	Now        func() time.Time
}

func NewEndpoint(adapter channel.HTTPAdapter, pipeline Acceptor, challenges ChallengeVerifier) (*Endpoint, error) {
	if adapter == nil || pipeline == nil || challenges == nil || adapter.ID() == "" {
		return nil, runtime.ErrInvariantViolation
	}
	return &Endpoint{Adapter: adapter, Pipeline: pipeline, Challenges: challenges}, nil
}

func (e *Endpoint) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if e == nil || e.Adapter == nil || e.Pipeline == nil || e.Challenges == nil {
		http.Error(writer, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	if request.Method != http.MethodGet && request.Method != http.MethodPost {
		writer.Header().Set("Allow", "GET, POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	callback, err := e.callbackRequest(writer, request)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(writer, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return
	}
	if e.Adapter.IsChallenge(callback) {
		response, err := e.Challenges.Verify(request.Context(), callback)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeSuccess(writer, response)
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	accepted, err := e.Pipeline.Accept(request.Context(), callback)
	if err != nil {
		writeError(writer, err)
		return
	}
	if len(accepted) == 0 {
		writeError(writer, runtime.ErrInvariantViolation)
		return
	}
	writeSuccess(writer, e.Adapter.CallbackACK())
}

func (e *Endpoint) callbackRequest(writer http.ResponseWriter, request *http.Request) (channel.CallbackRequest, error) {
	limit := e.MaxBody
	if limit <= 0 {
		limit = defaultMaxBodyBytes
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, limit))
	if err != nil {
		return channel.CallbackRequest{}, err
	}
	now := time.Now().UTC()
	if e.Now != nil {
		now = e.Now().UTC()
	}
	headers := make(map[string]string, len(request.Header))
	for key, values := range request.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	query := make(map[string]string, len(request.URL.Query()))
	for key, values := range request.URL.Query() {
		if len(values) > 0 {
			query[key] = values[0]
		}
	}
	return channel.CallbackRequest{Headers: headers, Query: query, Body: body, ReceivedAt: now}, nil
}

type Route struct {
	Pattern  string
	Endpoint *Endpoint
}

// NewMux is the injectable HTTP composition root. Persistent stores, scoped
// secrets, workers, and provider clients are constructed by the process and
// passed through each Endpoint; this function never creates in-memory fallbacks.
func NewMux(routes ...Route) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	seen := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		if route.Pattern == "" || !strings.HasPrefix(route.Pattern, "/") || route.Endpoint == nil {
			return nil, runtime.ErrInvariantViolation
		}
		if _, exists := seen[route.Pattern]; exists {
			return nil, runtime.ErrIdempotencyCollision
		}
		seen[route.Pattern] = struct{}{}
		mux.Handle(route.Pattern, route.Endpoint)
	}
	if len(seen) == 0 {
		return nil, runtime.ErrInvariantViolation
	}
	return mux, nil
}

func writeSuccess(writer http.ResponseWriter, response channel.HTTPResponse) {
	if response.ContentType == "" || len(response.Body) == 0 || len(response.Body) > 64<<10 {
		writeError(writer, runtime.ErrInvariantViolation)
		return
	}
	writer.Header().Set("Content-Type", response.ContentType)
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(response.Body)
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, runtime.ErrInvalidEnvelope), errors.Is(err, runtime.ErrVersionMismatch),
		errors.Is(err, runtime.ErrNotFound), errors.Is(err, runtime.ErrTenantScope):
		status = http.StatusUnauthorized
	case errors.Is(err, runtime.ErrIdempotencyCollision), errors.Is(err, runtime.ErrVersionConflict):
		status = http.StatusConflict
	case errors.Is(err, runtime.ErrBackendUnavailable):
		status = http.StatusServiceUnavailable
	}
	http.Error(writer, http.StatusText(status), status)
}
