package gateway

import (
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// HTTPHandlerOptions describes the production platform API composition. The
// constructor refuses partial wiring so a listener cannot accidentally expose
// an unauthenticated or non-durable run endpoint.
type HTTPHandlerOptions struct {
	Submitter      RunSubmitter
	Tasks          TaskStore
	Events         SharedEventStore
	Principals     PrincipalResolver
	Routes         RunRouteResolver
	Cursors        *CursorCodec
	Readiness      interface{ Ready() bool }
	MaxBody        int64
	PollInterval   time.Duration
	ReplayLimit    int
	MaxSubscribers int64
}

// NewHTTPHandler mounts the durable platform endpoints:
//
//	POST /v1/agent-runs
//	GET/POST /v1/agent-runs/{request}[/cancel]
//	GET /v1/agent-runs/{request}/events
func NewHTTPHandler(options HTTPHandlerOptions) (http.Handler, error) {
	if options.Submitter.Inbox == nil || options.Submitter.Payloads == nil || options.Submitter.Dispatcher == nil || options.Submitter.PayloadKeyVersion < 1 ||
		options.Tasks == nil || options.Events == nil || options.Principals == nil || options.Routes == nil || options.Cursors == nil || options.Readiness == nil {
		return nil, runtime.ErrInvariantViolation
	}
	runs := RunHandler{Submitter: options.Submitter, Principals: options.Principals, Routes: options.Routes, MaxBody: options.MaxBody}
	control := ControlHandler{Tasks: options.Tasks, Principals: options.Principals}
	events := &SSEHandler{Events: options.Events, Principals: options.Principals, Cursors: options.Cursors,
		PollInterval: options.PollInterval, ReplayLimit: options.ReplayLimit, MaxSubscribers: options.MaxSubscribers}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimRight(r.URL.Path, "/")
		switch {
		case path == "/v1/agent-runs":
			if r.Method == http.MethodPost && !options.Readiness.Ready() {
				writeControlError(w, runtime.ErrBackendUnavailable)
				return
			}
			runs.ServeHTTP(w, r)
		case strings.HasSuffix(path, "/events"):
			events.ServeHTTP(w, r)
		default:
			control.ServeHTTP(w, r)
		}
	}), nil
}
