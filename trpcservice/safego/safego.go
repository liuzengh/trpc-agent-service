package safego

import (
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
)

// PanicError reports a recovered panic at an asynchronous execution boundary.
type PanicError struct {
	Component string
	Value     any
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("%s panic: %v", e.Component, e.Value)
}

// Run executes fn and converts a panic into an observable error. Callers that
// supervise the goroutine can propagate the returned error through their
// existing lifecycle/error channel instead of terminating the process.
func Run(component string, fn func()) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			stack := debug.Stack()
			slog.Error("goroutine panic recovered",
				"component", component,
				"panic", fmt.Sprint(recovered),
				"stack", string(stack),
			)
			err = &PanicError{Component: component, Value: recovered}
		}
	}()
	fn()
	return nil
}

// Go starts a best-effort asynchronous task whose panic must not terminate the
// process. Prefer Run when the caller has an error/lifecycle channel.
func Go(component string, fn func()) {
	go func() { _ = Run(component, fn) }()
}

// RecoverHTTP keeps a handler panic inside the current request boundary. The
// stack is logged server-side while the client receives a generic 500 response.
func RecoverHTTP(next http.Handler) http.Handler {
	if next == nil {
		next = http.DefaultServeMux
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered == http.ErrAbortHandler {
					panic(recovered)
				}
				slog.Error("http handler panic recovered",
					"method", request.Method,
					"path", request.URL.Path,
					"panic", fmt.Sprint(recovered),
					"stack", string(debug.Stack()),
				)
				http.Error(writer, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(writer, request)
	})
}

// HTTPErrorLog routes net/http server diagnostics through the structured
// application logger instead of the process-global standard logger.
func HTTPErrorLog() *log.Logger {
	return log.New(httpErrorWriter{}, "", 0)
}

type httpErrorWriter struct{}

func (httpErrorWriter) Write(payload []byte) (int, error) {
	message := strings.TrimSpace(string(payload))
	if message != "" {
		slog.Error("http server error", "message", message)
	}
	return len(payload), nil
}
