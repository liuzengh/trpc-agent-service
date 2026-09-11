package trpcagent

import (
	"context"
	"os"

	sdklog "trpc.group/trpc-go/trpc-agent-go/log"
)

// BindLogging removes SDK free-text logging at its source. Error-path SDK logs
// include tool arguments and entire events independently of SpanAttributePolicy.
// The sink receives only a fixed level, never the original format or arguments.
// Bootstrap owns this process-wide bridge; restoration is for quiescent tests.
func BindLogging(sink func(context.Context, string)) func() {
	old, oldContext := sdklog.Default, sdklog.ContextDefault
	logger := sdkLogger{sink: sink}
	sdklog.Default, sdklog.ContextDefault = logger, logger
	return func() { sdklog.Default, sdklog.ContextDefault = old, oldContext }
}

type sdkLogger struct{ sink func(context.Context, string) }

func (l sdkLogger) emit(level string) {
	if l.sink != nil {
		l.sink(context.Background(), level)
	}
}
func (sdkLogger) Debug(...any)            {}
func (sdkLogger) Debugf(string, ...any)   {}
func (l sdkLogger) Info(...any)           { l.emit("info") }
func (l sdkLogger) Infof(string, ...any)  { l.emit("info") }
func (l sdkLogger) Warn(...any)           { l.emit("warn") }
func (l sdkLogger) Warnf(string, ...any)  { l.emit("warn") }
func (l sdkLogger) Error(...any)          { l.emit("error") }
func (l sdkLogger) Errorf(string, ...any) { l.emit("error") }
func (l sdkLogger) Fatal(...any)          { l.emit("fatal"); os.Exit(1) }
func (l sdkLogger) Fatalf(string, ...any) { l.emit("fatal"); os.Exit(1) }
