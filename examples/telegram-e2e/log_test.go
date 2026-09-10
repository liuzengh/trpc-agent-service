package main

import (
	"errors"
	"io"
	"testing"

	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"go.uber.org/zap"
)

func TestConfigureLoggerReturnsFactoryError(t *testing.T) {
	originalLogger := newLogger
	t.Cleanup(func() { newLogger = originalLogger })

	newLogger = func(servicelog.Config) (*zap.Logger, error) {
		return nil, errors.New("logger unavailable")
	}
	if restore, err := configureLogger(io.Discard); err == nil {
		t.Fatal("configureLogger() accepted the factory error")
	} else if restore != nil {
		t.Fatal("configureLogger() returned a restore function on error")
	}
}
