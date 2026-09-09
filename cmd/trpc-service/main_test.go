package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The Admin API authenticates, but the process still speaks plain HTTP: a
// credential sent to a non-loopback bind is a credential on the wire in the
// clear. The loopback restriction has to be enforced, not just documented and
// defaulted.
func TestValidateListenAddrAcceptsLoopbackOnly(t *testing.T) {
	for _, addr := range []string{
		"127.0.0.1:8080",
		"127.0.0.1:0",
		"127.9.9.9:8080",
		"localhost:8080",
		"LocalHost:8080",
		"[::1]:8080",
	} {
		t.Run("accepts "+addr, func(t *testing.T) {
			require.NoError(t, validateListenAddr(addr))
		})
	}

	for _, addr := range []string{
		":8080",             // wildcard, the easiest accidental exposure
		"0.0.0.0:8080",      // explicit IPv4 wildcard
		"[::]:8080",         // explicit IPv6 wildcard
		"192.168.1.10:8080", // routable on the local network
		"203.0.113.5:8080",  // routable on the internet
		"example.com:8080",  // a name that is not loopback
		"127.0.0.1",         // no port at all
		"",
	} {
		t.Run("rejects "+addr, func(t *testing.T) {
			require.Error(t, validateListenAddr(addr))
		})
	}

	// The refusal has to name the address and say what is allowed instead.
	err := validateListenAddr("0.0.0.0:8080")
	require.ErrorContains(t, err, "0.0.0.0:8080")
	require.ErrorContains(t, err, "127.0.0.1:8080")
}

// run must refuse before it binds anything, so a non-loopback address never
// reaches http.Server. The address is TEST-NET-1, which is assigned to no local
// interface: if the guard is ever removed, this test fails on the bind error
// instead of publishing the unauthenticated control plane from a test run.
func TestRunRefusesNonLoopbackAddr(t *testing.T) {
	err := run("192.0.2.1:8080")
	require.ErrorContains(t, err, "refusing to listen")
	require.ErrorContains(t, err, "192.0.2.1:8080")
}

func TestShutdownHTTPServerGraceful(t *testing.T) {
	server := &fakeHTTPServerLifecycle{}

	require.NoError(t, shutdownHTTPServer(context.Background(), server))
	require.Equal(t, 1, server.shutdownCalls)
	require.Zero(t, server.closeCalls)
}

func TestShutdownHTTPServerForcesCloseAfterShutdownFailure(t *testing.T) {
	shutdownErr := context.DeadlineExceeded
	closeErr := errors.New("forced close failure")
	server := &fakeHTTPServerLifecycle{
		shutdownErr: shutdownErr,
		closeErr:    closeErr,
	}

	err := shutdownHTTPServer(context.Background(), server)
	require.ErrorIs(t, err, shutdownErr)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, 1, server.shutdownCalls)
	require.Equal(t, 1, server.closeCalls)
}

func TestWaitForStopShutsDownOnSignal(t *testing.T) {
	server := &fakeHTTPServerLifecycle{}
	signalCtx, stop := context.WithCancel(context.Background())
	stop()

	require.NoError(t, waitForStop(signalCtx, make(chan error), nil, server, time.Second))
	require.Equal(t, 1, server.shutdownCalls)
	require.Zero(t, server.closeCalls)
}

func TestWaitForStopForcesCloseWhenGracefulShutdownExpires(t *testing.T) {
	server := &fakeHTTPServerLifecycle{shutdownErr: context.DeadlineExceeded}
	signalCtx, stop := context.WithCancel(context.Background())
	stop()

	err := waitForStop(signalCtx, make(chan error), nil, server, time.Second)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, server.shutdownCalls)
	require.Equal(t, 1, server.closeCalls)
}

func TestWaitForStopForcesCloseAfterServeFailure(t *testing.T) {
	serveErr := errors.New("listen failure")
	server := &fakeHTTPServerLifecycle{}
	serveErrCh := make(chan error, 1)
	serveErrCh <- serveErr

	err := waitForStop(context.Background(), serveErrCh, nil, server, time.Second)
	require.ErrorIs(t, err, serveErr)
	require.Zero(t, server.shutdownCalls)
	require.Equal(t, 1, server.closeCalls)
}

func TestWaitForStopIgnoresServerClosed(t *testing.T) {
	server := &fakeHTTPServerLifecycle{}
	serveErrCh := make(chan error, 1)
	serveErrCh <- http.ErrServerClosed

	require.NoError(t, waitForStop(context.Background(), serveErrCh, nil, server, time.Second))
	require.Zero(t, server.shutdownCalls)
	require.Zero(t, server.closeCalls)
}

type fakeHTTPServerLifecycle struct {
	shutdownErr   error
	closeErr      error
	shutdownCalls int
	closeCalls    int
}

func (s *fakeHTTPServerLifecycle) Shutdown(context.Context) error {
	s.shutdownCalls++
	return s.shutdownErr
}

func (s *fakeHTTPServerLifecycle) Close() error {
	s.closeCalls++
	return s.closeErr
}
