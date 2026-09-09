package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// This file tests the process as a process: what run() assembles, in what
// order, and what it has released by the time it returns.
//
// The unit tests next door assert each piece in isolation. What they cannot
// assert is that main wires those pieces to each other — a Router that borrows
// the store this process opened, a RuntimeResolver that builds Runtimes through
// that Router, and a shutdown that unwinds the two in the one order that does
// not deadlock. Those are properties of runWith, so they are tested through
// runWith.

// runEnv is the environment a demo process boots on: an admin credential,
// because loadDemo has no default for one, and nothing else. The storage
// profile is left unset so the in-memory profile is what gets built, which is
// the only profile that boots with no server to connect to.
func runEnv() func(string) string {
	env := map[string]string{security.AdminAPIKeyEnvVar: runTestAdminKey}
	return func(name string) string { return env[name] }
}

// A process that cannot serve still has to let go of everything it opened.
//
// The listen address here is already bound, so ListenAndServe fails at the last
// possible moment: after storage is open, after the Router and the resolver are
// built, after the HTTP server is assembled. That is the deepest failure the
// startup path has, and it is the one where a missed defer leaks a pool for the
// life of a supervisor's restart loop.
//
// The stub records every constructor and every close, so "released what it
// opened, once, newest first" is an observation rather than an inference from
// the error that came back.
func TestRunReleasesEverythingItOpenedWhenItCannotServe(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { require.NoError(t, occupied.Close()) }()

	stub := &stubStorage{}

	err = runWith(occupied.Addr().String(), runEnv(), stub.deps())

	// The serve failure is what comes back: nothing earlier refused, so the
	// process really did assemble all the way to the listener.
	require.ErrorIs(t, err, syscall.EADDRINUSE)
	require.Equal(t, []string{"new sessions", "new coordinator"}, stub.steps,
		"the process did not reach storage construction")
	require.Equal(t, []string{"coordination", "session service"}, stub.closed,
		"storage was not released newest-first, exactly once")
}

// The whole point of the Lease is that a Runtime holds its storage Bundle for
// its whole life, and this is where that has to not deadlock.
//
// The process here is the real one: real in-memory storage, the real Router
// over it, the real resolver building Runtimes through it, a real chat turn
// served over a real socket. By the time the signal arrives there is a Runtime
// cached with a live lease on the process default Bundle, which is the state a
// serving process is in between requests and the state shutdown has to unwind.
//
// What it proves, in order of how it would fail:
//
//   - main resolves storage through the Router. The turn is found afterwards in
//     the very session service the process opened, so the Bundle the Runtime ran
//     on was that store and not one the Factory built alongside it.
//   - Shutdown terminates. The resolver closes its Runtimes, each releases its
//     lease, and only then can the Router's wait finish; the reverse order
//     blocks forever, so the deadline is the assertion.
//   - The process store is closed once, by the storageStack that owns it. The
//     Router borrowed it and must not have closed it too.
func TestRunServesAChatTurnAndShutsDownOnSignal(t *testing.T) {
	// Registered for the whole test so the signal below cannot kill the test
	// binary in the window before runWith installs its own handler.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	built := make(chan *countingSessions, 1)
	deps := defaultStorageDeps()
	newSessions := deps.newSessions
	deps.newSessions = func(cfg sessionbackend.Config) (session.Service, error) {
		inner, err := newSessions(cfg)
		if err != nil {
			return nil, err
		}
		// Over a channel rather than a captured variable: this runs on the
		// process goroutine and is read from the test's.
		counted := &countingSessions{Service: inner}
		built <- counted
		return counted, nil
	}

	addr := freeLoopbackAddr(t)
	stopped := make(chan error, 1)
	go func() { stopped <- runWith(addr, runEnv(), deps) }()

	waitForListener(t, addr)
	const sessionID = "lifecycle-shutdown-session"
	status, body := chatTurn(t, addr, sessionID, "hello from the lifecycle test")
	require.Equal(t, http.StatusOK, status, body)

	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("runWith did not return after SIGTERM: shutdown blocked, most " +
			"likely on a storage lease a cached Runtime had not released")
	}

	sessions := <-built
	require.Equal(t, 1, sessions.closeCount(),
		"the process session store must be closed exactly once, by the stack that owns it")

	// The turn landed in the store the process opened, so that is the Bundle the
	// Router handed the Runtime. The in-memory service keeps serving reads after
	// Close, which is what makes this observable here rather than before the
	// shutdown it is meant to have survived.
	loaded, err := sessions.GetSession(context.Background(), sessionKeyFor(t, sessionID))
	require.NoError(t, err)
	require.NotNil(t, loaded, "the chat turn was not stored in the process session store")
	require.NotEmpty(t, loaded.Events)
}

// freeLoopbackAddr picks a loopback port that is free right now and gives it
// back. The race with another listener is unavoidable — there is no way to hand
// runWith a socket — but it is a race against this machine's ephemeral range,
// not against anything in this package.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

// waitForListener blocks until the process under test is accepting, or fails the
// test. Nothing else reports that ListenAndServe has bound.
func waitForListener(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			require.NoError(t, conn.Close())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the process never started listening on %s", addr)
}

// chatTurn sends one turn as the demo chat principal and reads the response to
// completion, so the run is over by the time it returns.
func chatTurn(t *testing.T, addr, sessionID, prompt string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body := fmt.Sprintf(`{"model":"ignored","messages":[{"role":"user","content":%q}]}`, prompt)
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, "http://"+addr+"/v1/chat/completions", strings.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	// The demo profile serves chat with the published development key when
	// TRPC_SERVICE_API_KEY is unset, which is how runEnv leaves it.
	request.Header.Set(web.HeaderAuthorization, "Bearer "+security.DevelopmentChatAPIKey)
	request.Header.Set(web.HeaderAgentAppID, platformconfig.DemoAgentAppID)
	request.Header.Set(web.HeaderSessionID, sessionID)

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	payload, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response.StatusCode, string(payload)
}
