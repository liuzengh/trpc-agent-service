package wecom

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
)

func TestClientDoesNotExposeDialErrorsAfterReconnectExhaustion(t *testing.T) {
	server := newMockServer(t)
	cfg := testConfig(t, server, testBinding())
	cfg.MaxReconnectAttempts = 1
	transportError := errors.New(secretMarker)
	cfg.dial = func(context.Context) (wsConn, error) { return nil, transportError }
	client, err := New(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err = client.Run(ctx)
	require.ErrorIs(t, err, ErrReconnectExhausted)
	requireRedacted(t, err)
	require.False(t, errors.Is(err, transportError), "raw transport errors must not be unwrapped")
}

// The whole accepted path, over a real WebSocket connection to a local server:
// subscribe, receipt, inbound single-chat text, final reply, matching success
// receipt. Every frame is asserted as the protocol defines it, because this is
// the one test that says what this adapter actually speaks.
func TestClientDeliversDirectTextAndConfirmsTheFinalReply(t *testing.T) {
	server := newMockServer(t)
	binding := testBinding()
	run := startClient(t, testConfig(t, server, binding))

	conn := server.accept()
	subscribe := conn.next()
	require.Equal(t, cmdSubscribe, subscribe.Cmd)
	require.True(t, strings.HasPrefix(subscribe.Headers.ReqID, cmdSubscribe+"-"),
		"the subscribe req_id keeps the command prefix")
	require.Greater(t, len(subscribe.Headers.ReqID), len(cmdSubscribe)+8,
		"the subscribe req_id carries random bytes")
	var credentials subscribeBody
	decodeBody(t, subscribe, &credentials)
	require.Equal(t, botIDMarker, credentials.BotID)
	require.Equal(t, secretMarker, credentials.Secret,
		"the Secret is sent in the subscribe body and nowhere else")
	conn.ack(subscribe.Headers.ReqID, 0)

	const callbackReqID = "callback-req-1"
	conn.callback(callbackReqID, textCallback())
	msg := run.receive(t)

	// Tenant, app and binding come from the static binding; identity is
	// derived, never echoed.
	require.Equal(t, binding.TenantID, msg.TenantID)
	require.Equal(t, binding.AgentAppID, msg.AgentAppID)
	require.Equal(t, binding.BindingID, msg.BindingID)
	require.Equal(t,
		principalID(binding.TenantID, binding.BindingID, userIDMarker), msg.PrincipalID)
	require.Equal(t,
		directSessionID(binding.TenantID, binding.AgentAppID, binding.BindingID, msg.PrincipalID),
		msg.SessionID)
	require.NotContains(t, msg.SessionID, userIDMarker)
	require.NotContains(t, msg.PrincipalID, userIDMarker)
	// The external message id is preserved verbatim: deduplication happens
	// downstream, on this value, and a rewritten id would break it.
	require.Equal(t, msgIDMarker, msg.ExternalMessageID)
	require.Equal(t, bodyMarker, msg.Text)
	require.False(t, msg.ReceivedAt.IsZero())

	replied := make(chan error, 1)
	go func() { replied <- run.client.SendFinalText(context.Background(), msg.Reply, replyMarker) }()

	respond := conn.next()
	require.Equal(t, cmdRespond, respond.Cmd)
	require.Equal(t, callbackReqID, respond.Headers.ReqID,
		"the reply is correlated by the callback req_id")
	var reply respondBody
	decodeBody(t, respond, &reply)
	require.Equal(t, msgTypeStream, reply.MsgType)
	require.Equal(t, streamID(binding.TenantID, binding.BindingID, msgIDMarker), reply.Stream.ID)
	require.Equal(t, replyMarker, reply.Stream.Content)
	require.True(t, reply.Stream.Finish)
	conn.ack(respond.Headers.ReqID, 0)
	require.NoError(t, <-replied)

	// One message is answered once. A second final stream on the same req_id
	// would be a second answer, not a retry, and it never reaches the wire.
	err := run.client.SendFinalText(context.Background(), msg.Reply, replyMarker)
	require.ErrorIs(t, err, ErrReplyAlreadySent)
	conn.silent(60 * time.Millisecond)

	require.ErrorIs(t, run.stop(t), context.Canceled)
}

// A rejected credential is terminal. Redialing would repeat the same rejection
// with the same Secret, and repeated failed authentication is how an account
// gets locked out.
func TestClientStopsWhenTheSubscribeIsRejected(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))

	conn := server.accept()
	subscribe := conn.next()
	conn.ack(subscribe.Headers.ReqID, 40001)

	err := run.wait(t)
	require.ErrorIs(t, err, ErrAuthRejected)
	requireRedacted(t, err)
	require.NotContains(t, err.Error(), "40001",
		"the platform errcode is not propagated either")
	server.idle(80 * time.Millisecond)
}

// A receipt without an explicit errcode is not an acknowledgement, so the
// connection never authenticates and the callbacks that follow it are not
// served. The client treats it as an ordinary connection failure and
// reconnects.
func TestClientRefusesAReceiptWithoutAnExplicitErrcode(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))

	conn := server.accept()
	subscribe := conn.next()
	conn.ackWithoutErrcode(subscribe.Headers.ReqID)
	conn.callback("callback-before-auth", textCallback())

	run.noMessage(t, 120*time.Millisecond)
	// The subscribe ack times out, the connection ends, and a new one starts.
	server.accept()
	require.ErrorIs(t, run.stop(t), context.Canceled)
}

// disconnected_event means a second client has taken this bot over.
// Reconnecting would take it back and start a fight that drops messages on
// both sides, so the client stops instead.
func TestClientStopsOnTakeover(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))

	conn := server.authenticate()
	conn.event(eventDisconnected)

	err := run.wait(t)
	require.ErrorIs(t, err, ErrTakenOver)
	requireRedacted(t, err)
	server.idle(80 * time.Millisecond)
}

// A negative receipt is a definite failure and is reported as one. It is not
// an unknown outcome, and it does not cost the connection: the next message on
// the same connection is still served.
func TestReplyRejectionIsNotSuccessAndKeepsTheConnection(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))
	conn := server.authenticate()

	conn.callback("callback-req-1", textCallback())
	msg := run.receive(t)
	replied := make(chan error, 1)
	go func() { replied <- run.client.SendFinalText(context.Background(), msg.Reply, replyMarker) }()
	respond := conn.next()
	conn.ack(respond.Headers.ReqID, 45009)

	err := <-replied
	require.ErrorIs(t, err, ErrReplyRejected)
	require.NotErrorIs(t, err, ErrReplyOutcomeUnknown)
	requireRedacted(t, err)

	second := textCallback()
	second["msgid"] = msgIDMarker + "-2"
	conn.callback("callback-req-2", second)
	require.Equal(t, msgIDMarker+"-2", run.receive(t).ExternalMessageID)
	require.ErrorIs(t, run.stop(t), context.Canceled)
}

// A reply that is written and never receipted is neither success nor failure.
// The connection is retired so a late receipt cannot be read as the answer to
// something else, the client reconnects, and the old target — which addresses
// a req_id the platform correlated on a connection that no longer exists — is
// refused. Nothing is resent on its own.
func TestUnknownReplyOutcomeRetiresTheConnectionAndExpiresItsTargets(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))
	conn := server.authenticate()

	conn.callback("callback-req-1", textCallback())
	msg := run.receive(t)

	err := run.client.SendFinalText(context.Background(), msg.Reply, replyMarker)
	require.ErrorIs(t, err, ErrReplyOutcomeUnknown)
	requireRedacted(t, err)

	reconnected := server.accept()
	subscribe := reconnected.next()
	reconnected.ack(subscribe.Headers.ReqID, 0)
	// The heartbeat only starts once the connection is authenticated, so one
	// ping is the signal that this connection is the live one.
	reconnected.waitPings(1)

	err = run.client.SendFinalText(context.Background(), msg.Reply, replyMarker)
	require.ErrorIs(t, err, ErrReplyTargetExpired)
	// No automatic resend: the new connection carries heartbeats and nothing
	// else.
	reconnected.silent(80 * time.Millisecond)
	require.ErrorIs(t, run.stop(t), context.Canceled)
}

// Heartbeats keep the connection, and losing their receipts ends it. Both
// halves matter: a client that never pinged would sit on a half-open socket,
// and one that dropped the connection on a single missed beat would reconnect
// on noise.
func TestHeartbeatKeepsTheConnectionAndItsLossReconnects(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))
	conn := server.authenticate()

	conn.waitPings(3)
	conn.callback("callback-req-1", textCallback())
	require.Equal(t, bodyMarker, run.receive(t).Text,
		"the connection is still serving after several heartbeats")

	conn.dropPings.Store(true)
	reconnected := server.accept()
	subscribe := reconnected.next()
	require.Equal(t, cmdSubscribe, subscribe.Cmd,
		"a reconnect subscribes again rather than resuming")
	require.ErrorIs(t, run.stop(t), context.Canceled)
}

// Cancelling the context stops the client promptly and releases everything
// waiting on the connection: the reader, the heartbeat and a reply still
// waiting for its receipt. Run only returns after joining its goroutines, so
// a Run that returns is also the assertion that none was left behind.
func TestShutdownReleasesEveryWaiter(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))
	conn := server.authenticate()

	conn.callback("callback-req-1", textCallback())
	msg := run.receive(t)
	replied := make(chan error, 1)
	go func() { replied <- run.client.SendFinalText(context.Background(), msg.Reply, replyMarker) }()
	conn.next() // the reply is on the wire and will never be receipted.

	started := time.Now()
	require.ErrorIs(t, run.stop(t), context.Canceled)
	require.Less(t, time.Since(started), testTimeout,
		"shutdown waited for a deadline instead of releasing its waiters")

	select {
	case err := <-replied:
		require.ErrorIs(t, err, ErrReplyOutcomeUnknown)
	case <-time.After(testTimeout):
		t.Fatal("the pending reply was never released")
	}
}

// One binding is one connection. A second Run would subscribe with the same
// credential and have the platform disconnect the first.
func TestRunRefusesASecondConcurrentRun(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))
	server.authenticate()

	require.ErrorIs(t, run.client.Run(context.Background()), ErrRunning)
	require.ErrorIs(t, run.stop(t), context.Canceled)
}

// A callback addressed to another bot is not this binding's message. It is
// dropped rather than routed, and dropping it costs neither the connection nor
// the next message.
func TestCallbackForAnotherBotIsNotDelivered(t *testing.T) {
	server := newMockServer(t)
	run := startClient(t, testConfig(t, server, testBinding()))
	conn := server.authenticate()

	foreign := textCallback()
	foreign["aibotid"] = "another-" + botIDMarker
	conn.callback("callback-req-1", foreign)
	run.noMessage(t, 80*time.Millisecond)

	conn.callback("callback-req-2", textCallback())
	require.Equal(t, bodyMarker, run.receive(t).Text)
	require.ErrorIs(t, run.stop(t), context.Canceled)
}

// Two bindings that see the same external user produce different principals
// and different sessions, end to end. Identity is scoped by the binding it
// arrived through, so one tenant's user id can never resolve into another
// tenant's conversation.
func TestBindingsIsolateIdentityEndToEnd(t *testing.T) {
	server := newMockServer(t)
	first := testBinding()
	second := Binding{
		TenantID:   "tenant-b",
		AgentAppID: "app-b",
		BindingID:  "binding-b",
		BotID:      botIDMarker,
		SecretRef:  first.SecretRef,
	}

	runFirst := startClient(t, testConfig(t, server, first))
	firstConn := server.authenticate()
	firstConn.callback("callback-req-1", textCallback())
	firstMsg := runFirst.receive(t)

	runSecond := startClient(t, testConfig(t, server, second))
	secondConn := server.authenticate()
	secondConn.callback("callback-req-1", textCallback())
	secondMsg := runSecond.receive(t)

	require.Equal(t, firstMsg.ExternalMessageID, secondMsg.ExternalMessageID,
		"the same external message reached both bindings")
	require.NotEqual(t, firstMsg.PrincipalID, secondMsg.PrincipalID)
	require.NotEqual(t, firstMsg.SessionID, secondMsg.SessionID)
	require.Equal(t, "tenant-a", firstMsg.TenantID)
	require.Equal(t, "tenant-b", secondMsg.TenantID)
	require.ErrorIs(t, runFirst.stop(t), context.Canceled)
	require.ErrorIs(t, runSecond.stop(t), context.Canceled)
}

// The entitlement check runs before the reference is resolved. A tenant that
// may not name a variable must not learn whether it is set, so a denied
// binding never reaches the environment at all.
func TestNewAuthorizesTheSecretReferenceBeforeResolvingIt(t *testing.T) {
	binding := testBinding()
	lookups := 0
	_, err := New(Config{
		Binding:    binding,
		Authorizer: security.DenyCapabilities(),
		Getenv: func(string) string {
			lookups++
			return secretMarker
		},
	})
	require.ErrorIs(t, err, security.ErrNotEntitled)
	require.Zero(t, lookups, "a denied reference was resolved anyway")
	requireRedacted(t, err)
}

// The remaining refusals New owes a caller: no authorizer at all, and an
// entitled reference whose variable is not set.
func TestNewRefusesIncompleteConfiguration(t *testing.T) {
	binding := testBinding()
	_, err := New(Config{Binding: binding})
	require.ErrorIs(t, err, ErrConfig)

	_, err = New(Config{
		Binding:    binding,
		Authorizer: testAuthorizer(t, binding),
		Getenv:     func(string) string { return "" },
	})
	require.ErrorIs(t, err, ErrConfig)
	requireRedacted(t, err)

	_, err = New(Config{
		Binding:    Binding{},
		Authorizer: testAuthorizer(t, binding),
	})
	require.ErrorIs(t, err, ErrConfig)
}

// A reply needs a live connection and a target from it. Neither is something
// a caller can supply on its own.
func TestReplyWithoutAConnectionIsRefused(t *testing.T) {
	binding := testBinding()
	client, err := New(Config{
		Binding:    binding,
		Authorizer: testAuthorizer(t, binding),
		Getenv:     func(string) string { return secretMarker },
	})
	require.NoError(t, err)

	err = client.SendFinalText(context.Background(), ReplyTarget{}, replyMarker)
	require.ErrorIs(t, err, ErrNotConnected)

	// Text is validated before anything looks for a connection, so an
	// oversized reply is refused without a socket being involved.
	err = client.SendFinalText(context.Background(), ReplyTarget{},
		strings.Repeat("a", maxReplyTextBytes+1))
	require.ErrorIs(t, err, ErrTextTooLong)
	require.False(t, errors.Is(err, ErrNotConnected))
}

// The platform may send a callback immediately behind the subscribe receipt.
// A connection therefore becomes authenticated in the reader, as that receipt
// is delivered, and not on the goroutine waiting for it: otherwise the reader
// could dispatch — and drop as pre-authentication — a message that arrived
// after the platform had already accepted the subscribe.
func TestSubscribeReceiptAuthenticatesInTheReader(t *testing.T) {
	errcode := func(v int) *int { return &v }

	conn := newConnection(nil, "generation-1", testTimeout)
	waiter, ok := conn.expectSubscribe("subscribe-1")
	require.True(t, ok)
	require.False(t, conn.authenticated())

	// A receipt for another frame authenticates nothing.
	conn.deliverAck(frame{Headers: headers{ReqID: "ping-1"}, ErrCode: errcode(0)})
	require.False(t, conn.authenticated())

	conn.deliverAck(frame{Headers: headers{ReqID: "subscribe-1"}, ErrCode: errcode(0)})
	require.True(t, conn.authenticated(),
		"the connection is authenticated before the waiter is even read")
	require.True(t, (<-waiter).ok())

	// A refused or unreadable subscribe receipt authenticates nothing either.
	for _, code := range []*int{errcode(40001), nil} {
		refused := newConnection(nil, "generation-2", testTimeout)
		_, ok := refused.expectSubscribe("subscribe-2")
		require.True(t, ok)
		refused.deliverAck(frame{Headers: headers{ReqID: "subscribe-2"}, ErrCode: code})
		require.False(t, refused.authenticated())
	}
}
