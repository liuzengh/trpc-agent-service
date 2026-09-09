package text

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// The tests in this package drive the consumer through a channel that does not
// exist, on purpose. If anything in here could only be satisfied by WeCom, the
// extraction would not have finished.
const (
	fakeChannel    = channels.ChannelType("fake")
	fakeReplyLimit = 20480
	// fakePrincipal is the one user of the fake channel in these tests.
	fakePrincipal = "p-fake-user"
)

func testIdentity() channels.BindingIdentity {
	return channels.BindingIdentity{
		TenantID:   "tenant-a",
		AgentAppID: "app-a",
		BindingID:  "binding-a",
		Channel:    fakeChannel,
	}
}

// fakeAdapter is a whole platform in thirty lines: it delivers the envelopes a
// test hands it and records what it was asked to send.
type fakeAdapter struct {
	identity channels.BindingIdentity
	limit    int
	inbound  chan channels.InboundEnvelope
	// report is what the next Send answers with. Delivered by default, so a
	// test that cares about delivery says so and no other test has to.
	report channels.DeliveryReport

	mu   sync.Mutex
	sent []channels.OutboundMessage
	// afterAccept runs once accept has returned nil, which by contract means
	// the event is durable. A test uses it to go looking for the row.
	afterAccept func(channels.InboundEnvelope)
}

func newFakeAdapter() *fakeAdapter {
	return &fakeAdapter{
		identity: testIdentity(),
		limit:    fakeReplyLimit,
		inbound:  make(chan channels.InboundEnvelope, 8),
		report:   channels.DeliveryReport{Outcome: channels.Delivered},
	}
}

func (a *fakeAdapter) Identity() channels.BindingIdentity { return a.identity }

func (a *fakeAdapter) ReplyTextLimit() int { return a.limit }

func (a *fakeAdapter) Serve(ctx context.Context, accept channels.AcceptFunc) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case envelope := <-a.inbound:
			if err := accept(ctx, envelope); err != nil {
				return err
			}
			if a.afterAccept != nil {
				a.afterAccept(envelope)
			}
		}
	}
}

func (a *fakeAdapter) Send(
	_ context.Context, _ channels.DeliveryTarget, message channels.OutboundMessage,
) channels.DeliveryReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sent = append(a.sent, message)
	return a.report
}

func (a *fakeAdapter) delivered() []channels.OutboundMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]channels.OutboundMessage(nil), a.sent...)
}

// envelope is one inbound text message on the fake channel.
func (a *fakeAdapter) envelope(externalID, sessionID, text string) channels.InboundEnvelope {
	return channels.InboundEnvelope{
		TenantID:         a.identity.TenantID,
		Channel:          a.identity.Channel,
		ChannelBindingID: a.identity.BindingID,
		AgentAppID:       a.identity.AgentAppID,
		PrincipalID:      fakePrincipal,
		SessionID:        sessionID,
		ExternalEventID:  externalID,
		ReceivedAt:       time.Now().UTC(),
		Message:          channels.InboundMessage{Text: text},
		DeliveryTarget: channels.DeliveryTarget{
			Channel: a.identity.Channel,
			Version: 1,
			Payload: []byte(`{"to":"someone"}`),
		},
	}
}

// orderStore is the delivery side of a Store, enough to drive drain. Every
// other method is the embedded nil interface: a test that reaches one has left
// the path it meant to exercise.
type orderStore struct {
	channels.Store
	parts     map[string]channels.OutboxPart
	runs      map[string]channels.Run
	dispatch  []channels.OutboxDispatch
	failScans int
	claims    []string
	runScans  int
}

func (s *orderStore) ListDispatchableRuns(
	context.Context, channels.DispatchScanRequest,
) ([]channels.RunDispatch, error) {
	s.runScans++
	return nil, nil
}

func (s *orderStore) ListDispatchableOutbox(
	context.Context, channels.DispatchScanRequest,
) ([]channels.OutboxDispatch, error) {
	if s.failScans > 0 {
		s.failScans--
		return nil, errors.New("scan unavailable")
	}
	return s.dispatch, nil
}

func (s *orderStore) GetOutboxPart(
	_ context.Context, _ tenant.TenantContext, outboxID string,
) (channels.OutboxPart, error) {
	return s.parts[outboxID], nil
}

func (s *orderStore) GetRun(
	_ context.Context, _ tenant.TenantContext, runID string,
) (channels.Run, error) {
	return s.runs[runID], nil
}

func (s *orderStore) ClaimOutbox(
	_ context.Context, _ tenant.TenantContext, request channels.ClaimOutboxRequest,
) (channels.OutboxClaim, bool, error) {
	s.claims = append(s.claims, request.OutboxID)
	part := s.parts[request.OutboxID]
	part.SendToken = request.SendToken
	part.Attempt++
	return channels.OutboxClaim{Part: part}, true, nil
}

func (s *orderStore) CompleteOutbox(
	_ context.Context, _ tenant.TenantContext, request channels.CompleteOutboxRequest,
) (channels.OutboxStatus, error) {
	delete(s.parts, request.Token.OutboxID)
	remaining := s.dispatch[:0]
	for _, candidate := range s.dispatch {
		if candidate.OutboxID != request.Token.OutboxID {
			remaining = append(remaining, candidate)
		}
	}
	s.dispatch = remaining
	return channels.OutboxFailed, nil
}

func (s *orderStore) RecoverRuns(
	context.Context, channels.RecoverRequest,
) ([]channels.RunRecovery, error) {
	return nil, nil
}

func (s *orderStore) RecoverOutbox(
	context.Context, channels.RecoverRequest,
) ([]channels.OutboxRecovery, error) {
	return nil, nil
}

// answerPart is one recorded answer of session-s, at accept position sequence.
func answerPart(identity channels.BindingIdentity, outboxID, runID string, sequence int64) (
	channels.OutboxPart, channels.Run, channels.OutboxDispatch,
) {
	part := channels.OutboxPart{
		TenantID:         identity.TenantID,
		OutboxID:         outboxID,
		RunID:            runID,
		Channel:          identity.Channel,
		ChannelBindingID: identity.BindingID,
		SessionID:        "session-s",
		Status:           channels.OutboxPending,
		MaxAttempts:      sendAttempts,
	}
	run := channels.Run{
		TenantID:         identity.TenantID,
		RunID:            runID,
		Channel:          identity.Channel,
		ChannelBindingID: identity.BindingID,
		AgentAppID:       identity.AgentAppID,
		SessionID:        "session-s",
		AcceptSequence:   sequence,
	}
	dispatch := channels.OutboxDispatch{
		OutboxRef: channels.OutboxRef{TenantID: identity.TenantID, OutboxID: outboxID},
		Channel:   identity.Channel,
	}
	return part, run, dispatch
}

func newTestConsumer(t *testing.T, store channels.Store) (*Consumer, *fakeAdapter) {
	t.Helper()
	adapter := newFakeAdapter()
	consumer, err := New(Config{
		Identity:  testIdentity(),
		Adapter:   adapter,
		Store:     store,
		Runs:      &sessionrun.Service{},
		Revisions: func(context.Context, string, string, string) error { return nil },
	})
	require.NoError(t, err)
	return consumer, adapter
}

// A failed send scan must stop the pass, and the answers of one Session must go
// out in the order they were accepted however the scan happens to sort them.
func TestConsumerSendsOneSessionAnswersInAcceptOrder(t *testing.T) {
	identity := testIdentity()
	first, firstRun, firstDispatch := answerPart(identity, "out-a", "run-a", 1)
	second, secondRun, secondDispatch := answerPart(identity, "out-b", "run-b", 2)
	store := &orderStore{
		parts: map[string]channels.OutboxPart{"out-a": first, "out-b": second},
		runs:  map[string]channels.Run{"run-a": firstRun, "run-b": secondRun},
		// Reverse identifier order, which is all the scan promises.
		dispatch:  []channels.OutboxDispatch{secondDispatch, firstDispatch},
		failScans: 1,
	}
	consumer, _ := newTestConsumer(t, store)

	require.NoError(t, consumer.drain(context.Background()))
	require.Empty(t, store.claims)
	require.Zero(t, store.runScans, "a Run must not be executed while the send side is unknown")

	require.NoError(t, consumer.drain(context.Background()))
	require.Equal(t, []string{"out-a", "out-b"}, store.claims)
	require.Equal(t, 1, store.runScans)
}

func TestNewConsumerRequiresItsDependencies(t *testing.T) {
	consumer, err := New(Config{Identity: testIdentity(), Adapter: newFakeAdapter()})
	require.Error(t, err)
	require.Nil(t, consumer)
}

// The configured identity is the trusted one, and the adapter's is a claim. A
// difference in any of the four fields is a consumer that would record one
// conversation and answer a different one.
func TestNewConsumerRejectsAnotherAdaptersBinding(t *testing.T) {
	for _, mismatch := range identityMismatches() {
		t.Run(mismatch.field, func(t *testing.T) {
			adapter := newFakeAdapter()
			adapter.identity = mismatch.identity
			consumer, err := New(Config{
				Identity:  testIdentity(),
				Adapter:   adapter,
				Store:     &orderStore{},
				Runs:      &sessionrun.Service{},
				Revisions: func(context.Context, string, string, string) error { return nil },
			})
			require.ErrorIs(t, err, ErrConfig,
				"adapter and consumer must share the same trusted binding")
			require.Nil(t, consumer)
		})
	}
}

// A reply limit is read once, at construction, and refused there: too small to
// hold its own truncation notice, or larger than the Store will accept.
func TestNewConsumerRejectsAnUnusableReplyLimit(t *testing.T) {
	for _, limit := range []int{0, 1, minReplyTextLimit - 1, channels.MaxMessageTextBytes + 1} {
		adapter := newFakeAdapter()
		adapter.limit = limit
		consumer, err := New(Config{
			Identity:  testIdentity(),
			Adapter:   adapter,
			Store:     &orderStore{},
			Runs:      &sessionrun.Service{},
			Revisions: func(context.Context, string, string, string) error { return nil },
		})
		require.ErrorIs(t, err, ErrConfig)
		require.Nil(t, consumer)
	}
}

// identityMismatches is one wrong value per identity field.
func identityMismatches() []struct {
	field    string
	identity channels.BindingIdentity
} {
	tenantID, appID := testIdentity(), testIdentity()
	bindingID, channel := testIdentity(), testIdentity()
	tenantID.TenantID = "tenant-b"
	appID.AgentAppID = "app-b"
	bindingID.BindingID = "binding-b"
	channel.Channel = channels.ChannelWeCom
	return []struct {
		field    string
		identity channels.BindingIdentity
	}{
		{"tenant", tenantID},
		{"app", appID},
		{"binding", bindingID},
		{"channel", channel},
	}
}

func finalAnswer(text string) *event.Event {
	return &event.Event{Response: &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, Content: text},
		}},
	}}
}

func TestReplyCollectorKeepsOnlyTheFinalAnswer(t *testing.T) {
	var collected replyCollector
	collected.observe(&event.Event{Response: &model.Response{
		Object:    model.ObjectTypeChatCompletionChunk,
		IsPartial: true,
		Choices:   []model.Choice{{Delta: model.Message{Content: "par"}}},
	}})
	collected.observe(&event.Event{Response: &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Choices: []model.Choice{{Message: model.Message{
			Role:      model.RoleAssistant,
			ToolCalls: []model.ToolCall{{ID: "call-1"}},
		}}},
	}})
	collected.observe(&event.Event{Response: &model.Response{
		Object: model.ObjectTypeToolResponse,
		Done:   true,
		Choices: []model.Choice{{Message: model.Message{
			Role:    model.RoleTool,
			ToolID:  "call-1",
			Content: "tool output",
		}}},
	}})
	collected.observe(finalAnswer("the answer"))
	collected.observe(&event.Event{Response: &model.Response{
		Object: model.ObjectTypeRunnerCompletion,
		Done:   true,
	}})
	require.Equal(t, "the answer", collected.answer())
	require.Equal(t, int32(5), collected.events)

	// A failure after the answer still fails the Run.
	collected.observe(&event.Event{Response: &model.Response{
		Object: model.ObjectTypeError,
		Error:  &model.ResponseError{Message: "upstream refused"},
	}})
	require.True(t, collected.failed)
	require.Empty(t, collected.answer())
}

func TestBoundReplyStaysStorableAndSendable(t *testing.T) {
	require.Equal(t, "line\tone\nline two", boundReply("line\tone\x00\nline\x07 two", fakeReplyLimit))

	// Every rune is three bytes, so the cut lands inside one.
	long := strings.Repeat("汉", fakeReplyLimit)
	bounded := boundReply(long, fakeReplyLimit)
	require.LessOrEqual(t, len(bounded), fakeReplyLimit)
	require.True(t, strings.HasSuffix(bounded, truncationNotice))
	require.True(t, strings.HasPrefix(bounded, "汉汉"))
	require.NotContains(t, strings.TrimSuffix(bounded, truncationNotice), "�")
	require.NoError(t, channels.OutboundMessage{Text: bounded}.Validate())
}

// One report, two judgements, and they may never disagree: what the Store
// stores about a send and what an operator is shown about it come from the same
// value, and anything this package does not recognise is unknown rather than
// assumed to have failed safely.
func TestADeliveryReportFailsClosed(t *testing.T) {
	for _, testCase := range []struct {
		report  channels.DeliveryReport
		outcome channels.SendOutcome
		stage   telemetry.Outcome
	}{
		{channels.DeliveryReport{Outcome: channels.Delivered}, channels.SendSucceeded, telemetry.OutcomeSucceeded},
		{channels.DeliveryReport{Outcome: channels.DeliveryTargetStale}, channels.SendPermanent, telemetry.OutcomeStaleTarget},
		{channels.DeliveryReport{Outcome: channels.DeliveryRejected}, channels.SendPermanent, telemetry.OutcomeRejected},
		{channels.DeliveryReport{Outcome: channels.DeliveryUnknown}, channels.SendUnknown, telemetry.OutcomeUnknown},
		// Neither of these is a report this package knows how to store.
		{channels.DeliveryReport{}, channels.SendUnknown, telemetry.OutcomeUnknown},
		{channels.DeliveryReport{Outcome: "invented"}, channels.SendUnknown, telemetry.OutcomeUnknown},
		// Delivered, but with a platform id the Store would refuse. The send is
		// not re-attempted on the strength of an id nobody can record.
		{
			channels.DeliveryReport{
				Outcome:           channels.Delivered,
				ExternalMessageID: strings.Repeat("m", channels.MaxExternalMessageIDBytes+1),
			},
			channels.SendUnknown, telemetry.OutcomeUnknown,
		},
	} {
		result := testCase.report.SendResult()
		require.Equal(t, testCase.outcome, result.Outcome)
		require.Equal(t, testCase.stage, deliverOutcome(result, testCase.report))
		require.NoError(t, result.Validate())
	}
	require.Equal(t, channels.ErrorOutcomeUnknown,
		channels.DeliveryReport{Outcome: channels.DeliveryUnknown}.SendResult().ErrorType)
}
