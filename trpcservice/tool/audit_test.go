package tool_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/stretchr/testify/require"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// Payload that must never reach the trail. Each value is distinctive so a
// rendered log line can be searched for it.
const (
	secretArguments = `{"text":"sk-secret-argument-value"}`
	secretResult    = "sk-secret-result-value"
	secretErrorText = "sk-secret-error-text"
)

// The trail records identity from the platform RunContext, and only from
// there: nothing in it comes from the model, the tool, or the request body.
func TestAuditCallbacksRecordTrustedScope(t *testing.T) {
	sink := &recordingSink{}
	callbacks := servicetool.NewAuditCallbacks(sink)
	ctx := runContextFor(t, context.Background())

	after := runToolCallbacks(t, callbacks, ctx, "call-1", servicetool.RefAdd, nil)

	events := sink.events()
	require.Len(t, events, 2)

	expectedScope := servicetool.AuditScope{
		RequestID:   "request-1",
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: "principal-1",
		SessionID:   "conversation-1",
		RevisionID:  "revision-1",
	}
	require.Equal(t, servicetool.AuditPhaseBefore, events[0].Phase)
	require.Equal(t, servicetool.AuditPhaseAfter, events[1].Phase)
	for _, event := range events {
		require.True(t, event.ScopeValid)
		require.Equal(t, expectedScope, event.Scope)
		require.Equal(t, servicetool.RefAdd, event.ToolName)
		require.Equal(t, "call-1", event.ToolCallID)
	}
	require.True(t, events[1].Succeeded)
	require.True(t, events[1].DurationValid)
	require.Positive(t, events[1].Duration)

	// The after callback must not replace the tool result.
	require.NotNil(t, after)
	require.Nil(t, after.CustomResult)
	require.False(t, after.SkipSummarization)
}

// Two turns of one conversation are two requests, and the trail has to tell
// them apart. The request id is read from each run's own scope, so it varies
// where everything else about the two runs — tenant, app, principal, session,
// revision — is identical. Without it the trail could say which conversation ran
// a tool but never which request did, and a caller holding an X-Request-ID would
// have nothing to look the tool calls up by.
func TestAuditRecordsThePerRequestID(t *testing.T) {
	sink := &recordingSink{}
	callbacks := servicetool.NewAuditCallbacks(sink)

	for _, requestID := range []string{"request-1", "request-2"} {
		ctx, err := identity.WithRunContext(context.Background(), identity.RunContext{
			RequestID:   requestID,
			TenantID:    "tenant-a",
			AppID:       "assistant",
			PrincipalID: "principal-1",
			SessionID:   "conversation-1",
			RevisionID:  "revision-1",
		})
		require.NoError(t, err)
		runToolCallbacks(t, callbacks, ctx, "call-"+requestID, servicetool.RefAdd, nil)
	}

	events := sink.events()
	require.Len(t, events, 4)
	recorded := make([]string, 0, len(events))
	for _, event := range events {
		require.True(t, event.ScopeValid)
		require.Equal(t, "conversation-1", event.Scope.SessionID)
		require.Equal(t, "revision-1", event.Scope.RevisionID)
		recorded = append(recorded, event.Scope.RequestID)
	}
	// Before and after of each call, in order: the id belongs to the run, not to
	// the phase or to the tool.
	require.Equal(t,
		[]string{"request-1", "request-1", "request-2", "request-2"}, recorded)
}

// The audit trail observes a tool call; it must not become part of it.
//
// This is asserted on the value the framework actually consumes, because the
// framework substitutes the tool result with any CustomResult a callback
// leaves behind — including one it synthesizes itself when every callback
// returns nil. A trail that answered nil would look correct here and still
// route every tool result through a replacement path it has no business in.
func TestAuditCallbacksReplaceNothing(t *testing.T) {
	callbacks := servicetool.NewAuditCallbacks(&recordingSink{})
	ctx := runContextFor(t, context.Background())

	before, err := callbacks.RunBeforeTool(ctx, &agenttool.BeforeToolArgs{
		ToolCallID: "call-1",
		ToolName:   servicetool.RefAdd,
		Arguments:  []byte(secretArguments),
	})
	require.NoError(t, err)
	require.NotNil(t, before)
	require.Nil(t, before.CustomResult, "the tool must still run")
	require.Nil(t, before.ModifiedArguments, "the tool must run on what the model sent")
	require.NotNil(t, before.Context, "the start time has to reach the after callback")

	for name, args := range map[string]*agenttool.AfterToolArgs{
		"successful call": {
			ToolCallID: "call-1", ToolName: servicetool.RefAdd,
			Result: servicetool.AddOutput{Sum: 5},
		},
		"failed call": {
			ToolCallID: "call-1", ToolName: servicetool.RefAdd,
			Error: errors.New(secretErrorText),
		},
	} {
		t.Run(name, func(t *testing.T) {
			after, err := callbacks.RunAfterTool(before.Context, args)
			require.NoError(t, err)
			require.NotNil(t, after)
			// A CustomResult here would replace the tool result, and on a
			// failed call it would additionally clear the tool error.
			require.Nil(t, after.CustomResult)
			require.False(t, after.SkipSummarization)
		})
	}
}

// The audit trail is written to a log that has none of the retention and
// access rules conversation content has, so the event type itself must have no
// field that could hold conversation content. This enumerates the contract: a
// new field has to be added here deliberately, and an Arguments or Result field
// fails the test rather than quietly shipping.
func TestAuditEventCarriesNoPayloadFields(t *testing.T) {
	allowed := map[string]struct{}{
		"Phase": {}, "Scope": {}, "ScopeValid": {}, "ToolName": {},
		"ToolCallID": {}, "Succeeded": {}, "Duration": {}, "DurationValid": {},
	}
	eventType := reflect.TypeOf(servicetool.AuditEvent{})
	require.Equal(t, len(allowed), eventType.NumField())
	for index := range eventType.NumField() {
		require.Contains(t, allowed, eventType.Field(index).Name)
	}

	allowedScope := []string{
		"RequestID", "TenantID", "AppID", "PrincipalID", "SessionID", "RevisionID",
	}
	scopeType := reflect.TypeOf(servicetool.AuditScope{})
	scopeFields := make([]string, 0, scopeType.NumField())
	for index := range scopeType.NumField() {
		scopeFields = append(scopeFields, scopeType.Field(index).Name)
	}
	require.ElementsMatch(t, allowedScope, scopeFields)
}

// The end of the same argument, at the other end of the pipe: whatever the
// framework hands the callbacks, none of it may appear in what is written.
func TestAuditOutputNeverContainsPayload(t *testing.T) {
	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	callbacks := servicetool.NewAuditCallbacks(servicetool.NewSlogAuditSink(logger))
	ctx := runContextFor(t, context.Background())

	runToolCallbacks(t, callbacks, ctx, "call-1", servicetool.RefEcho, errors.New(secretErrorText))

	output := logged.String()
	// The trail happened, and it identifies the run.
	require.Contains(t, output, servicetool.RefEcho)
	require.Contains(t, output, "call-1")
	require.Contains(t, output, "request_id=request-1")
	require.Contains(t, output, "tenant-a")
	require.Contains(t, output, "principal-1")
	require.Contains(t, output, "revision-1")
	require.Contains(t, output, "phase=before")
	require.Contains(t, output, "phase=after")
	require.Contains(t, output, "scope_valid=true")
	// A failed call is recorded as failed, and not as why.
	require.Contains(t, output, "success=false")
	require.Contains(t, output, "duration_ms=")
	require.NotContains(t, output, secretArguments)
	require.NotContains(t, output, secretResult)
	require.NotContains(t, output, secretErrorText)
	require.NotContains(t, output, "sk-secret")
}

// A tool call that arrives without a trusted scope is recorded as unscoped.
// The audit layer must not be the thing that stops it: refusing an untrusted
// run is contextRunner's job, and a tool that reached this point some other way
// is exactly what the trail should show rather than suppress.
func TestAuditWithoutRunContextRecordsInvalidScope(t *testing.T) {
	sink := &recordingSink{}
	callbacks := servicetool.NewAuditCallbacks(sink)

	runToolCallbacks(t, callbacks, context.Background(), "call-1", servicetool.RefEcho, nil)

	events := sink.events()
	require.Len(t, events, 2)
	for _, event := range events {
		require.False(t, event.ScopeValid)
		require.Equal(t, servicetool.AuditScope{}, event.Scope)
		require.Equal(t, servicetool.RefEcho, event.ToolName)
	}
	require.True(t, events[1].Succeeded)
}

// The framework turns a callback error — and a callback panic — into a tool
// failure. A defective sink is therefore capable of taking down tool execution,
// so it is contained here: the trail may lose a record, the request may not
// lose a tool.
func TestAuditNeverFailsTheToolCall(t *testing.T) {
	callbacks := servicetool.NewAuditCallbacks(panickingSink{})
	ctx := runContextFor(t, context.Background())

	before, err := callbacks.RunBeforeTool(ctx, &agenttool.BeforeToolArgs{
		ToolCallID: "call-1",
		ToolName:   servicetool.RefEcho,
		Arguments:  []byte(secretArguments),
	})
	require.NoError(t, err)
	require.NotNil(t, before)
	// Execution continues: no replacement result, no rewritten arguments.
	require.Nil(t, before.CustomResult)
	require.Nil(t, before.ModifiedArguments)
	require.NotNil(t, before.Context)

	after, err := callbacks.RunAfterTool(before.Context, &agenttool.AfterToolArgs{
		ToolCallID: "call-1",
		ToolName:   servicetool.RefEcho,
		Result:     secretResult,
	})
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Nil(t, after.CustomResult)
}

// Defensive: the framework always passes both, but a nil here must not be the
// thing that fails a tool call either.
func TestAuditToleratesMissingCallbackArguments(t *testing.T) {
	callbacks := servicetool.NewAuditCallbacks(&recordingSink{})

	before, err := callbacks.RunBeforeTool(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, before)

	after, err := callbacks.RunAfterTool(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Nil(t, after.CustomResult)
}

// A model may call the same tool more than once in one turn, and the framework
// may run those calls at the same time. Correlation is therefore by tool call
// ID, carried on the per-call context — never by tool name, which would pair a
// before with the wrong after as soon as a name repeats.
//
// Both calls below use the same tool name and take deliberately different
// amounts of time, so a name-keyed implementation reports the wrong durations.
func TestAuditCorrelatesConcurrentCallsOfOneTool(t *testing.T) {
	sink := &recordingSink{}
	callbacks := servicetool.NewAuditCallbacks(sink)
	ctx := runContextFor(t, context.Background())

	calls := map[string]time.Duration{
		"call-slow": 120 * time.Millisecond,
		"call-fast": 0,
	}
	var group sync.WaitGroup
	for callID, work := range calls {
		group.Add(1)
		go func() {
			defer group.Done()
			runToolCallbacks(t, callbacks, ctx, callID, servicetool.RefEcho, nil, work)
		}()
	}
	group.Wait()

	measured := map[string]time.Duration{}
	for _, event := range sink.events() {
		if event.Phase != servicetool.AuditPhaseAfter {
			continue
		}
		require.True(t, event.DurationValid)
		measured[event.ToolCallID] = event.Duration
	}
	require.Len(t, measured, len(calls))
	require.GreaterOrEqual(t, measured["call-slow"], calls["call-slow"])
	// The fast call did no work, so a name-keyed store would have handed it the
	// slow call's start time.
	require.Less(t, measured["call-fast"], calls["call-slow"])
}

// A path that did not thread the before callback's context reports the
// duration as unmeasured rather than as zero, which would read as an instant
// call.
func TestAuditReportsUnmeasuredDurationWithoutStart(t *testing.T) {
	sink := &recordingSink{}
	callbacks := servicetool.NewAuditCallbacks(sink)

	_, err := callbacks.RunAfterTool(context.Background(), &agenttool.AfterToolArgs{
		ToolCallID: "call-1",
		ToolName:   servicetool.RefEcho,
	})
	require.NoError(t, err)

	events := sink.events()
	require.Len(t, events, 1)
	require.False(t, events[0].DurationValid)
	require.Zero(t, events[0].Duration)
}

// The production default writes to slog without any of this depending on the
// process-wide logger being replaced.
func TestSlogAuditSinkUsesTheInjectedLogger(t *testing.T) {
	var logged bytes.Buffer
	sink := servicetool.NewSlogAuditSink(slog.New(slog.NewTextHandler(&logged, nil)))

	sink.Record(context.Background(), servicetool.AuditEvent{
		Phase:      servicetool.AuditPhaseBefore,
		ToolName:   servicetool.RefAdd,
		ToolCallID: "call-1",
	})

	require.Contains(t, logged.String(), "tool call")
	require.Contains(t, logged.String(), "scope_valid=false")
	// A before event carries no outcome, so it must not report one.
	require.NotContains(t, logged.String(), "success=")
	require.NotContains(t, logged.String(), "duration_ms=")
}

func TestSlogAuditSinkFallsBackToDefaultLogger(t *testing.T) {
	sink := servicetool.NewSlogAuditSink(nil)
	require.NotPanics(t, func() {
		sink.Record(context.Background(), servicetool.AuditEvent{Phase: servicetool.AuditPhaseAfter})
	})
}

// runToolCallbacks drives one tool call the way the framework does: the
// context returned by the before callback is threaded through the work and into
// the after callback.
func runToolCallbacks(
	t *testing.T,
	callbacks *agenttool.Callbacks,
	ctx context.Context,
	toolCallID string,
	toolName string,
	toolErr error,
	work ...time.Duration,
) *agenttool.AfterToolResult {
	t.Helper()
	before, err := callbacks.RunBeforeTool(ctx, &agenttool.BeforeToolArgs{
		ToolCallID: toolCallID,
		ToolName:   toolName,
		Arguments:  []byte(secretArguments),
	})
	require.NoError(t, err)
	require.NotNil(t, before)
	require.NotNil(t, before.Context)
	for _, duration := range work {
		time.Sleep(duration)
	}
	after, err := callbacks.RunAfterTool(before.Context, &agenttool.AfterToolArgs{
		ToolCallID: toolCallID,
		ToolName:   toolName,
		Arguments:  []byte(secretArguments),
		Result:     secretResult,
		Error:      toolErr,
	})
	require.NoError(t, err)
	return after
}

func runContextFor(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	scoped, err := identity.WithRunContext(ctx, identity.RunContext{
		RequestID:   "request-1",
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: "principal-1",
		SessionID:   "conversation-1",
		RevisionID:  "revision-1",
	})
	require.NoError(t, err)
	return scoped
}

type recordingSink struct {
	mu       sync.Mutex
	recorded []servicetool.AuditEvent
}

func (s *recordingSink) Record(_ context.Context, event servicetool.AuditEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recorded = append(s.recorded, event)
}

func (s *recordingSink) events() []servicetool.AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]servicetool.AuditEvent(nil), s.recorded...)
}

type panickingSink struct{}

func (panickingSink) Record(context.Context, servicetool.AuditEvent) {
	panic("sink is broken")
}
