// Package worker executes trusted envelopes against shared service ports.
package worker

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

// DeterministicTestModel is the narrow model seam used by unit tests that do
// not exercise tRPC-Agent-Go Runner behavior.
type DeterministicTestModel interface {
	Generate(context.Context, runtime.ExecutionEnvelope, profile.ExecutionProfileSnapshot) (string, error)
}

// DeterministicTestExecutor is test scaffolding for Dispatcher/storage
// contracts. Runtime and integration paths must use RunnerExecutor.
type DeterministicTestExecutor struct {
	Tasks    gateway.TaskStore
	Profiles profile.ExecutionProfileResolver
	Sessions sessionstore.AtomicSessionStore
	Model    DeterministicTestModel
}

func (w DeterministicTestExecutor) Execute(ctx context.Context, envelope runtime.ExecutionEnvelope) error {
	return w.ExecuteWithFence(ctx, envelope, 1)
}

func (w DeterministicTestExecutor) ExecuteWithFence(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64) error {
	return w.ExecuteWithLease(ctx, envelope, fence, nil)
}

func (w DeterministicTestExecutor) ExecuteWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, beforeCommit func(context.Context) error) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	if fence == 0 {
		return runtime.ErrStaleFence
	}
	authoritative, err := w.Tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
	if err != nil {
		return err
	}
	trusted := authoritative.Envelope
	if trusted.TenantID != envelope.TenantID || trusted.AgentAppID != envelope.AgentAppID ||
		trusted.RequestID != envelope.RequestID || trusted.SessionID != envelope.SessionID ||
		trusted.UserID != envelope.UserID || trusted.Channel != envelope.Channel {
		return runtime.ErrTenantScope
	}
	trustedCreatedAt, deliveredCreatedAt := trusted.CreatedAt, envelope.CreatedAt
	trusted.CreatedAt, envelope.CreatedAt = time.Time{}, time.Time{}
	if trusted != envelope || !trustedCreatedAt.Equal(deliveredCreatedAt) {
		return runtime.ErrVersionMismatch
	}
	key := profile.ExecutionProfileKey{TenantID: envelope.TenantID, TenantVersion: envelope.TenantVersion, AgentAppID: envelope.AgentAppID, AgentAppVersion: envelope.AgentAppVersion, AgentAppRevision: envelope.AgentAppRevision, ContentDigest: envelope.AgentContentDigest, ConfigVersion: envelope.ConfigVersion, PolicyVersion: envelope.PolicyVersion}
	snapshot, err := w.Profiles.Resolve(ctx, key)
	if err != nil {
		return err
	}
	if snapshot.TenantVersion != envelope.TenantVersion || snapshot.AgentAppVersion != envelope.AgentAppVersion {
		return runtime.ErrVersionMismatch
	}
	sk := sessionstore.SessionKey{TenantID: envelope.TenantID, AgentAppID: envelope.AgentAppID, SessionID: envelope.SessionID}
	head, err := w.Sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: sk, RequestID: envelope.RequestID, InputSeq: envelope.InputSeq, Fence: fence})
	if err != nil {
		if err == runtime.ErrAlreadyTerminal {
			_, readErr := w.Sessions.GetTerminalByInputSeq(ctx, sessionstore.TerminalKey{SessionKey: sk, InputSeq: envelope.InputSeq})
			return readErr
		}
		return err
	}
	resultRef, err := w.Model.Generate(ctx, envelope, snapshot)
	if err != nil {
		return err
	}
	if beforeCommit != nil {
		if err := beforeCommit(ctx); err != nil {
			return err
		}
	}
	_, err = w.Sessions.CommitTurn(ctx, sessionstore.CommitTurnRequest{SessionKey: sk, RequestID: envelope.RequestID, CommitID: envelope.RequestID + ":terminal:0", Stage: "terminal", InputSeq: envelope.InputSeq, Fence: fence, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded, ResultRef: resultRef, ReplyCursor: envelope.RequestID + ":1", Outbox: []sessionstore.OutboxEvent{terminalAuditOutbox(ctx, envelope, runtime.OutcomeSucceeded)}})
	if errors.Is(err, runtime.ErrAlreadyTerminal) {
		return nil
	}
	return err
}
