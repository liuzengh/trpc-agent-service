package agent

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

// stubConfirmations satisfies governance.ConfirmationCoordinator for tests that
// build an Agent without exercising the tool-approval path. Every operation
// returns the zero value, so a test that unexpectedly depends on confirmation
// behaviour observes an empty result instead of a stubbed success.
//
// The governance in-memory Store is deliberately not reused here: it owns
// policy and budget snapshots, not the confirmation lifecycle, and only the
// postgres Store implements the full coordinator contract.
type stubConfirmations struct{}

func (stubConfirmations) Suspend(context.Context, sessionstore.CommitTurnRequest, governance.SuspensionRequest) (governance.Confirmation, error) {
	return governance.Confirmation{}, nil
}

func (stubConfirmations) Decide(context.Context, governance.ConfirmationDecision) (governance.Confirmation, error) {
	return governance.Confirmation{}, nil
}

func (stubConfirmations) ExpireDue(context.Context, time.Time, int) ([]governance.Confirmation, error) {
	return nil, nil
}

func (stubConfirmations) ConsumeGrant(context.Context, governance.GrantClaim) (governance.Grant, error) {
	return governance.Grant{}, nil
}

func (stubConfirmations) GetConfirmation(context.Context, string, string) (governance.Confirmation, error) {
	return governance.Confirmation{}, nil
}

func (stubConfirmations) GetConfirmationByRequest(context.Context, string, string) (governance.Confirmation, error) {
	return governance.Confirmation{}, nil
}

func (stubConfirmations) GetGrantByConfirmation(context.Context, string, string) (governance.Grant, error) {
	return governance.Grant{}, nil
}

func (stubConfirmations) GetToolAttempt(context.Context, string, string) (governance.ToolAttempt, error) {
	return governance.ToolAttempt{}, nil
}

func (stubConfirmations) FinishToolAttempt(context.Context, governance.FinishToolAttemptRequest) (governance.ToolAttempt, error) {
	return governance.ToolAttempt{}, nil
}

var _ governance.ConfirmationCoordinator = stubConfirmations{}
