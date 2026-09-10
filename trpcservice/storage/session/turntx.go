package session

import (
	"context"

	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

type TurnTx interface {
	SessionService() agentsession.Service
	Commit(context.Context, CommitTurnRequest) (CommitTurnResult, error)
	Rollback(context.Context) error
}
