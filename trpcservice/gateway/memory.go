package gateway

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

type memoryInbound struct {
	result      AcceptResult
	payloadHash string
}

type memoryConversation struct {
	id         string
	revisionID string
	lastTurn   int64
}

// MemoryJournal is a process-local transactional model for tests and the
// dependency-free tutorial.
type MemoryJournal struct {
	mu            sync.Mutex
	closed        bool
	inbound       map[string]memoryInbound
	conversations map[string]*memoryConversation
	tasks         []workqueue.AgentTask
}

// NewMemoryJournal creates an empty journal.
func NewMemoryJournal() *MemoryJournal {
	return &MemoryJournal{
		inbound:       make(map[string]memoryInbound),
		conversations: make(map[string]*memoryConversation),
	}
}

func (j *MemoryJournal) Accept(
	ctx context.Context,
	request InboundRequest,
) (AcceptResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return AcceptResult{}, context.Cause(ctx)
	}
	if err := validateInbound(&request); err != nil {
		return AcceptResult{}, err
	}
	inboundKey := request.Scope.ChannelBindingID + "\x00" + request.ExternalMessageID
	payloadHash := inboundPayloadHash(request)
	conversationKey := request.Scope.StorageScope + "\x00" + request.UserID + "\x00" + request.SessionID

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return AcceptResult{}, ErrJournalClosed
	}
	if existing, ok := j.inbound[inboundKey]; ok {
		if existing.payloadHash != payloadHash {
			return AcceptResult{}, ErrMessageConflict
		}
		result := existing.result
		result.Duplicate = true
		return result, nil
	}
	conversation := j.conversations[conversationKey]
	if conversation == nil {
		conversation = &memoryConversation{
			id: stableID(
				"conv_",
				request.Scope.StorageScope,
				request.UserID,
				request.SessionID,
			),
			revisionID: request.Scope.RevisionID,
		}
		j.conversations[conversationKey] = conversation
	}
	conversation.lastTurn++
	result := AcceptResult{
		InboundID: stableID(
			"in_",
			request.Scope.ChannelBindingID,
			request.ExternalMessageID,
		),
		RequestID: stableID(
			"req_",
			request.Scope.ChannelBindingID,
			request.ExternalMessageID,
		),
		ConversationID: conversation.id,
		RevisionID:     conversation.revisionID,
		TurnSeq:        conversation.lastTurn,
	}
	scope := request.Scope
	scope.RevisionID = conversation.revisionID
	j.tasks = append(j.tasks, workqueue.AgentTask{
		InboundID:      result.InboundID,
		RequestID:      result.RequestID,
		ConversationID: result.ConversationID,
		Scope:          scope,
		MessageID:      request.ExternalMessageID,
		UserID:         request.UserID,
		SessionID:      request.SessionID,
		Text:           request.Text,
		TurnSeq:        result.TurnSeq,
	})
	j.inbound[inboundKey] = memoryInbound{result: result, payloadHash: payloadHash}
	return result, nil
}

// Tasks returns a copy of durable tasks created by accepted messages.
func (j *MemoryJournal) Tasks() []workqueue.AgentTask {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]workqueue.AgentTask(nil), j.tasks...)
}

func (j *MemoryJournal) Ready(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return ErrJournalClosed
	}
	return nil
}

func (j *MemoryJournal) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	j.closed = true
	j.mu.Unlock()
	return nil
}

var _ Journal = (*MemoryJournal)(nil)
