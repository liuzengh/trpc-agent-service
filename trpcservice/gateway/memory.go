package gateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
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

type memoryQueueOutbox struct {
	id          string
	task        workqueue.AgentTask
	status      string
	lockedBy    string
	lockedUntil time.Time
	nextAttempt time.Time
}

type memoryRun struct {
	status   string
	workerID string
	result   RunResult
	errType  string
}

type memoryOutbound struct {
	item        OutboundItem
	status      string
	lockedBy    string
	lockedUntil time.Time
	nextAttempt time.Time
}

// MemoryJournal is a process-local transactional model for tests and the
// dependency-free tutorial.
type MemoryJournal struct {
	mu            sync.Mutex
	closed        bool
	inbound       map[string]memoryInbound
	conversations map[string]*memoryConversation
	outbox        map[string]*memoryQueueOutbox
	runs          map[string]*memoryRun
	outbound      map[string]*memoryOutbound
}

// NewMemoryJournal creates an empty journal.
func NewMemoryJournal() *MemoryJournal {
	return &MemoryJournal{
		inbound:       make(map[string]memoryInbound),
		conversations: make(map[string]*memoryConversation),
		outbox:        make(map[string]*memoryQueueOutbox),
		runs:          make(map[string]*memoryRun),
		outbound:      make(map[string]*memoryOutbound),
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
	traceParent, traceState := outboundTraceHeaders(ctx)
	task := workqueue.AgentTask{
		InboundID:         result.InboundID,
		RequestID:         result.RequestID,
		ConversationID:    result.ConversationID,
		Scope:             scope,
		MessageID:         request.ExternalMessageID,
		UserID:            request.UserID,
		SessionID:         request.SessionID,
		Text:              request.Text,
		ReplyTarget:       request.ReplyTarget,
		TurnSeq:           result.TurnSeq,
		TraceParent:       traceParent,
		TraceState:        traceState,
		ApprovedTools:     append([]string(nil), request.ApprovedTools...),
		ApprovedToolCalls: append([]governance.ApprovedToolCall(nil), request.ApprovedToolCalls...),
		ApprovalID:        request.ApprovalID,
	}
	outboxID := stableID("qout_", result.RequestID)
	j.outbox[outboxID] = &memoryQueueOutbox{
		id:          outboxID,
		task:        task,
		status:      "pending",
		nextAttempt: time.Now(),
	}
	j.inbound[inboundKey] = memoryInbound{result: result, payloadHash: payloadHash}
	j.runs[result.RequestID] = &memoryRun{status: "queued"}
	return result, nil
}

// Tasks returns a copy of durable tasks created by accepted messages.
func (j *MemoryJournal) Tasks() []workqueue.AgentTask {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make([]workqueue.AgentTask, 0, len(j.outbox))
	for _, item := range j.outbox {
		result = append(result, item.task)
	}
	return result
}

func (j *MemoryJournal) ClaimQueueOutbox(
	ctx context.Context,
	workerID string,
	limit int,
	lease time.Duration,
) ([]QueueOutboxItem, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if workerID == "" || limit <= 0 || lease <= 0 {
		return nil, fmt.Errorf("outbox worker, limit and lease are required")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil, ErrJournalClosed
	}
	now := time.Now()
	result := make([]QueueOutboxItem, 0, limit)
	for _, item := range j.outbox {
		if len(result) >= limit {
			break
		}
		if item.status == "published" || item.nextAttempt.After(now) ||
			(item.lockedBy != "" && item.lockedUntil.After(now)) {
			continue
		}
		item.status = "publishing"
		item.lockedBy = workerID
		item.lockedUntil = now.Add(lease)
		result = append(result, QueueOutboxItem{ID: item.id, Task: item.task})
	}
	return result, nil
}

func (j *MemoryJournal) MarkQueueOutboxPublished(
	_ context.Context,
	outboxID string,
	workerID string,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	item := j.outbox[outboxID]
	if item == nil || item.lockedBy != workerID {
		return fmt.Errorf("queue outbox ownership mismatch")
	}
	item.status = "published"
	item.lockedBy = ""
	item.lockedUntil = time.Time{}
	return nil
}

func (j *MemoryJournal) MarkQueueOutboxFailed(
	_ context.Context,
	outboxID string,
	workerID string,
	retryAt time.Time,
	_ error,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	item := j.outbox[outboxID]
	if item == nil || item.lockedBy != workerID {
		return fmt.Errorf("queue outbox ownership mismatch")
	}
	item.status = "pending"
	item.lockedBy = ""
	item.lockedUntil = time.Time{}
	item.nextAttempt = retryAt
	return nil
}

func (j *MemoryJournal) MarkRunRunning(
	_ context.Context,
	requestID string,
	workerID string,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	run := j.runs[requestID]
	if run == nil {
		return fmt.Errorf("Agent run not found")
	}
	if run.status == "completed" {
		return nil
	}
	run.status = "running"
	run.workerID = workerID
	return nil
}

func (j *MemoryJournal) CompleteRun(
	_ context.Context,
	task workqueue.AgentTask,
	result RunResult,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	run := j.runs[task.RequestID]
	if run == nil {
		return fmt.Errorf("Agent run not found")
	}
	if run.status == "completed" && run.result.FencingToken > result.FencingToken {
		return fmt.Errorf("stale Agent run fencing token")
	}
	run.status = "completed"
	run.result = result
	outboundID := stableID("out_", task.RequestID)
	if j.outbound[outboundID] == nil {
		j.outbound[outboundID] = &memoryOutbound{
			item: OutboundItem{
				ID:               outboundID,
				RequestID:        task.RequestID,
				TenantID:         task.Scope.TenantID,
				ChannelBindingID: task.Scope.ChannelBindingID,
				Text:             result.Reply,
				ReplyTarget:      task.ReplyTarget,
			},
			status:      "pending",
			nextAttempt: time.Now(),
		}
	}
	return nil
}

func (j *MemoryJournal) FailRun(
	_ context.Context,
	requestID string,
	errorType string,
	_ error,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	run := j.runs[requestID]
	if run == nil {
		return fmt.Errorf("Agent run not found")
	}
	if run.status != "completed" {
		run.status = "failed"
		run.errType = errorType
	}
	return nil
}

// RunStatus exposes the in-memory journal state to tests and local status APIs.
func (j *MemoryJournal) RunStatus(requestID string) (string, RunResult, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	run := j.runs[requestID]
	if run == nil {
		return "", RunResult{}, false
	}
	return run.status, run.result, true
}

func (j *MemoryJournal) ClaimOutbound(
	ctx context.Context,
	workerID string,
	limit int,
	lease time.Duration,
) ([]OutboundItem, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if workerID == "" || limit <= 0 || lease <= 0 {
		return nil, fmt.Errorf("outbound worker, limit and lease are required")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	now := time.Now()
	result := make([]OutboundItem, 0, limit)
	for _, outbound := range j.outbound {
		if len(result) >= limit {
			break
		}
		if outbound.status == "sent" || outbound.status == "dead" ||
			outbound.nextAttempt.After(now) ||
			(outbound.lockedBy != "" && outbound.lockedUntil.After(now)) {
			continue
		}
		outbound.status = "sending"
		outbound.lockedBy = workerID
		outbound.lockedUntil = now.Add(lease)
		outbound.item.AttemptCount++
		result = append(result, outbound.item)
	}
	return result, nil
}

func (j *MemoryJournal) MarkOutboundSent(
	_ context.Context,
	outboundID string,
	workerID string,
	_ string,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	outbound := j.outbound[outboundID]
	if outbound == nil || outbound.lockedBy != workerID {
		return fmt.Errorf("outbound ownership mismatch")
	}
	outbound.status = "sent"
	outbound.lockedBy = ""
	outbound.lockedUntil = time.Time{}
	return nil
}

func (j *MemoryJournal) MarkOutboundFailed(
	_ context.Context,
	outboundID string,
	workerID string,
	retryAt time.Time,
	terminal bool,
	_ error,
) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	outbound := j.outbound[outboundID]
	if outbound == nil || outbound.lockedBy != workerID {
		return fmt.Errorf("outbound ownership mismatch")
	}
	if terminal {
		outbound.status = "dead"
	} else {
		outbound.status = "pending"
		outbound.nextAttempt = retryAt
	}
	outbound.lockedBy = ""
	outbound.lockedUntil = time.Time{}
	return nil
}

func (j *MemoryJournal) OutboundStatus(outboundID string) (string, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	outbound := j.outbound[outboundID]
	if outbound == nil {
		return "", false
	}
	return outbound.status, true
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
