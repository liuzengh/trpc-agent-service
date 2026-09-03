package background

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/extractor"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type ProcessorOptions struct {
	WorkerID     string
	ClaimLease   time.Duration
	PollInterval time.Duration
	RetryDelay   time.Duration
}

type Processor struct {
	jobs           Repository
	control        controlplane.Repository
	sessions       session.Service
	memories       memory.Service
	memoryMigrator *platformstorage.MemoryRouter
	knowledge      *platformstorage.KnowledgeRouter
	extractor      extractor.MemoryExtractor
	audit          audit.Writer
	options        ProcessorOptions
}

func NewProcessor(
	jobs Repository,
	control controlplane.Repository,
	sessions session.Service,
	memories memory.Service,
	knowledgeRouter *platformstorage.KnowledgeRouter,
	selectedModel model.Model,
	auditWriter audit.Writer,
	options ProcessorOptions,
) (*Processor, error) {
	if jobs == nil || control == nil || sessions == nil || memories == nil ||
		knowledgeRouter == nil || selectedModel == nil {
		return nil, errors.New("background processor dependencies are required")
	}
	if options.WorkerID == "" || options.ClaimLease <= 0 ||
		options.PollInterval <= 0 || options.RetryDelay <= 0 {
		return nil, errors.New("background processor options are invalid")
	}
	memoryExtractor := extractor.NewExtractor(selectedModel)
	if setter, ok := memoryExtractor.(interface {
		SetEnabledTools(map[string]struct{})
	}); ok {
		setter.SetEnabledTools(map[string]struct{}{
			memory.AddToolName: {},
		})
	}
	memoryMigrator, ok := memories.(*platformstorage.MemoryRouter)
	if !ok {
		return nil, errors.New("background processor requires platform MemoryRouter")
	}
	return &Processor{
		jobs: jobs, control: control, sessions: sessions, memories: memories,
		memoryMigrator: memoryMigrator, knowledge: knowledgeRouter,
		extractor: memoryExtractor, audit: auditWriter,
		options: options,
	}, nil
}

func (p *Processor) ProcessOne(ctx context.Context) (bool, error) {
	job, err := p.jobs.Claim(ctx, p.options.WorkerID, p.options.ClaimLease)
	if errors.Is(err, ErrNoJob) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	jobCtx := contextWithTraceParent(ctx, job.TraceParent)
	jobCtx, span := otel.Tracer("trpc-agent-service/background").Start(jobCtx, "background."+job.Type)
	span.SetAttributes(
		attribute.String("tenant.id", job.TenantID),
		attribute.String("agent.app.id", job.AppID),
		attribute.String("agent.revision.id", job.RevisionID),
		attribute.String("background.job.id", job.ID),
		attribute.Int("background.job.attempt", job.AttemptCount),
	)
	started := time.Now()
	processErr := p.process(jobCtx, job)
	span.End()
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if processErr != nil {
		retryAt := time.Now().Add(p.retryDelay(job.AttemptCount))
		failErr := p.jobs.Fail(finalizeCtx, job, p.options.WorkerID, retryAt, processErr)
		auditErr := p.recordAudit(finalizeCtx, job, "background_job_failed", started, processErr)
		return true, errors.Join(processErr, failErr, auditErr)
	}
	completeErr := p.jobs.Complete(finalizeCtx, job.ID, p.options.WorkerID)
	auditErr := p.recordAudit(finalizeCtx, job, "background_job_completed", started, nil)
	return true, errors.Join(completeErr, auditErr)
}

func (p *Processor) process(ctx context.Context, job Job) error {
	revision, err := p.control.GetRevision(ctx, job.TenantID, job.RevisionID)
	if err != nil {
		return err
	}
	if revision.AppID != job.AppID {
		return errors.New("background job revision scope mismatch")
	}
	switch job.Type {
	case JobSummary:
		var payload SessionJobPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		return p.processSummary(ctx, revision, payload)
	case JobMemoryExtract:
		var payload SessionJobPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		return p.processMemory(ctx, revision, payload)
	case JobKnowledgeUpsert:
		var payload KnowledgeUpsertPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		scope, err := jobScope(job)
		if err != nil {
			return err
		}
		_, err = p.knowledge.UpsertDocument(ctx, scope, revision, payload.Document)
		return err
	case JobKnowledgeDelete:
		var payload KnowledgeDeletePayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		scope, err := jobScope(job)
		if err != nil {
			return err
		}
		return p.knowledge.DeleteDocument(ctx, scope, revision, payload.DocumentID)
	case JobMemoryBackfill:
		var payload MemoryMigrationPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		for _, userID := range payload.UserIDs {
			if _, err := p.memoryMigrator.BackfillUser(
				ctx, job.TenantID, payload.MigrationID, userID,
			); err != nil {
				return err
			}
		}
		return nil
	case JobMemoryVerify:
		var payload MemoryMigrationPayload
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		results := make([]platformstorage.MemoryMigrationVerification, 0, len(payload.UserIDs))
		passed := true
		for _, userID := range payload.UserIDs {
			result, err := p.memoryMigrator.VerifyUser(
				ctx, job.TenantID, payload.MigrationID, userID,
			)
			if err != nil {
				return err
			}
			results = append(results, result)
			passed = passed && result.Passed
		}
		if !passed {
			return errors.New("memory migration verification did not pass")
		}
		migration, err := p.control.GetBackendMigration(
			ctx, job.TenantID, payload.MigrationID,
		)
		if err != nil {
			return err
		}
		verification, err := json.Marshal(map[string]any{
			"passed": true, "users": results,
		})
		if err != nil {
			return err
		}
		mutable, ok := p.control.(controlplane.MutableRepository)
		if !ok {
			return errors.New("control plane cannot update migration verification")
		}
		if migration.RepairBacklog > 0 {
			if err := p.control.AdjustBackendMigrationRepair(
				ctx, job.TenantID, payload.MigrationID, -migration.RepairBacklog,
			); err != nil {
				return err
			}
		}
		_, err = mutable.TransitionBackendMigration(
			ctx, job.TenantID, payload.MigrationID, controlplane.MigrationVerify,
			payload.ExpectedVersion, nil, verification,
		)
		return err
	default:
		return fmt.Errorf("unsupported background job type %q", job.Type)
	}
}

type agentBackgroundConfig struct {
	SummaryEveryTurns int `json:"summary_every_turns"`
}

func (p *Processor) processSummary(
	ctx context.Context,
	revision controlplane.AgentRevision,
	payload SessionJobPayload,
) error {
	var config agentBackgroundConfig
	if err := json.Unmarshal(revision.AgentConfig, &config); err != nil {
		return err
	}
	if config.SummaryEveryTurns <= 0 || payload.TurnSeq%int64(config.SummaryEveryTurns) != 0 {
		return nil
	}
	sess, err := p.sessions.GetSession(ctx, session.Key{
		AppName: payload.StorageScope, UserID: payload.UserID, SessionID: payload.SessionID,
	})
	if err != nil {
		return err
	}
	return p.sessions.CreateSessionSummary(ctx, sess, "", true)
}

type memoryBackgroundConfig struct {
	AutoExtract bool `json:"auto_extract"`
	EveryTurns  int  `json:"every_turns"`
}

func (p *Processor) processMemory(
	ctx context.Context,
	revision controlplane.AgentRevision,
	payload SessionJobPayload,
) error {
	var config memoryBackgroundConfig
	if err := json.Unmarshal(revision.MemoryConfig, &config); err != nil {
		return err
	}
	if !config.AutoExtract || config.EveryTurns <= 0 ||
		payload.TurnSeq%int64(config.EveryTurns) != 0 {
		return nil
	}
	key := session.Key{
		AppName: payload.StorageScope, UserID: payload.UserID, SessionID: payload.SessionID,
	}
	sess, err := p.sessions.GetSession(ctx, key)
	if err != nil {
		return err
	}
	lastExtractAt := parseWatermark(sess.State[memory.SessionStateKeyAutoMemoryLastExtractAt])
	messages, latest := sessionMessagesAfter(sess.Events, lastExtractAt)
	if len(messages) == 0 {
		return nil
	}
	userKey := memory.UserKey{AppName: payload.StorageScope, UserID: payload.UserID}
	existing, err := p.memories.ReadMemories(ctx, userKey, 100)
	if err != nil {
		return err
	}
	operations, err := p.extractor.Extract(ctx, messages, existing)
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if err := p.applyMemoryOperation(ctx, userKey, operation); err != nil {
			return err
		}
	}
	if latest.IsZero() {
		latest = time.Now().UTC()
	}
	return p.sessions.UpdateSessionState(ctx, key, session.StateMap{
		memory.SessionStateKeyAutoMemoryLastExtractAt: []byte(latest.UTC().Format(time.RFC3339Nano)),
	})
}

func (p *Processor) applyMemoryOperation(
	ctx context.Context,
	userKey memory.UserKey,
	operation *extractor.Operation,
) error {
	if operation == nil {
		return nil
	}
	metadata := &memory.Metadata{
		Kind: operation.MemoryKind, EventTime: operation.EventTime,
		Participants: operation.Participants, Location: operation.Location,
	}
	switch operation.Type {
	case extractor.OperationAdd:
		return p.memories.AddMemory(
			ctx, userKey, operation.Memory, operation.Topics, memory.WithMetadata(metadata),
		)
	case extractor.OperationUpdate:
		return p.memories.UpdateMemory(ctx, memory.Key{
			AppName: userKey.AppName, UserID: userKey.UserID, MemoryID: operation.MemoryID,
		}, operation.Memory, operation.Topics, memory.WithUpdateMetadata(metadata))
	case extractor.OperationDelete:
		return p.memories.DeleteMemory(ctx, memory.Key{
			AppName: userKey.AppName, UserID: userKey.UserID, MemoryID: operation.MemoryID,
		})
	case extractor.OperationClear:
		return p.memories.ClearMemories(ctx, userKey)
	default:
		return fmt.Errorf("unsupported memory operation %q", operation.Type)
	}
}

func sessionMessagesAfter(events []event.Event, watermark time.Time) ([]model.Message, time.Time) {
	result := make([]model.Message, 0)
	var latest time.Time
	for _, item := range events {
		if !item.Timestamp.After(watermark) || item.Response == nil {
			continue
		}
		if item.Timestamp.After(latest) {
			latest = item.Timestamp
		}
		for _, choice := range item.Response.Choices {
			if choice.Message.Role != "" &&
				(choice.Message.Content != "" || len(choice.Message.ToolCalls) > 0) {
				result = append(result, choice.Message)
			}
		}
	}
	return result, latest
}

func parseWatermark(value []byte) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, string(value))
	return parsed
}

func jobScope(job Job) (runtimecontext.Scope, error) {
	return runtimecontext.NewScope(job.TenantID, job.AppID, job.RevisionID, "job", "background-job")
}

func (p *Processor) retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		attempt = 6
	}
	return p.options.RetryDelay * time.Duration(1<<(attempt-1))
}

func (p *Processor) recordAudit(
	ctx context.Context,
	job Job,
	decision string,
	started time.Time,
	cause error,
) error {
	if p.audit == nil {
		return nil
	}
	errorType := ""
	if cause != nil {
		errorType = "background_job"
	}
	return p.audit.Record(ctx, audit.Event{
		TenantID: job.TenantID, RevisionID: job.RevisionID,
		TraceID: audit.TraceID(ctx), Decision: decision,
		Latency: time.Since(started), ErrorType: errorType,
		Details: map[string]any{
			"job_id": job.ID, "job_type": job.Type,
			"attempt": strconv.Itoa(job.AttemptCount),
		},
	})
}

func (p *Processor) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.options.PollInterval)
	defer ticker.Stop()
	for {
		_, _ = p.ProcessOne(ctx)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}
