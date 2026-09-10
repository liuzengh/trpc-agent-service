package inmemory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct {
	mu      sync.Mutex
	inboxes map[messaging.InboxKey]messaging.InboxRecord
	jobs    map[string]preprocess.Job
	now     func() time.Time
}

func New() *Store {
	return &Store{inboxes: make(map[messaging.InboxKey]messaging.InboxRecord), jobs: make(map[string]preprocess.Job), now: func() time.Time { return time.Now().UTC() }}
}

func (s *Store) ClaimInboxAndSchedule(ctx context.Context, in preprocess.ClaimRequest) (messaging.InboxRecord, preprocess.Job, error) {
	if err := ctx.Err(); err != nil {
		return messaging.InboxRecord{}, preprocess.Job{}, err
	}
	if in.Inbox.TenantID == "" || in.Inbox.Channel == "" || in.Inbox.ExternalAccountID == "" || in.Inbox.ExternalMessageID == "" ||
		in.Inbox.AgentAppID == "" || in.Inbox.SessionID == "" || in.Inbox.PayloadDigest == "" || in.Inbox.KeyVersion < 1 ||
		in.Inbox.InitialState != messaging.InboxPreprocessPending || in.TenantVersion < 1 || in.ConfigVersion < 1 ||
		in.ChannelBindingID == "" || in.UserID == "" {
		return messaging.InboxRecord{}, preprocess.Job{}, runtime.ErrInvariantViolation
	}
	requestID, payloadRef := messaging.StableInboxIdentity(in.Inbox.InboxKey)
	jobID, _ := preprocess.StableJobID(in.Inbox.TenantID, requestID)
	now := s.now()
	record := messaging.InboxRecord{InboxKey: in.Inbox.InboxKey, RequestID: requestID, AgentAppID: in.Inbox.AgentAppID,
		SessionID: in.Inbox.SessionID, ExternalChatID: in.Inbox.ExternalChatID, ExternalUserID: in.Inbox.ExternalUserID,
		State: messaging.InboxPreprocessPending, PayloadRef: payloadRef, PayloadDigest: in.Inbox.PayloadDigest,
		KeyVersion: in.Inbox.KeyVersion, CreatedAt: now, UpdatedAt: now}
	job := preprocess.Job{TenantID: in.Inbox.TenantID, RequestID: requestID, JobID: jobID, PayloadRef: payloadRef,
		AgentAppID: in.Inbox.AgentAppID, SessionID: in.Inbox.SessionID, UserID: in.UserID, Channel: in.Inbox.Channel,
		ChannelBindingID: in.ChannelBindingID, TraceParent: in.TraceParent, TenantVersion: in.TenantVersion,
		ConfigVersion: in.ConfigVersion, State: preprocess.Pending, CreatedAt: now, UpdatedAt: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.inboxes[in.Inbox.InboxKey]; ok {
		existingJob := s.jobs[jobID]
		if existing.RequestID != record.RequestID || existing.AgentAppID != record.AgentAppID || existing.SessionID != record.SessionID ||
			existing.ExternalChatID != record.ExternalChatID || existing.ExternalUserID != record.ExternalUserID ||
			existing.PayloadRef != record.PayloadRef || existing.PayloadDigest != record.PayloadDigest || existing.KeyVersion != record.KeyVersion ||
			existingJob.JobID != job.JobID || existingJob.TenantVersion != job.TenantVersion || existingJob.AgentAppID != job.AgentAppID ||
			existingJob.SessionID != job.SessionID || existingJob.UserID != job.UserID || existingJob.Channel != job.Channel ||
			existingJob.ChannelBindingID != job.ChannelBindingID || existingJob.ConfigVersion != job.ConfigVersion ||
			existingJob.PayloadRef != job.PayloadRef {
			return messaging.InboxRecord{}, preprocess.Job{}, runtime.ErrIdempotencyCollision
		}
		return existing, existingJob, nil
	}
	s.inboxes[in.Inbox.InboxKey] = record
	s.jobs[jobID] = job
	return record, job, nil
}

func (s *Store) ClaimReadyForDispatch(ctx context.Context, options preprocess.ClaimOptions) ([]preprocess.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.Owner == "" || options.Now.IsZero() || options.TTL <= 0 || options.Limit < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.jobs))
	for id, job := range s.jobs {
		if job.State == preprocess.Ready && job.DispatchedAt.IsZero() && (job.LeaseOwner == "" || !job.LeaseUntil.After(options.Now)) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > options.Limit {
		ids = ids[:options.Limit]
	}
	result := make([]preprocess.Job, 0, len(ids))
	for _, id := range ids {
		job := s.jobs[id]
		job.LeaseOwner, job.LeaseUntil = options.Owner, options.Now.Add(options.TTL)
		job.Version++
		job.UpdatedAt = options.Now
		s.jobs[id] = job
		result = append(result, job)
	}
	return result, nil
}

func (s *Store) ClaimJobs(ctx context.Context, options preprocess.ClaimOptions) ([]preprocess.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.Owner == "" || options.Now.IsZero() || options.TTL <= 0 || options.Limit < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.jobs))
	for id, job := range s.jobs {
		if (job.State == preprocess.Pending || job.State == preprocess.RetryWait) && !job.NotBefore.After(options.Now) ||
			job.State == preprocess.Running && !job.LeaseUntil.After(options.Now) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > options.Limit {
		ids = ids[:options.Limit]
	}
	result := make([]preprocess.Job, 0, len(ids))
	for _, id := range ids {
		job := s.jobs[id]
		job.State, job.LeaseOwner, job.LeaseUntil = preprocess.Running, options.Owner, options.Now.Add(options.TTL)
		job.Attempt++
		job.Version++
		job.UpdatedAt = options.Now
		s.jobs[id] = job
		result = append(result, job)
	}
	return result, nil
}

func (s *Store) FinishReady(ctx context.Context, claimed preprocess.Job) (preprocess.Job, error) {
	return s.finish(ctx, claimed, preprocess.Ready, time.Time{}, "")
}

func (s *Store) FinishRetry(ctx context.Context, claimed preprocess.Job, notBefore time.Time, reason string) (preprocess.Job, error) {
	if notBefore.IsZero() || reason == "" {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	return s.finish(ctx, claimed, preprocess.RetryWait, notBefore, reason)
}

func (s *Store) FinishRejected(ctx context.Context, claimed preprocess.Job, reason string) (preprocess.Job, error) {
	if reason == "" {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	return s.finish(ctx, claimed, preprocess.Rejected, time.Time{}, reason)
}

func (s *Store) finish(ctx context.Context, claimed preprocess.Job, state preprocess.State, notBefore time.Time, reason string) (preprocess.Job, error) {
	if err := ctx.Err(); err != nil {
		return preprocess.Job{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.jobs[claimed.JobID]
	if !ok {
		return preprocess.Job{}, runtime.ErrNotFound
	}
	if current.State != preprocess.Running || current.Version != claimed.Version || current.LeaseOwner == "" || current.LeaseOwner != claimed.LeaseOwner {
		return preprocess.Job{}, runtime.ErrVersionConflict
	}
	if state != preprocess.Ready && claimed.PreparedPayloadRef != "" {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	current.State, current.NotBefore, current.RejectReason = state, notBefore, reason
	if state == preprocess.Ready && claimed.PreparedPayloadRef != "" {
		current.PreparedPayloadRef = claimed.PreparedPayloadRef
	}
	current.LeaseOwner, current.LeaseUntil = "", time.Time{}
	current.Version++
	current.UpdatedAt = s.now()
	s.jobs[current.JobID] = current
	for key, inbox := range s.inboxes {
		if inbox.TenantID == current.TenantID && inbox.RequestID == current.RequestID {
			switch state {
			case preprocess.Ready:
				inbox.State = messaging.InboxDispatchPending
			case preprocess.Rejected:
				inbox.State, inbox.TerminalReason = messaging.InboxTerminal, reason
			}
			inbox.Version++
			inbox.UpdatedAt = current.UpdatedAt
			s.inboxes[key] = inbox
			break
		}
	}
	return current, nil
}

func (s *Store) ListReadyForDispatch(ctx context.Context, limit int) ([]preprocess.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]preprocess.Job, 0, limit)
	for _, job := range s.jobs {
		if job.State == preprocess.Ready && job.DispatchedAt.IsZero() {
			result = append(result, job)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].JobID < result[j].JobID })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *Store) MarkDispatched(ctx context.Context, expected preprocess.Job, at time.Time) (preprocess.Job, error) {
	if err := ctx.Err(); err != nil {
		return preprocess.Job{}, err
	}
	if at.IsZero() {
		return preprocess.Job{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.jobs[expected.JobID]
	if !ok {
		return preprocess.Job{}, runtime.ErrNotFound
	}
	if current.Version != expected.Version || current.State != preprocess.Ready || !current.DispatchedAt.IsZero() ||
		current.LeaseOwner == "" || current.LeaseOwner != expected.LeaseOwner {
		return preprocess.Job{}, runtime.ErrVersionConflict
	}
	current.DispatchedAt, current.UpdatedAt = at, at
	current.LeaseOwner, current.LeaseUntil = "", time.Time{}
	current.Version++
	s.jobs[current.JobID] = current
	return current, nil
}
