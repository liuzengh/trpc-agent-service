// Package inmemory provides durable-semantics reference stores for tests.
package inmemory

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct {
	mu           sync.Mutex
	inbox        map[messaging.InboxKey]messaging.InboxRecord
	deliveries   map[messaging.DeliveryKey]messaging.DeliveryRecord
	payloads     map[string]messaging.PayloadRecord
	prepared     map[string]messaging.PreparedPayloadRecord
	results      map[string]messaging.ResultRecord
	toolResults  map[string]messaging.ToolResultRecord
	interactions map[string]messaging.InteractionRecord
	routes       map[string]messaging.ReplyRoute
}

func New() *Store {
	return &Store{inbox: make(map[messaging.InboxKey]messaging.InboxRecord), deliveries: make(map[messaging.DeliveryKey]messaging.DeliveryRecord), payloads: make(map[string]messaging.PayloadRecord), prepared: make(map[string]messaging.PreparedPayloadRecord), results: make(map[string]messaging.ResultRecord), toolResults: make(map[string]messaging.ToolResultRecord), interactions: make(map[string]messaging.InteractionRecord), routes: make(map[string]messaging.ReplyRoute)}
}

func (s *Store) PutInteraction(ctx context.Context, in messaging.InteractionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	contentType, err := messaging.NormalizeContentType(in.ContentType)
	if in.TenantID == "" || in.RequestID == "" || in.ContentRef == "" || in.ContentDigest == "" || len(in.Content) == 0 || err != nil {
		return runtime.ErrCommitConflict
	}
	in.ContentType = contentType
	if in.KeyVersion < 1 {
		in.KeyVersion = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.RequestID + "\x00" + in.ContentRef
	if old, ok := s.interactions[key]; ok {
		if old.ContentDigest != in.ContentDigest || old.ContentType != in.ContentType || old.KeyVersion != in.KeyVersion || string(old.Content) != string(in.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	in.Content = append([]byte(nil), in.Content...)
	in.CreatedAt = time.Now().UTC()
	s.interactions[key] = in
	return nil
}

func (s *Store) GetReplyContent(ctx context.Context, tenantID, requestID, contentRef string) (messaging.ResultRecord, error) {
	terminalExists := false
	if result, err := s.GetResult(ctx, tenantID, requestID); err == nil {
		if result.ResultRef == contentRef {
			return result, nil
		}
		terminalExists = true
	} else if !errors.Is(err, runtime.ErrNotFound) {
		return messaging.ResultRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.interactions[tenantID+"\x00"+requestID+"\x00"+contentRef]
	if !ok {
		if terminalExists {
			return messaging.ResultRecord{}, runtime.ErrVersionMismatch
		}
		return messaging.ResultRecord{}, runtime.ErrNotFound
	}
	return messaging.ResultRecord{TenantID: tenantID, RequestID: requestID, ResultRef: contentRef, ContentDigest: value.ContentDigest, ContentType: value.ContentType,
		Content: append([]byte(nil), value.Content...), KeyVersion: value.KeyVersion, CreatedAt: value.CreatedAt}, nil
}

func (s *Store) PutToolResult(ctx context.Context, in messaging.ToolResultRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.TenantID == "" || in.GrantID == "" || in.RequestID == "" || in.ResultRef == "" || in.ContentDigest == "" || len(in.Content) == 0 {
		return runtime.ErrCommitConflict
	}
	if in.KeyVersion < 1 {
		in.KeyVersion = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.GrantID
	if old, ok := s.toolResults[key]; ok {
		if old.RequestID != in.RequestID || old.ResultRef != in.ResultRef || old.ContentDigest != in.ContentDigest || old.KeyVersion != in.KeyVersion || string(old.Content) != string(in.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	in.Content = append([]byte(nil), in.Content...)
	in.CreatedAt = time.Now().UTC()
	s.toolResults[key] = in
	return nil
}

func (s *Store) GetToolResult(ctx context.Context, tenantID, grantID string) (messaging.ToolResultRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.ToolResultRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.toolResults[tenantID+"\x00"+grantID]
	if !ok {
		return messaging.ToolResultRecord{}, runtime.ErrNotFound
	}
	record.Content = append([]byte(nil), record.Content...)
	return record, nil
}

func (s *Store) PutReplyRoute(route messaging.ReplyRoute) error {
	if route.TenantID == "" || route.RequestID == "" || route.Channel == "" || route.ChannelBindingID == "" || route.ExternalAccountID == "" || route.ConfigVersion < 1 {
		return runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := route.TenantID + "\x00" + route.RequestID
	if old, ok := s.routes[key]; ok && old != route {
		return runtime.ErrIdempotencyCollision
	}
	s.routes[key] = route
	return nil
}

func (s *Store) ResolveReplyRoute(ctx context.Context, tenantID, requestID string) (messaging.ReplyRoute, error) {
	if err := ctx.Err(); err != nil {
		return messaging.ReplyRoute{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.routes[tenantID+"\x00"+requestID]
	if !ok {
		return messaging.ReplyRoute{}, runtime.ErrNotFound
	}
	return route, nil
}

func (s *Store) PutResult(ctx context.Context, in messaging.ResultRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	contentType, err := messaging.NormalizeContentType(in.ContentType)
	if in.TenantID == "" || in.RequestID == "" || in.ResultRef == "" || in.ContentDigest == "" || len(in.Content) == 0 || err != nil {
		return runtime.ErrCommitConflict
	}
	in.ContentType = contentType
	if in.KeyVersion < 1 {
		in.KeyVersion = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.RequestID
	if old, ok := s.results[key]; ok {
		if old.ResultRef != in.ResultRef || old.ContentDigest != in.ContentDigest || old.ContentType != in.ContentType || old.KeyVersion != in.KeyVersion || string(old.Content) != string(in.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	in.Content = append([]byte(nil), in.Content...)
	in.CreatedAt = time.Now().UTC()
	s.results[key] = in
	return nil
}

func (s *Store) GetResult(ctx context.Context, tenantID, requestID string) (messaging.ResultRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.ResultRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.results[tenantID+"\x00"+requestID]
	if !ok {
		return messaging.ResultRecord{}, runtime.ErrNotFound
	}
	record.Content = append([]byte(nil), record.Content...)
	return record, nil
}

func (s *Store) PutPayload(ctx context.Context, in messaging.PayloadRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.TenantID == "" || in.RequestID == "" || in.PayloadRef == "" || in.ContentDigest == "" || len(in.Content) == 0 {
		return runtime.ErrCommitConflict
	}
	if in.KeyVersion < 1 {
		in.KeyVersion = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.RequestID
	if old, ok := s.payloads[key]; ok {
		if old.PayloadRef != in.PayloadRef || old.ContentDigest != in.ContentDigest || old.KeyVersion != in.KeyVersion || string(old.Content) != string(in.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	in.Content = append([]byte(nil), in.Content...)
	in.CreatedAt = time.Now().UTC()
	s.payloads[key] = in
	return nil
}

func (s *Store) GetPayload(ctx context.Context, tenantID, requestID string) (messaging.PayloadRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.PayloadRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.payloads[tenantID+"\x00"+requestID]
	if !ok {
		return messaging.PayloadRecord{}, runtime.ErrNotFound
	}
	record.Content = append([]byte(nil), record.Content...)
	return record, nil
}

func (s *Store) PutPreparedPayload(ctx context.Context, in messaging.PreparedPayloadRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.TenantID == "" || in.RequestID == "" || !strings.HasPrefix(in.PayloadRef, "prepared://") || !strings.HasPrefix(in.SourcePayloadRef, "inbound://") || in.ContentDigest == "" || len(in.Content) == 0 || in.KeyVersion < 1 ||
		in.ArtifactRetention < time.Second || in.ArtifactRetention%time.Second != 0 || !validPreparedArtifactReferences(in.ArtifactReferences) {
		return runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.TenantID + "\x00" + in.RequestID + "\x00" + in.PayloadRef
	if old, ok := s.prepared[key]; ok {
		if old.SourcePayloadRef != in.SourcePayloadRef || old.ContentDigest != in.ContentDigest || old.KeyVersion != in.KeyVersion ||
			old.ArtifactRetention != in.ArtifactRetention || !samePreparedArtifactReferences(old.ArtifactReferences, in.ArtifactReferences) ||
			string(old.Content) != string(in.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	in.Content = append([]byte(nil), in.Content...)
	in.ArtifactReferences = append([]messaging.PreparedArtifactReference(nil), in.ArtifactReferences...)
	in.CreatedAt = time.Now().UTC()
	s.prepared[key] = in
	return nil
}

func validPreparedArtifactReferences(values []messaging.PreparedArtifactReference) bool {
	if len(values) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.ArtifactID == "" {
			return false
		}
		if _, exists := seen[value.ArtifactID]; exists {
			return false
		}
		seen[value.ArtifactID] = struct{}{}
	}
	return true
}

func samePreparedArtifactReferences(left, right []messaging.PreparedArtifactReference) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, value := range left {
		seen[value.ArtifactID] = struct{}{}
	}
	for _, value := range right {
		if _, ok := seen[value.ArtifactID]; !ok {
			return false
		}
	}
	return true
}

func (s *Store) GetPreparedPayload(ctx context.Context, tenantID, requestID, payloadRef string) (messaging.PayloadRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.PayloadRecord{}, err
	}
	if tenantID == "" || requestID == "" || !strings.HasPrefix(payloadRef, "prepared://") {
		return messaging.PayloadRecord{}, runtime.ErrTenantScope
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.prepared[tenantID+"\x00"+requestID+"\x00"+payloadRef]
	if !ok {
		return messaging.PayloadRecord{}, runtime.ErrNotFound
	}
	return messaging.PayloadRecord{TenantID: record.TenantID, RequestID: record.RequestID, PayloadRef: record.PayloadRef,
		ContentDigest: record.ContentDigest, Content: append([]byte(nil), record.Content...), KeyVersion: record.KeyVersion, CreatedAt: record.CreatedAt}, nil
}

func (s *Store) ClaimInbox(ctx context.Context, in messaging.ClaimInboxRequest) (messaging.InboxRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.InboxRecord{}, err
	}
	if in.TenantID == "" || in.Channel == "" || in.ExternalAccountID == "" || in.ExternalMessageID == "" {
		return messaging.InboxRecord{}, runtime.ErrTenantScope
	}
	if in.AgentAppID == "" || in.PayloadDigest == "" || in.KeyVersion < 1 ||
		(in.InitialState != messaging.InboxPreprocessPending && in.InitialState != messaging.InboxDispatchPending) {
		return messaging.InboxRecord{}, runtime.ErrCommitConflict
	}
	requestID, payloadRef := messaging.StableInboxIdentity(in.InboxKey)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.inbox[in.InboxKey]; ok {
		if old.PayloadDigest != in.PayloadDigest || old.PayloadRef != payloadRef || old.AgentAppID != in.AgentAppID || old.SessionID != in.SessionID ||
			old.ExternalChatID != in.ExternalChatID || old.ExternalUserID != in.ExternalUserID {
			return messaging.InboxRecord{}, runtime.ErrIdempotencyCollision
		}
		return old, nil
	}
	now := time.Now().UTC()
	record := messaging.InboxRecord{InboxKey: in.InboxKey, RequestID: requestID, AgentAppID: in.AgentAppID, SessionID: in.SessionID,
		ExternalChatID: in.ExternalChatID, ExternalUserID: in.ExternalUserID, State: in.InitialState, PayloadRef: payloadRef,
		PayloadDigest: in.PayloadDigest, KeyVersion: in.KeyVersion, CreatedAt: now, UpdatedAt: now}
	s.inbox[in.InboxKey] = record
	return record, nil
}

func (s *Store) GetInbox(ctx context.Context, key messaging.InboxKey) (messaging.InboxRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.InboxRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.inbox[key]
	if !ok {
		return messaging.InboxRecord{}, runtime.ErrNotFound
	}
	return record, nil
}

func (s *Store) GetDelivery(ctx context.Context, key messaging.DeliveryKey) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.deliveries[key]
	if !ok {
		return messaging.DeliveryRecord{}, runtime.ErrNotFound
	}
	return record, nil
}

func (s *Store) ClaimDelivery(ctx context.Context, key messaging.DeliveryKey, plan messaging.DeliveryPlan, claim messaging.DeliveryClaim) (messaging.DeliveryRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if err := validateDeliveryPlan(key, plan); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if claim.Owner == "" || claim.TTL <= 0 {
		return messaging.DeliveryRecord{}, false, runtime.ErrCommitConflict
	}
	clientRequestID, err := messaging.StableDeliveryRequestID(key)
	if err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	record, exists := s.deliveries[key]
	if !exists {
		record = messaging.DeliveryRecord{DeliveryKey: key, State: messaging.DeliverySending, Plan: plan, Attempt: 1, Version: 1,
			ClientRequestID: clientRequestID, ClaimOwner: claim.Owner, ClaimUntil: now.Add(claim.TTL), UpdatedAt: now}
		s.deliveries[key] = record
		return record, true, nil
	}
	if record.Plan != plan || record.ClientRequestID != clientRequestID {
		return messaging.DeliveryRecord{}, false, runtime.ErrIdempotencyCollision
	}
	if record.State == messaging.DeliverySending && !record.ClaimUntil.After(now) {
		record.State, record.LastErrorClass = messaging.DeliveryAmbiguous, "owner_lost"
		record.ClaimOwner, record.ClaimUntil = "", time.Time{}
		record.Version, record.UpdatedAt = record.Version+1, now
		s.deliveries[key] = record
		return record, false, nil
	}
	if record.State != messaging.DeliveryPending && (record.State != messaging.DeliveryRetryWait || record.NotBefore.After(now)) {
		return record, false, nil
	}
	record.State, record.Attempt, record.Version, record.UpdatedAt = messaging.DeliverySending, record.Attempt+1, record.Version+1, now
	record.ClaimOwner, record.ClaimUntil = claim.Owner, now.Add(claim.TTL)
	s.deliveries[key] = record
	return record, true, nil
}

func (s *Store) FinishDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryAmbiguous && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.deliveries[in.DeliveryKey]
	if !exists || old.Version != expectedVersion || old.State != messaging.DeliverySending || old.Plan != in.Plan ||
		old.ClaimOwner == "" || old.ClaimOwner != in.ClaimOwner || old.ClientRequestID != in.ClientRequestID {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	in.Version = expectedVersion + 1
	in.Attempt = old.Attempt
	in.UpdatedAt = time.Now().UTC()
	s.deliveries[in.DeliveryKey] = in
	return in, nil
}

func (s *Store) RenewDeliveryClaim(ctx context.Context, in messaging.DeliveryRecord, ttl time.Duration) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	if ttl <= 0 || in.ClaimOwner == "" || in.ClientRequestID == "" {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	old, exists := s.deliveries[in.DeliveryKey]
	if !exists || old.Version != in.Version || old.State != messaging.DeliverySending || old.ClaimOwner != in.ClaimOwner ||
		old.ClientRequestID != in.ClientRequestID || !old.ClaimUntil.After(now) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	old.ClaimUntil, old.Version, old.UpdatedAt = now.Add(ttl), old.Version+1, now
	s.deliveries[in.DeliveryKey] = old
	return old, nil
}

func (s *Store) DeferDeliveryReconciliation(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	if in.State != messaging.DeliveryAmbiguous || in.ReconcileAttempt < 1 || in.NotBefore.IsZero() {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.deliveries[in.DeliveryKey]
	if !exists || old.Version != expectedVersion || old.State != messaging.DeliveryAmbiguous || old.Plan != in.Plan ||
		old.ClientRequestID != in.ClientRequestID || in.ReconcileAttempt != old.ReconcileAttempt+1 {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.Version, in.Attempt, in.UpdatedAt = expectedVersion+1, old.Attempt, time.Now().UTC()
	s.deliveries[in.DeliveryKey] = in
	return in, nil
}

func (s *Store) ReconcileDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.DeliveryRecord{}, err
	}
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.deliveries[in.DeliveryKey]
	if !exists || old.Version != expectedVersion || old.State != messaging.DeliveryAmbiguous || old.Plan != in.Plan || old.ClientRequestID != in.ClientRequestID {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	in.Version, in.Attempt, in.UpdatedAt = expectedVersion+1, old.Attempt, time.Now().UTC()
	s.deliveries[in.DeliveryKey] = in
	return in, nil
}

func validateDeliveryPlan(key messaging.DeliveryKey, plan messaging.DeliveryPlan) error {
	if key.TenantID == "" || key.DeliveryKey == "" || key.SegmentNo < 0 {
		return runtime.ErrTenantScope
	}
	if plan.RendererVersion == "" || plan.FormatVersion == "" || plan.ContentDigest == "" || plan.SegmentCount < 1 || key.SegmentNo >= plan.SegmentCount {
		return runtime.ErrCommitConflict
	}
	return nil
}

var _ messaging.InboxClaimer = (*Store)(nil)
var _ messaging.PayloadStore = (*Store)(nil)
var _ messaging.PreparedPayloadStore = (*Store)(nil)
var _ messaging.ResultStore = (*Store)(nil)
var _ messaging.ToolResultStore = (*Store)(nil)
var _ messaging.InteractionStore = (*Store)(nil)
var _ messaging.ReplyRouteStore = (*Store)(nil)
var _ messaging.DeliveryLedger = (*Store)(nil)
