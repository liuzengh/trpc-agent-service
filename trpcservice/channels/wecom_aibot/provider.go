package wecom_aibot

import (
	"context"
	"strconv"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// Provider delivers durable final replies through the manager's current
// connection. Partial stream events are intentionally excluded from this
// provider; only the durable final segment is retried after reconnect.
type Provider struct {
	manager *Manager
	store   DeliveryStore
}

var _ outbox.Provider = (*Provider)(nil)

// DeliveryStore supplies the durable correlation and acknowledgement records
// required to recover a final reply after a process restart.
type DeliveryStore interface {
	storage.ReplyCorrelationStore
	storage.ReplyReceiptRecorder
}

// NewProvider creates the durable final-reply adapter for one manager.
func NewProvider(manager *Manager, store DeliveryStore) (*Provider, error) {
	if manager == nil || store == nil {
		return nil, ErrInvalid
	}
	return &Provider{manager: manager, store: store}, nil
}

// Deliver sends and acknowledges a durable final reply for its correlated request.
func (p *Provider) Deliver(ctx context.Context, value storage.ReplyOutbox) (string, error) {
	if p == nil || p.manager == nil || p.store == nil || ctx == nil || strings.TrimSpace(value.Payload) == "" || value.ReplyID == "" || value.LeaseOwner == "" || value.FencingToken <= 0 {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	if receipt := strings.TrimSpace(value.ProviderMessageID); receipt != "" {
		return receipt, nil
	}
	if !p.manager.Ready() {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	correlation, err := p.store.GetReplyCorrelation(ctx, value.TenantID, value.EventID)
	if err != nil || strings.TrimSpace(correlation.RequestID) == "" {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	reqID := correlation.RequestID
	body := StreamReply{MsgType: "stream", Stream: struct {
		ID      string `json:"id"`
		Finish  bool   `json:"finish"`
		Content string `json:"content,omitempty"`
	}{ID: reqID, Finish: value.SegmentIndex+1 == value.SegmentCount, Content: value.Payload}}
	if err := p.manager.sendReply(ctx, reqID, body); err != nil {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	receipt := reqID + ":" + strconv.Itoa(value.SegmentIndex)
	if _, err := p.store.RecordReplyReceipt(ctx, storage.ReplyReceipt{TenantID: value.TenantID, ReplyID: value.ReplyID, SegmentIndex: value.SegmentIndex, Owner: value.LeaseOwner, FencingToken: value.FencingToken, ProviderID: receipt}); err != nil {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	return receipt, nil
}

// Reconcile reports durably acknowledged replies as accepted.
func (p *Provider) Reconcile(_ context.Context, value storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if p == nil || p.store == nil {
		return outbox.DeliveryUnknown, "", nil
	}
	if receipt := strings.TrimSpace(value.ProviderMessageID); receipt != "" {
		return outbox.DeliveryAccepted, receipt, nil
	}
	return outbox.DeliveryUnknown, "", nil
}
