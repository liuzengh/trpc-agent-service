package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// IntakeRequest is the untrusted Test Channel input before binding resolution.
type IntakeRequest struct {
	Media             *runtimecontext.MediaReference
	BindingKey        string
	ExternalMessageID string
	UserID            string
	SessionID         string
	ChatType          string
	Text              string
	ReplyTarget       string
	// AuthorizeScope is an internal pre-persistence check, never an HTTP field.
	AuthorizeScope func(runtimecontext.Scope) error
	// DirectReply is platform-authored unsupported-media/control feedback.
	DirectReply string
	rateChecked bool
}

// Intake resolves a binding and durably accepts its normalized message.
type Intake struct {
	resolver routing.Resolver
	journal  Journal
	audit    audit.Writer
	metrics  *platformmetrics.Recorder
	quota    *tenant.Guard
}

type IntakeOption func(*Intake)

func WithAuditWriter(writer audit.Writer) IntakeOption {
	return func(intake *Intake) { intake.audit = writer }
}

func WithMetrics(recorder *platformmetrics.Recorder) IntakeOption {
	return func(intake *Intake) { intake.metrics = recorder }
}

func WithQuotaGuard(guard *tenant.Guard) IntakeOption {
	return func(intake *Intake) { intake.quota = guard }
}

func NewIntake(
	resolver routing.Resolver,
	journal Journal,
	opts ...IntakeOption,
) (*Intake, error) {
	if resolver == nil {
		return nil, fmt.Errorf("gateway route resolver is required")
	}
	if journal == nil {
		return nil, fmt.Errorf("gateway inbound journal is required")
	}
	intake := &Intake{resolver: resolver, journal: journal}
	for _, opt := range opts {
		if opt != nil {
			opt(intake)
		}
	}
	return intake, nil
}

func (i *Intake) Accept(ctx context.Context, input IntakeRequest) (AcceptResult, error) {
	ctx, span := otel.Tracer("trpc-agent-service/gateway").Start(ctx, "gateway.accept")
	defer span.End()
	input.BindingKey = strings.TrimSpace(input.BindingKey)
	if input.BindingKey == "" {
		return AcceptResult{}, fmt.Errorf("binding key is required")
	}
	scope, err := i.resolveScope(ctx, input.BindingKey, input.UserID, input.SessionID)
	if err != nil {
		return AcceptResult{}, err
	}
	if input.AuthorizeScope != nil {
		if err := input.AuthorizeScope(scope); err != nil {
			return AcceptResult{}, err
		}
	}
	span.SetAttributes(attribute.String("tenant.id", scope.TenantID), attribute.String("agent.app.id", scope.AppID))
	if i.quota != nil && !input.rateChecked {
		if err := i.quota.AllowInbound(ctx, scope.TenantID, input.UserID); err != nil {
			if i.audit != nil {
				_ = i.audit.Record(ctx, audit.Event{
					TenantID: scope.TenantID, Channel: scope.ChannelType,
					ChannelBindingID: scope.ChannelBindingID,
					UserID:           input.UserID, SessionID: input.SessionID,
					MessageID: input.ExternalMessageID,
					TraceID:   audit.TraceID(ctx), Decision: "inbound_rate_rejected",
					ErrorType: "tenant_quota",
				})
			}
			return AcceptResult{}, err
		}
	}
	result, err := i.journal.Accept(ctx, InboundRequest{
		Media:             input.Media,
		Scope:             scope,
		ExternalMessageID: input.ExternalMessageID,
		UserID:            input.UserID,
		SessionID:         input.SessionID,
		ChatType:          input.ChatType,
		Text:              input.Text,
		ReplyTarget:       input.ReplyTarget,
		DirectReply:       input.DirectReply,
	})
	if err != nil {
		return AcceptResult{}, err
	}
	if i.audit != nil {
		decision := "inbound_accepted"
		if result.Duplicate {
			decision = "inbound_duplicate"
		}
		if err := i.audit.Record(ctx, audit.Event{
			TenantID:         scope.TenantID,
			Channel:          scope.ChannelType,
			ChannelBindingID: scope.ChannelBindingID,
			UserID:           input.UserID,
			SessionID:        input.SessionID,
			MessageID:        input.ExternalMessageID,
			RequestID:        result.RequestID,
			RevisionID:       result.RevisionID,
			TraceID:          audit.TraceID(ctx),
			Decision:         decision,
		}); err != nil {
			return AcceptResult{}, fmt.Errorf("write inbound audit: %w", err)
		}
	}
	i.metrics.RecordInbound(ctx, scope.TenantID, scope.ChannelType, result.Duplicate)
	return result, nil
}

func (i *Intake) resolveScope(ctx context.Context, bindingKey, userID, sessionID string) (runtimecontext.Scope, error) {
	if resolver, ok := i.resolver.(routing.RequestResolver); ok {
		return resolver.ResolveFor(ctx, bindingKey, userID+"\x00"+sessionID)
	}
	return i.resolver.Resolve(ctx, bindingKey)
}

func (i *Intake) Ready(ctx context.Context) error {
	if i == nil || i.journal == nil {
		return ErrJournalClosed
	}
	return i.journal.Ready(ctx)
}

func (i *Intake) Close() error {
	if i == nil || i.journal == nil {
		return nil
	}
	return i.journal.Close()
}
