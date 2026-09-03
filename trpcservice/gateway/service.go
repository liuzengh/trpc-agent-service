package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
)

// IntakeRequest is the untrusted Test Channel input before binding resolution.
type IntakeRequest struct {
	BindingKey        string
	ExternalMessageID string
	UserID            string
	SessionID         string
	ChatType          string
	Text              string
	ReplyTarget       string
}

// Intake resolves a binding and durably accepts its normalized message.
type Intake struct {
	resolver routing.Resolver
	journal  Journal
}

func NewIntake(resolver routing.Resolver, journal Journal) (*Intake, error) {
	if resolver == nil {
		return nil, fmt.Errorf("Gateway route resolver is required")
	}
	if journal == nil {
		return nil, fmt.Errorf("Gateway inbound journal is required")
	}
	return &Intake{resolver: resolver, journal: journal}, nil
}

func (i *Intake) Accept(ctx context.Context, input IntakeRequest) (AcceptResult, error) {
	input.BindingKey = strings.TrimSpace(input.BindingKey)
	if input.BindingKey == "" {
		return AcceptResult{}, fmt.Errorf("binding key is required")
	}
	scope, err := i.resolver.Resolve(ctx, input.BindingKey)
	if err != nil {
		return AcceptResult{}, err
	}
	return i.journal.Accept(ctx, InboundRequest{
		Scope:             scope,
		ExternalMessageID: input.ExternalMessageID,
		UserID:            input.UserID,
		SessionID:         input.SessionID,
		ChatType:          input.ChatType,
		Text:              input.Text,
		ReplyTarget:       input.ReplyTarget,
	})
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
