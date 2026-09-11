package application

import (
	"context"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

// SenderOrigin comes from the first committed Admission, never a current socket
// substituted for an old callback. Epoch isolates newly constructed Clients.
type SenderOrigin struct {
	AccountID, InstanceID, RequestID string
	Epoch, Revision                  int64
	SocketGeneration                 uint64
}
type ReplyTarget struct {
	RequestID        string
	SocketGeneration uint64
}
type FinalCommand struct{ StreamID, Content string }
type Certainty string

const (
	NotSent  Certainty = "NOT_SENT"
	Accepted Certainty = "ACCEPTED"
	Rejected Certainty = "REJECTED"
	Unknown  Certainty = "UNKNOWN"
)

type SendCode string

const (
	SendOK          SendCode = ""
	SendInvalid     SendCode = "invalid_request"
	SendUnavailable SendCode = "not_ready"
	SendStale       SendCode = "stale_origin"
	SendQuiescing   SendCode = "quiescing"
	SendReleased    SendCode = "reservation_released"
	SendUsed        SendCode = "reservation_used"
	SendCanceled    SendCode = "canceled"
	SendCapacity    SendCode = "capacity_exceeded"
	SendUnknown     SendCode = "unclassified_failure"
)

// SendResult is call evidence only, not retry permission or ledger authorization.
type SendResult struct {
	Certainty    Certainty
	Code         SendCode
	ProviderCode *int64
}

// ReservedSender has one invocation. Release must follow bounded result/evidence
// persistence, not just the network return. It is safe to call Release repeatedly.
type ReservedSender interface {
	SendFinal(context.Context, FinalCommand) SendResult
	Release()
}

// FinalClient is an optional capability of a lifecycle Client. ReserveFinal is
// bounded, performs no Provider side effect, and counts toward Client.Drain.
type FinalClient interface {
	ReserveFinal(context.Context, ReplyTarget) (ReservedSender, error)
}

var (
	ErrSenderUnavailable = errors.New("connection sender unavailable")
	ErrStaleOrigin       = errors.New("connection reply origin stale")
	ErrSenderCapacity    = errors.New("connection sender capacity exceeded")
	ErrInvalidSender     = errors.New("connection sender input invalid")
)

func (o SenderOrigin) Validate() error {
	if domain.ValidateOwnerIdentity(o.AccountID, o.InstanceID, o.Epoch, o.Revision) != nil {
		return ErrInvalidSender
	}
	return (ReplyTarget{o.RequestID, o.SocketGeneration}).Validate()
}
func (r ReplyTarget) Validate() error {
	if r.SocketGeneration == 0 || r.RequestID == "" || len(r.RequestID) > 256 || !utf8.ValidString(r.RequestID) || strings.TrimSpace(r.RequestID) != r.RequestID {
		return ErrInvalidSender
	}
	for _, v := range r.RequestID {
		if unicode.IsControl(v) || v <= 32 {
			return ErrInvalidSender
		}
	}
	return nil
}
