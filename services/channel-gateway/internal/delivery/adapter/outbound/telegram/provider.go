// Package telegramadapter translates durable text Final attempts to the pinned
// Telegram Go SDK. It does not own credentials, execution authorization or claims.
package telegramadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

const maxCallTimeout = time.Minute

var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type Provider struct {
	accounts  map[string]*bot.Bot
	artifacts application.ArtifactReader
}

var _ application.SenderProvider = (*Provider)(nil)

// NewProvider copies a preconfigured immutable account-to-client mapping. It
// performs no authentication or HTTP request. The composition owner must supply
// authenticated clients with bounded transport and sanitized SDK error handlers,
// and must not mutate a client's token/configuration while this Provider uses it.
func NewProvider(accounts map[string]*bot.Bot, readers ...application.ArtifactReader) (*Provider, error) {
	p := &Provider{accounts: make(map[string]*bot.Bot, len(accounts))}
	if len(readers) > 1 {
		return nil, domain.ErrInvalid
	}
	if len(readers) == 1 {
		p.artifacts = readers[0]
	}
	for account, client := range accounts {
		if !idPattern.MatchString(account) || client == nil {
			return nil, domain.ErrInvalid
		}
		p.accounts[account] = client
	}
	return p, nil
}

// Reserve validates the fixed payload and returns one local, one-shot handle. It
// does not call Telegram and does not manufacture evidence that A2 committed.
func (p *Provider) Reserve(ctx context.Context, req application.SendRequest) (application.ReservedSender, error) {
	if ctx == nil {
		return nil, domain.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || req.Claim.Target.Provider != "telegram" || req.Claim.Part.State != domain.Claimed {
		return nil, domain.ErrInvalid
	}
	if err := (domain.CallingRequest{Claim: req.Claim, RequestID: req.RequestID, RequestDigest: req.RequestDigest, Timeout: time.Second}).Validate(); err != nil {
		return nil, err
	}
	client := p.accounts[req.Claim.Target.AccountID]
	if client == nil {
		return nil, domain.ErrUnavailable
	}
	// Telegram validation already rejects both mutable pointer fields (Origin and
	// Owner), so this value copy owns every captured payload and capability field.
	req.Claim.Intent.Attachments = append([]domain.Attachment(nil), req.Claim.Intent.Attachments...)
	return &reservation{client: client, request: req, artifacts: p.artifacts}, nil
}

type reservation struct {
	artifacts      application.ArtifactReader
	client         *bot.Bot
	request        application.SendRequest
	mu             sync.Mutex
	used, released bool
	cancel         context.CancelFunc
}

var _ application.ReservedSender = (*reservation)(nil)

func (r *reservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = true
	if r.cancel != nil {
		r.cancel()
	}
}
func notSent(class domain.ErrorClass) domain.Result {
	return domain.Result{Certainty: domain.CertaintyNotSent, ErrorClass: class}
}

func (r *reservation) SendFinal(parent context.Context, a domain.Attempt) domain.Result {
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return notSent(domain.ErrorStaleOrigin)
	}
	if r.used {
		r.mu.Unlock()
		return notSent(domain.ErrorPermanent)
	}
	r.used = true
	if parent == nil {
		r.mu.Unlock()
		return notSent(domain.ErrorPermanent)
	}
	ctx, cancel := context.WithTimeout(parent, maxCallTimeout)
	r.cancel = cancel
	r.mu.Unlock()
	defer cancel()
	if ctx.Err() != nil {
		return notSent(domain.ErrorDeadline)
	}
	if !r.matches(a) {
		return notSent(domain.ErrorPermanent)
	}
	deadline := a.Intent.Deadline
	if a.CallingUntil.Before(deadline) {
		deadline = a.CallingUntil
	}
	callCtx, stop := context.WithDeadline(ctx, deadline)
	defer stop()
	if callCtx.Err() != nil {
		return notSent(domain.ErrorDeadline)
	}
	chatID, err := strconv.ParseInt(a.Target.ConversationID, 10, 64)
	if err != nil {
		return notSent(domain.ErrorPermanent)
	}
	threadID := 0
	if a.Target.ThreadID != "" {
		threadID, err = strconv.Atoi(a.Target.ThreadID)
		if err != nil {
			return notSent(domain.ErrorPermanent)
		}
	}
	params := &bot.SendMessageParams{ChatID: chatID, MessageThreadID: threadID, Text: a.Text}
	if a.Target.SourceMessageID != "" {
		sourceID, err := strconv.Atoi(a.Target.SourceMessageID)
		if err != nil {
			return notSent(domain.ErrorPermanent)
		}
		params.ReplyParameters = &models.ReplyParameters{MessageID: sourceID, ChatID: chatID, AllowSendingWithoutReply: false}
	}
	// The certainty boundary is entering the SDK call. Subsequent network errors,
	// cancellation and malformed responses cannot prove that no bytes were sent.
	var message *models.Message
	documentSent := false
	if attachment, ok := domain.AttachmentAt(a.Target, a.Intent, r.request.Claim.Part.Index); ok {
		documentSent = true
		// Public Telegram Bot API multipart document limit (50 MB).
		if attachment.SizeBytes > 50*1024*1024 {
			return notSent(domain.ErrorPermanent)
		}
		if r.artifacts == nil {
			return notSent(domain.ErrorPermanent)
		}
		body, readErr := r.artifacts.ReadReplyArtifact(callCtx, a.Intent, attachment)
		if readErr != nil {
			if errors.Is(readErr, domain.ErrInvalid) || errors.Is(readErr, domain.ErrUnauthorized) {
				return notSent(domain.ErrorPermanent)
			}
			return notSent(domain.ErrorTemporary)
		}
		hash := sha256.Sum256(body)
		if int64(len(body)) != attachment.SizeBytes || hex.EncodeToString(hash[:]) != attachment.SHA256 {
			return notSent(domain.ErrorPermanent)
		}
		message, err = r.client.SendDocument(callCtx, &bot.SendDocumentParams{ChatID: chatID, MessageThreadID: threadID, ReplyParameters: params.ReplyParameters, Document: &models.InputFileUpload{Filename: attachment.Name, Data: bytes.NewReader(body)}})
	} else {
		message, err = r.client.SendMessage(callCtx, params)
	}
	if err != nil {
		return classify(err)
	}
	if documentSent && (message == nil || message.Document == nil || message.Document.FileID == "") {
		return domain.Result{Certainty: domain.CertaintyUnknown, ErrorClass: domain.ErrorPermanent}
	}
	if message == nil || message.ID <= 0 || message.Chat.ID != chatID || (threadID != 0 && message.MessageThreadID != threadID) {
		return domain.Result{Certainty: domain.CertaintyUnknown, ErrorClass: domain.ErrorPermanent}
	}
	return domain.Result{Certainty: domain.CertaintyAccepted, ProviderMessageID: strconv.Itoa(message.ID)}
}
func (r *reservation) matches(a domain.Attempt) bool {
	c := r.request.Claim
	if !idPattern.MatchString(a.ID) || !opaque(a.EvidenceToken, 256) || a.Number < 1 || a.CallingUntil.IsZero() || a.IntentID != c.Intent.ID || a.PartID != c.Part.ID || a.ClaimToken != c.Token || a.InstanceID != c.InstanceID || a.RequestID != r.request.RequestID || a.RequestDigest != r.request.RequestDigest || a.Owner != nil {
		return false
	}
	c.Intent = a.Intent
	c.Target = a.Target
	c.Part.Text = a.Text
	c.Owner = a.Owner
	c.InstanceID = a.InstanceID
	digest, err := domain.RequestDigest(c)
	return err == nil && digest == r.request.RequestDigest
}
func opaque(v string, max int) bool {
	if v == "" || len(v) > max || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

// classify returns only bounded semantic enums, never raw SDK descriptions,
// response bodies, HTTP URLs, migration targets or token-bearing error strings.
func classify(err error) domain.Result {
	var limited *bot.TooManyRequestsError
	if errors.As(err, &limited) || errors.Is(err, bot.ErrorTooManyRequests) {
		return domain.Result{Certainty: domain.CertaintyRejected, ErrorClass: domain.ErrorRateLimited}
	}
	var migrated *bot.MigrateError
	if errors.As(err, &migrated) || errors.Is(err, bot.ErrorBadRequest) || errors.Is(err, bot.ErrorForbidden) || errors.Is(err, bot.ErrorUnauthorized) || errors.Is(err, bot.ErrorNotFound) || errors.Is(err, bot.ErrorConflict) {
		return domain.Result{Certainty: domain.CertaintyRejected, ErrorClass: domain.ErrorPermanent}
	}
	class := domain.ErrorTemporary
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		class = domain.ErrorDeadline
	}
	return domain.Result{Certainty: domain.CertaintyUnknown, ErrorClass: class}
}
