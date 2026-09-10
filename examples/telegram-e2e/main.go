// Package main runs the Telegram end-to-end example.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelsinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"go.uber.org/zap"
)

const (
	defaultRunTimeout  = 2 * time.Minute
	defaultPollTimeout = 5 * time.Second
	shutdownTimeout    = 5 * time.Second
	e2eReply           = "telegram-e2e-ok"
)

type runConfig struct {
	botToken          string
	senderBotToken    string
	testMessage       string
	runTimeout        time.Duration
	pollTimeout       time.Duration
	deleteWebhook     bool
	dropPendingUpdate bool
}

type webhookClient interface {
	GetWebhookInfo(context.Context) (*models.WebhookInfo, error)
	DeleteWebhook(context.Context, *bot.DeleteWebhookParams) (bool, error)
}

type prepareBotFunc func(context.Context, string, time.Duration, bool, bool) (*models.User, error)

type deterministicDispatcher struct {
	marker string
	reply  string
	seen   chan gateway.InboundMessage
	once   sync.Once
}

var _ gateway.DispatchService = (*deterministicDispatcher)(nil)

func main() {
	restoreLogger, err := configureLogger(os.Stderr)
	if err != nil {
		_, _ = io.WriteString(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
	defer restoreLogger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv, os.Stdout); err != nil {
		packageLog.Error("telegram E2E failed", zap.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, lookup func(string) string, stdout io.Writer) error {
	return runWithPreflight(ctx, lookup, stdout, prepareBot)
}

func runWithPreflight(ctx context.Context, lookup func(string) string, stdout io.Writer, prepare prepareBotFunc) error {
	if ctx == nil || lookup == nil || stdout == nil {
		return errConfiguration
	}
	if prepare == nil {
		return errConfiguration
	}
	configuration, err := loadConfig(lookup)
	if err != nil {
		return err
	}
	runContext, cancel := context.WithTimeout(ctx, configuration.runTimeout)
	defer cancel()
	correlationID, err := newCorrelationID()
	if err != nil {
		return errConfiguration
	}
	reply := e2eReplyFor(correlationID)

	receiver, err := prepare(runContext, configuration.botToken, configuration.pollTimeout, configuration.deleteWebhook, configuration.dropPendingUpdate)
	if err != nil {
		return classifyPreflightResult(ctx.Err(), runContext.Err(), err)
	}
	target, err := newTrustedTarget(strconv.FormatInt(receiver.ID, 10))
	if err != nil {
		return errConfiguration
	}
	dispatcher := newDeterministicDispatcher(configuration.testMessage, reply)
	adapter, err := telegramAdapter(runContext, configuration, target, dispatcher)
	if err != nil {
		return classifyPreflightResult(ctx.Err(), runContext.Err(), err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- adapter.Run(runContext)
	}()

	packageLog.Info("telegram receiver listening", zap.String("username", receiver.Username), zap.Int64("receiver_id", receiver.ID))
	_, _ = fmt.Fprintf(stdout, "Send this ordinary text: %s\n", configuration.testMessage)

	var result error
	if configuration.senderBotToken != "" {
		senderResult := runAutomatedSender(runContext, configuration.senderBotToken, configuration.pollTimeout, receiver, configuration.testMessage, reply, configuration.deleteWebhook, configuration.dropPendingUpdate)
		result = classifyAutomatedSenderResult(ctx.Err(), runContext.Err(), senderResult)
		cancel()
		if stopErr := waitForAdapter(runDone); stopErr != nil && result == nil {
			result = stopErr
		}
	} else {
		select {
		case err := <-runDone:
			result = classifyManualRunResult(ctx.Err(), runContext.Err(), err)
		case <-runContext.Done():
			if stopErr := waitForAdapter(runDone); stopErr != nil {
				result = stopErr
			} else {
				result = classifyManualRunResult(ctx.Err(), runContext.Err(), nil)
			}
		}
	}
	cancel()
	if err := adapter.Close(); err != nil && result == nil {
		result = errAdapterClose
	}
	return result
}

func classifyPreflightResult(parentErr, runContextErr, preflightErr error) error {
	if parentErr != nil {
		return nil
	}
	if errors.Is(runContextErr, context.DeadlineExceeded) {
		return errRunTimeout
	}
	return preflightErr
}

func classifyManualRunResult(parentErr, runContextErr, adapterErr error) error {
	if parentErr != nil {
		return nil
	}
	if errors.Is(runContextErr, context.DeadlineExceeded) {
		return errRunTimeout
	}
	if errors.Is(runContextErr, context.Canceled) || errors.Is(adapterErr, context.Canceled) || errors.Is(adapterErr, context.DeadlineExceeded) {
		return nil
	}
	return errAdapterRun
}

func classifyAutomatedSenderResult(parentErr, runContextErr, senderErr error) error {
	if parentErr != nil {
		return nil
	}
	if errors.Is(runContextErr, context.DeadlineExceeded) {
		return errRunTimeout
	}
	if errors.Is(runContextErr, context.Canceled) {
		return nil
	}
	return senderErr
}

func loadConfig(lookup func(string) string) (runConfig, error) {
	if lookup == nil {
		return runConfig{}, errConfiguration
	}
	botToken := strings.TrimSpace(lookup("TELEGRAM_BOT_TOKEN"))
	if botToken == "" || hasControl(botToken) {
		return runConfig{}, errConfiguration
	}
	senderToken := strings.TrimSpace(lookup("TELEGRAM_SENDER_BOT_TOKEN"))
	if hasControl(senderToken) || (senderToken != "" && senderToken == botToken) {
		return runConfig{}, errConfiguration
	}
	message := strings.TrimSpace(lookup("TELEGRAM_TEST_MESSAGE"))
	if message == "" {
		message = fmt.Sprintf("telegram-e2e-%d", time.Now().UTC().UnixNano())
	}
	if hasControl(message) || len([]rune(message)) > 4096 || strings.Contains(message, botToken) || (senderToken != "" && strings.Contains(message, senderToken)) {
		return runConfig{}, errConfiguration
	}
	runTimeout, err := readDuration(lookup, "TELEGRAM_TIMEOUT", defaultRunTimeout)
	if err != nil || runTimeout <= 0 {
		return runConfig{}, errConfiguration
	}
	pollTimeout, err := readDuration(lookup, "TELEGRAM_POLL_TIMEOUT", defaultPollTimeout)
	if err != nil || pollTimeout < 2*time.Second || pollTimeout > 10*time.Minute {
		return runConfig{}, errConfiguration
	}
	deleteWebhook, err := readBool(lookup, "TELEGRAM_DELETE_WEBHOOK", false)
	if err != nil {
		return runConfig{}, errConfiguration
	}
	dropPending, err := readBool(lookup, "TELEGRAM_DROP_PENDING_UPDATES", false)
	if err != nil {
		return runConfig{}, errConfiguration
	}
	return runConfig{
		botToken: botToken, senderBotToken: senderToken, testMessage: message,
		runTimeout: runTimeout, pollTimeout: pollTimeout,
		deleteWebhook: deleteWebhook, dropPendingUpdate: dropPending,
	}, nil
}

func readDuration(lookup func(string) string, name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(lookup(name))
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func readBool(lookup func(string) string, name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(lookup(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseBool(value)
}

func prepareBot(ctx context.Context, token string, pollTimeout time.Duration, deleteWebhook, dropPending bool) (*models.User, error) {
	client, err := bot.New(token, bot.WithSkipGetMe(), bot.WithHTTPClient(pollTimeout, preflightHTTPClient(pollTimeout)))
	if err != nil {
		return nil, errPreflightClient
	}
	me, err := client.GetMe(ctx)
	if err != nil {
		return nil, classifyGetMeError(err)
	}
	if me == nil || !me.IsBot || me.ID <= 0 {
		return nil, errPreflightGetMeReply
	}
	if err := prepareLongPolling(ctx, client, deleteWebhook, dropPending); err != nil {
		return nil, err
	}
	return me, nil
}

func classifyGetMeError(err error) error {
	if err == nil {
		return errPreflightGetMeReply
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errPreflightGetMeTimeout
	}
	if errors.Is(err, bot.ErrorForbidden) || errors.Is(err, bot.ErrorBadRequest) || errors.Is(err, bot.ErrorUnauthorized) || errors.Is(err, bot.ErrorNotFound) || errors.Is(err, bot.ErrorConflict) || errors.Is(err, bot.ErrorTooManyRequests) {
		return errPreflightGetMeAPI
	}
	var tooManyRequestsError *bot.TooManyRequestsError
	if errors.As(err, &tooManyRequestsError) {
		return errPreflightGetMeAPI
	}
	var requestError *url.Error
	if errors.As(err, &requestError) {
		if requestError.Timeout() {
			return errPreflightGetMeTimeout
		}
		return errPreflightGetMeNetwork
	}
	return errPreflightGetMeReply
}

func classifyAdapterError(err error) error {
	switch {
	case errors.Is(err, telegram.ErrInvalid):
		return errAdapterConfiguration
	case errors.Is(err, telegram.ErrBotIdentityMismatch):
		return errAdapterIdentity
	case errors.Is(err, telegram.ErrInitialization):
		return errAdapterInitialization
	default:
		return errPreflight
	}
}

func preflightHTTPClient(pollTimeout time.Duration) *http.Client {
	return &http.Client{Timeout: pollTimeout + 5*time.Second}
}

func newCorrelationID() (string, error) {
	var nonce [8]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(nonce[:]), nil
}

func e2eReplyFor(correlationID string) string {
	return e2eReply + ":" + correlationID
}

func prepareLongPolling(ctx context.Context, client webhookClient, deleteWebhook, dropPending bool) error {
	if client == nil {
		return errPreflightWebhook
	}
	info, err := client.GetWebhookInfo(ctx)
	if err != nil {
		return errPreflightWebhook
	}
	if info == nil || info.URL == "" {
		return nil
	}
	if !deleteWebhook {
		return errWebhookConfigured
	}
	if _, err := client.DeleteWebhook(ctx, &bot.DeleteWebhookParams{DropPendingUpdates: dropPending}); err != nil {
		return errPreflightWebhook
	}
	return nil
}

func newTrustedTarget(providerAccountID string) (channels.RoutingTarget, error) {
	accountID, err := strconv.ParseInt(providerAccountID, 10, 64)
	if err != nil || accountID <= 0 || strconv.FormatInt(accountID, 10) != providerAccountID {
		return channels.RoutingTarget{}, errConfiguration
	}
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "telegram-e2e", DisplayName: "Telegram E2E Tenant",
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingStrict, TraceSamplingRate: 1,
	})
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{
		TenantID: root.TenantID, AppKey: "telegram-e2e", DisplayName: "Telegram E2E", Description: "Deterministic Telegram transport test",
	})
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	revision := int64(1)
	appRoot.Status = appmodel.StatusActive
	appRoot.CurrentRevision = &revision
	appRoot.Version++
	appRoot.UpdatedAt = appRoot.CreatedAt.Add(time.Second)
	if err := appRoot.Validate(); err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "telegram-e2e")
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	repository := channelsinmemory.NewRepository()
	// #nosec G101 -- deterministic verifier fixture, not a credential.
	secret := "telegram-e2e-verifier-secret"
	binding, _, err := repository.Create(context.Background(), channels.CreateInput{
		TenantID: root.TenantID, BindingKey: "telegram-e2e", Channel: channels.ChannelTelegram,
		ProviderAccountID: providerAccountID, PublicRouteKeyDigest: routeDigest, AppID: appRoot.AppID,
		SecretRef: "examples/telegram-e2e", // #nosec G101 -- symbolic fixture reference, not secret material.
		Status:    channels.StatusActive,
		Protocol:  channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}},
		Metadata:  exampleMetadata(),
	})
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	resolver := channelsinmemory.NewFakeCandidateResolver(repository, map[channels.SecretScope]string{{TenantID: binding.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := repository.LookupCandidates(context.Background(), channels.ChannelTelegram, routeDigest)
	if err != nil || len(candidates) != 1 {
		return channels.RoutingTarget{}, errConfiguration
	}
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{
		Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification,
	})
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	digest := sha256.Sum256([]byte("telegram-e2e-trusted-target"))
	verification := channels.VerificationRequest{
		Purpose: channels.PurposeWebhookVerification, Timestamp: time.Now().UTC(),
		Nonce: "telegram-e2e-target", MessageDigest: hex.EncodeToString(digest[:]),
	}
	verification.Signature = channelsinmemory.SignFakeRequest(secret, verification)
	verified, err := resolver.Verify(context.Background(), handle, verification)
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, appRoot, verified)
	if err != nil {
		return channels.RoutingTarget{}, errConfiguration
	}
	return target, nil
}

func exampleMetadata() channels.ChangeMetadata {
	return channels.ChangeMetadata{
		ActorType: "example", ActorID: "telegram-e2e", Reason: "live Telegram transport test",
		CorrelationID: "telegram-e2e",
	}
}

func telegramAdapter(ctx context.Context, configuration runConfig, target channels.RoutingTarget, dispatcher gateway.DispatchService) (*telegram.Adapter, error) {
	adapter, err := telegram.New(ctx, telegram.Config{
		BotToken: configuration.botToken, Target: target, Dispatcher: dispatcher,
		PollTimeout: configuration.pollTimeout,
		ErrorHook:   reportTelegramError,
	})
	if err != nil {
		return nil, classifyAdapterError(err)
	}
	return adapter, nil
}

func reportTelegramError(event telegram.ErrorEvent) {
	packageLog.Error("telegram operation failed", zap.String("operation", string(event.Operation)), zap.Error(event.Err))
}

func newDeterministicDispatcher(marker, reply string) *deterministicDispatcher {
	return &deterministicDispatcher{marker: marker, reply: reply, seen: make(chan gateway.InboundMessage, 1)}
}

func (dispatcher *deterministicDispatcher) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	if dispatcher == nil || ctx == nil {
		return nil, errConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Message.Content == dispatcher.marker {
		dispatcher.once.Do(func() {
			select {
			case dispatcher.seen <- request.Message:
			default:
			}
		})
	}
	events := make(chan gateway.DispatchEvent, 2)
	if request.Message.Content == dispatcher.marker {
		events <- gateway.DispatchEvent{Type: gateway.DispatchEventMessage, RequestID: request.RequestID, Text: dispatcher.reply}
	}
	events <- gateway.DispatchEvent{Type: gateway.DispatchEventDone, RequestID: request.RequestID, Status: "complete", Done: true}
	close(events)
	return events, nil
}

func runAutomatedSender(ctx context.Context, token string, pollTimeout time.Duration, receiver *models.User, marker, reply string, deleteWebhook, dropPending bool) error {
	if receiver == nil || receiver.ID <= 0 || receiver.Username == "" {
		return errSender
	}
	replyReceived := make(chan struct{}, 1)
	pollingFailed := make(chan struct{}, 1)
	sender, err := newAutomatedSender(token, pollTimeout, receiver.ID, reply, replyReceived, pollingFailed)
	if err != nil {
		return errSender
	}
	senderUser, err := sender.GetMe(ctx)
	if err != nil || senderUser == nil || !senderUser.IsBot || senderUser.ID <= 0 || senderUser.ID == receiver.ID {
		return errSender
	}
	if err := prepareLongPolling(ctx, sender, deleteWebhook, dropPending); err != nil {
		return errSender
	}
	return runSender(ctx, sender, receiver.Username, marker, replyReceived, pollingFailed)
}

func newAutomatedSender(token string, pollTimeout time.Duration, receiverID int64, reply string, replyReceived, pollingFailed chan<- struct{}) (*bot.Bot, error) {
	return bot.New(token,
		bot.WithSkipGetMe(),
		bot.WithDefaultHandler(func(_ context.Context, _ *bot.Bot, update *models.Update) {
			if isExpectedAutomatedReply(update, receiverID, reply) {
				select {
				case replyReceived <- struct{}{}:
				default:
				}
			}
		}),
		bot.WithNotAsyncHandlers(),
		bot.WithErrorsHandler(func(error) {
			select {
			case pollingFailed <- struct{}{}:
			default:
			}
		}),
		bot.WithHTTPClient(pollTimeout, preflightHTTPClient(pollTimeout)),
	)
}

func runSender(ctx context.Context, sender *bot.Bot, receiverUsername, marker string, replyReceived, pollingFailed <-chan struct{}) error {
	senderContext, cancel := context.WithCancel(ctx)
	senderDone := make(chan struct{})
	go func() {
		sender.Start(senderContext)
		close(senderDone)
	}()
	if _, err := sender.SendMessage(ctx, &bot.SendMessageParams{ChatID: "@" + receiverUsername, Text: marker}); err != nil {
		cancel()
		waitForSender(senderDone)
		return errSender
	}
	select {
	case <-replyReceived:
		cancel()
		if !waitForSender(senderDone) {
			return errSenderStopped
		}
		return nil
	case <-pollingFailed:
		cancel()
		waitForSender(senderDone)
		return errSender
	case <-senderDone:
		cancel()
		return errSenderStopped
	case <-ctx.Done():
		cancel()
		waitForSender(senderDone)
		return errSender
	}
}

func isExpectedAutomatedReply(update *models.Update, receiverID int64, reply string) bool {
	return update != nil && update.Message != nil && update.Message.From != nil &&
		update.Message.From.ID == receiverID && update.Message.Chat.ID == receiverID && update.Message.Text == reply
}

func waitForSender(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-time.After(shutdownTimeout):
		return false
	}
}

func waitForAdapter(done <-chan error) error {
	if err := <-done; err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return errAdapterRun
	}
	return nil
}

func hasControl(value string) bool {
	return strings.ContainsAny(value, "\r\n\x00")
}
