package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	senderTypeUser = "user"

	chatTypeP2P = "p2p"

	acceptTimeout = 2 * time.Second

	admissionCapacity = 16
)

// Adapter is this package's half of a text channel: it hands the durable pipeline what
// the connection accepted, and puts one final reply back on the conversation that asked
// for it.
type Adapter struct {
	client *Client

	identity channels.BindingIdentity

	ready func()

	served atomic.Bool
}

// A change to either side of the contract is a compile error here rather than a
// process that starts and cannot answer.
var _ channels.TextAdapter = (*Adapter)(nil)

// NewAdapter wraps a confirmed Client. The identity comes from the Client's own
// binding, so an adapter whose identity and connection disagree cannot be built.
func NewAdapter(client *Client, ready func()) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: an adapter needs a client", ErrConfig)
	}
	if client.tenantKey == "" {
		return nil, fmt.Errorf("%w: an adapter needs a confirmed enterprise", ErrConfig)
	}
	return &Adapter{
		client: client,
		identity: channels.BindingIdentity{
			TenantID:   client.binding.TenantID,
			AgentAppID: client.binding.AgentAppID,
			BindingID:  client.binding.BindingID,
			Channel:    channels.ChannelFeishu,
		},
		ready: ready,
	}, nil
}

// Identity is the static trust anchor this adapter runs on.
func (a *Adapter) Identity() channels.BindingIdentity { return a.identity }

// ReplyTextLimit is the largest reply this adapter will put on the wire.
func (a *Adapter) ReplyTextLimit() int { return maxReplyTextBytes }

// Serve runs the official long connection and records what arrives on it.
func (a *Adapter) Serve(ctx context.Context, accept channels.AcceptFunc) error {
	if accept == nil {
		return fmt.Errorf("%w: serve needs somewhere to record what it accepted", ErrConfig)
	}

	if !a.served.CompareAndSwap(false, true) {
		return ErrServing
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	session := &serveSession{
		adapter:   a,
		accept:    accept,
		cancel:    cancel,
		admission: make(chan struct{}, admissionCapacity),
		gate:      make(chan struct{}, 1),
	}

	events := larkdispatcher.NewEventDispatcher("", "")
	events.InitConfig(larkevent.WithLogger(silentLogger{}))
	events.OnP2MessageReceiveV1(session.handle)
	connection := larkws.NewClient(a.client.binding.AppID, a.client.secret,
		larkws.WithEventHandler(events),

		larkws.WithLogger(silentLogger{}),
		larkws.WithDomain(a.client.api.baseURL),
		// The bootstrap POST carries the app secret, so it goes through the same
		// client as every other call: fixed timeout, no redirect followed.
		larkws.WithHttpClient(a.client.api.client),
		larkws.WithOnReady(a.notifyReady),
	)

	_ = connection.Start(runCtx)
	if err := session.acceptError(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrConnection
}

func (a *Adapter) notifyReady() {
	if a.ready != nil {
		a.ready()
	}
}

// serveSession is the state of one Serve: who to hand events to, how many may be
// in flight, and the first refusal, which ends the run.
type serveSession struct {
	adapter *Adapter
	accept  channels.AcceptFunc
	cancel  context.CancelFunc

	admission chan struct{}
	gate      chan struct{}

	once   sync.Once
	failed atomic.Bool
	mu     sync.Mutex
	err    error
}

func (s *serveSession) fail(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		s.failed.Store(true)

		s.cancel()
	})
}

func (s *serveSession) acceptError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *serveSession) handle(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
	envelope, ok := s.adapter.normalize(event, time.Now().UTC())
	if !ok {

		return nil
	}

	callCtx, cancel := context.WithTimeout(ctx, acceptTimeout)
	defer cancel()
	select {
	case s.admission <- struct{}{}:
	default:
		return errNotAccepted
	}
	defer func() { <-s.admission }()
	select {
	case s.gate <- struct{}{}:
	case <-callCtx.Done():
		return errNotAccepted
	}
	defer func() { <-s.gate }()

	if callCtx.Err() != nil || s.failed.Load() {
		return errNotAccepted
	}
	if err := s.accept(callCtx, envelope); err != nil {
		s.fail(err)
		return errNotAccepted
	}
	return nil
}

func (a *Adapter) normalize(
	event *larkim.P2MessageReceiveV1,
	now time.Time,
) (channels.InboundEnvelope, bool) {
	none := channels.InboundEnvelope{}
	if event == nil || event.EventV2Base == nil || event.EventV2Base.Header == nil ||
		event.Event == nil {
		return none, false
	}

	header := event.EventV2Base.Header
	binding := a.client.binding

	if header.AppID != binding.AppID || header.TenantKey != a.client.tenantKey {
		return none, false
	}
	sender, message := event.Event.Sender, event.Event.Message
	if sender == nil || message == nil || sender.SenderId == nil {
		return none, false
	}
	if value(sender.SenderType) != senderTypeUser ||
		value(sender.TenantKey) != a.client.tenantKey {
		return none, false
	}
	openID := value(sender.SenderId.OpenId)
	chatID := value(message.ChatId)
	messageID := value(message.MessageId)
	if !checkExternalID(openID) || !checkExternalID(chatID) || !checkExternalID(messageID) {
		return none, false
	}
	if value(message.ChatType) != chatTypeP2P || value(message.MessageType) != messageTypeText {
		return none, false
	}
	text, ok := decodeInboundText(value(message.Content))
	if !ok {
		return none, false
	}
	target, err := encodeTarget(binding, a.client.tenantKey, messageID)
	if err != nil {
		return none, false
	}
	principal := principalID(binding, a.client.tenantKey, openID)
	envelope := channels.InboundEnvelope{
		TenantID:         a.identity.TenantID,
		Channel:          a.identity.Channel,
		ChannelBindingID: a.identity.BindingID,
		AgentAppID:       a.identity.AgentAppID,
		PrincipalID:      principal,
		SessionID:        directSessionID(binding, a.client.tenantKey, principal, chatID),
		// The platform's own message id, unchanged: what the pipeline
		// deduplicates a redelivery on.
		ExternalEventID: messageID,

		ReceivedAt:     now,
		Message:        channels.InboundMessage{Text: text},
		DeliveryTarget: target,
	}

	if err := envelope.Validate(a.identity.Scope()); err != nil {
		return none, false
	}
	return envelope, true
}

func decodeInboundText(content string) (string, bool) {
	if len(content) > maxInboundTextBytes {
		return "", false
	}
	var body struct {
		Text *string `json:"text"`
	}
	if err := json.Unmarshal([]byte(content), &body); err != nil || body.Text == nil {
		return "", false
	}
	text := *body.Text
	if strings.TrimSpace(text) == "" || len(text) > maxInboundTextBytes || !utf8.ValidString(text) {
		return "", false
	}
	return text, true
}

func value(field *string) string {
	if field == nil {
		return ""
	}
	return *field
}

// Send makes one attempt to put one recorded answer on the wire.
func (a *Adapter) Send(
	ctx context.Context,
	target channels.DeliveryTarget,
	message channels.OutboundMessage,
) channels.DeliveryReport {
	messageID, err := decodeTarget(a.client.binding, a.client.tenantKey, target)
	if err != nil {

		return channels.DeliveryReport{Outcome: channels.DeliveryTargetStale}
	}
	if err := message.Validate(); err != nil ||
		len(message.Text) > maxReplyTextBytes || !utf8.ValidString(message.Text) {

		return channels.DeliveryReport{Outcome: channels.DeliveryRejected}
	}
	token, err := a.client.api.tenantAccessToken(ctx, a.client.binding.AppID, a.client.secret)
	if err != nil {

		return channels.DeliveryReport{Outcome: channels.DeliveryUnknown}
	}
	return a.client.api.reply(ctx, token, messageID,
		replyUUID(a.client.binding, a.client.tenantKey, messageID), message.Text)
}

// silentLogger is what both SDK layers log through.
type silentLogger struct{}

var _ larkcore.Logger = silentLogger{}

func (silentLogger) Debug(context.Context, ...interface{}) {}
func (silentLogger) Info(context.Context, ...interface{})  {}
func (silentLogger) Warn(context.Context, ...interface{})  {}
func (silentLogger) Error(context.Context, ...interface{}) {}
