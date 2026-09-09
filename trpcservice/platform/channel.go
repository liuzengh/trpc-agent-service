package platform

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	ChannelMock             = "mock"
	ChannelEnterpriseWeChat = "enterprise_wechat"
	ChannelTelegram         = "telegram"
	ConversationSingle      = "single"
	ConversationGroup       = "group"
)

type ChannelBinding struct {
	ID               string    `json:"id"`
	TenantID         string    `json:"tenant_id"`
	AppID            string    `json:"app_id"`
	Channel          string    `json:"channel"`
	ProviderAccount  string    `json:"provider_account,omitempty"`
	ConversationType string    `json:"conversation_type"`
	ConversationID   string    `json:"external_conversation_id"`
	UserID           string    `json:"external_user_id"`
	SessionID        string    `json:"session_id"`
	Secret           string    `json:"secret,omitempty"`
	Enabled          bool      `json:"enabled"`
	CreatedAt        time.Time `json:"created_at"`
	ReplyReference   string    `json:"-"`
	ProviderSession  string    `json:"-"`
	PlatformOwned    bool      `json:"-"`
	ReplayProvider   string    `json:"-"`
}

type HMACChannelSignature struct{}

func (HMACChannelSignature) Verify(_ context.Context, credential ChannelCredential, body []byte, signature string) error {
	if credential.Secret == "" || signature == "" {
		return errors.New("platform: channel signature is required")
	}
	mac := hmac.New(sha256.New, []byte(credential.Secret))
	_, _ = mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(signature)), []byte(expected)) {
		return errors.New("platform: invalid channel signature")
	}
	return nil
}

type channelError struct{ code string }

func (e channelError) Error() string { return e.code }

func channelErrorCode(err error) string {
	var channelErr channelError
	if errors.As(err, &channelErr) {
		return channelErr.code
	}
	return "channel_unavailable"
}

type MockFaultConfiguration struct {
	Scenario            string `json:"scenario"`
	MessageLengthLimit  int    `json:"message_length_limit"`
	AttachmentSizeLimit int    `json:"attachment_size_limit"`
	RateLimit           int    `json:"rate_limit"`
	TimeoutMilliseconds int    `json:"timeout_ms"`
	RetryLimit          int    `json:"retry_limit"`
}

type ChannelCoordinator struct {
	mu               sync.RWMutex
	bindings         map[string]ChannelBinding
	lastSequences    map[string]uint64
	acceptedMessages map[string]struct{}
	adapter          ChannelAdapter
	adapters         map[string]ChannelAdapter
	load             func(context.Context) (map[string]ChannelBinding, error)
	mutate           func(context.Context, func(map[string]ChannelBinding) error) (map[string]ChannelBinding, error)
}

func (c *ChannelCoordinator) configurePersistence(load func(context.Context) (map[string]ChannelBinding, error), mutate func(context.Context, func(map[string]ChannelBinding) error) (map[string]ChannelBinding, error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.load = load
	c.mutate = mutate
}

func (c *ChannelCoordinator) refreshLocked(ctx context.Context) error {
	if c.load == nil {
		return nil
	}
	bindings, err := c.load(ctx)
	if err != nil {
		return err
	}
	c.bindings = bindings
	return nil
}

func (c *ChannelCoordinator) mutateLocked(ctx context.Context, mutate func(map[string]ChannelBinding) error) error {
	if c.mutate == nil {
		return mutate(c.bindings)
	}
	bindings, err := c.mutate(ctx, mutate)
	if err != nil {
		return err
	}
	c.bindings = bindings
	return nil
}

func NewChannelCoordinator(adapter ChannelAdapter) *ChannelCoordinator {
	return &ChannelCoordinator{
		bindings:         make(map[string]ChannelBinding),
		lastSequences:    make(map[string]uint64),
		acceptedMessages: make(map[string]struct{}),
		adapter:          adapter,
		adapters:         map[string]ChannelAdapter{ChannelMock: adapter},
	}
}

// RegisterAdapter adds a provider without changing the stable ChannelAdapter
// contract used by the chat runtime.
func (c *ChannelCoordinator) RegisterAdapter(channel string, adapter ChannelAdapter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.adapters == nil {
		c.adapters = make(map[string]ChannelAdapter)
	}
	c.adapters[channel] = adapter
}

func (c *ChannelCoordinator) ConfigureMockFaults(tenantID, sessionID, scenario string) (MockFaultConfiguration, error) {
	mock, ok := c.adapter.(*MockChannel)
	if !ok {
		return MockFaultConfiguration{}, errors.New("platform: mock channel unavailable")
	}
	return mock.ConfigureFaults(tenantID, sessionID, scenario)
}

func (c *ChannelCoordinator) MockFaults(tenantID, sessionID string) MockFaultConfiguration {
	mock, ok := c.adapter.(*MockChannel)
	if !ok {
		return MockFaultConfiguration{Scenario: "none"}
	}
	return mock.Faults(tenantID, sessionID)
}

func (c *ChannelCoordinator) CreateBinding(ctx context.Context, tenant TenantContext, request createChannelBindingRequest) (ChannelBinding, error) {
	if request.Channel != ChannelMock {
		return ChannelBinding{}, errors.New("platform: unsupported channel")
	}
	if request.ConversationType != ConversationSingle && request.ConversationType != ConversationGroup {
		return ChannelBinding{}, errors.New("platform: invalid conversation type")
	}
	sessionID := request.SessionID
	if sessionID != "" && !validResourceID(sessionID) {
		return ChannelBinding{}, errors.New("platform: invalid session id")
	}
	secret := strings.TrimSpace(request.Secret)
	if secret == "" {
		secretBytes := make([]byte, 32)
		if _, err := rand.Read(secretBytes); err != nil {
			return ChannelBinding{}, err
		}
		secret = hex.EncodeToString(secretBytes)
	}
	if len(secret) < 8 || len(secret) > 512 {
		return ChannelBinding{}, errors.New("platform: invalid channel secret")
	}
	idBytes := make([]byte, 8)
	if _, err := rand.Read(idBytes); err != nil {
		return ChannelBinding{}, err
	}
	binding := ChannelBinding{
		ID: "binding-" + hex.EncodeToString(idBytes), TenantID: tenant.TenantID, AppID: request.AppID,
		Channel: request.Channel, ConversationType: request.ConversationType, ConversationID: request.ConversationID,
		UserID: request.UserID, SessionID: sessionID, Secret: secret,
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if binding.SessionID == "" {
		binding.SessionID = "chat-" + hex.EncodeToString(idBytes)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.mutateLocked(ctx, func(bindings map[string]ChannelBinding) error {
		for _, existing := range bindings {
			if existing.TenantID == binding.TenantID && existing.Channel == binding.Channel && existing.ConversationID == binding.ConversationID && existing.AppID == binding.AppID {
				return ErrDuplicateEvent
			}
		}
		bindings[binding.ID] = binding
		return nil
	}); err != nil {
		return ChannelBinding{}, err
	}
	return binding, nil
}

func (c *ChannelCoordinator) UpdateBinding(ctx context.Context, tenantID, id string, enabled bool) (ChannelBinding, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var binding ChannelBinding
	if err := c.mutateLocked(ctx, func(bindings map[string]ChannelBinding) error {
		var ok bool
		binding, ok = bindings[id]
		if !ok || binding.TenantID != tenantID {
			return ErrNotFound
		}
		binding.Enabled = enabled
		bindings[id] = binding
		return nil
	}); err != nil {
		return ChannelBinding{}, err
	}
	binding.Secret = ""
	return binding, nil
}

func (c *ChannelCoordinator) ReplaceSecret(ctx context.Context, tenantID, id, secret string) (ChannelBinding, error) {
	secret = strings.TrimSpace(secret)
	if len(secret) < 8 || len(secret) > 512 {
		return ChannelBinding{}, errors.New("platform: invalid channel secret")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var binding ChannelBinding
	if err := c.mutateLocked(ctx, func(bindings map[string]ChannelBinding) error {
		var ok bool
		binding, ok = bindings[id]
		if !ok || binding.TenantID != tenantID {
			return ErrNotFound
		}
		binding.Secret = secret
		bindings[id] = binding
		return nil
	}); err != nil {
		return ChannelBinding{}, err
	}
	binding.Secret = ""
	return binding, nil
}

func (c *ChannelCoordinator) DeleteBinding(ctx context.Context, tenantID, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mutateLocked(ctx, func(bindings map[string]ChannelBinding) error {
		binding, ok := bindings[id]
		if !ok || binding.TenantID != tenantID {
			return ErrNotFound
		}
		delete(bindings, id)
		return nil
	})
}

func (c *ChannelCoordinator) Binding(ctx context.Context, tenantID, id string) (ChannelBinding, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.refreshLocked(ctx); err != nil {
		return ChannelBinding{}, false, err
	}
	binding, ok := c.bindings[id]
	return binding, ok && binding.TenantID == tenantID, nil
}

func (c *ChannelCoordinator) BindingForSession(ctx context.Context, tenantID, sessionID string) (ChannelBinding, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.refreshLocked(ctx); err != nil {
		return ChannelBinding{}, false, err
	}
	for _, binding := range c.bindings {
		if binding.TenantID == tenantID && binding.SessionID == sessionID {
			return binding, true, nil
		}
	}
	return ChannelBinding{}, false, nil
}

func (c *ChannelCoordinator) ListBindings(ctx context.Context, tenantID string) ([]ChannelBinding, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.refreshLocked(ctx); err != nil {
		return nil, err
	}
	items := []ChannelBinding{}
	for _, binding := range c.bindings {
		if binding.TenantID == tenantID {
			item := binding
			item.Secret = ""
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

func (c *ChannelCoordinator) Receive(ctx context.Context, tenantID string, callback ChannelCallback) (ChannelBinding, ChannelMessage, error) {
	binding, ok, err := c.Binding(ctx, tenantID, callback.BindingID)
	if err != nil {
		return ChannelBinding{}, ChannelMessage{}, err
	}
	if !ok {
		return ChannelBinding{}, ChannelMessage{}, ErrNotFound
	}
	if !binding.Enabled {
		return binding, ChannelMessage{}, channelError{code: "channel_disabled"}
	}
	return c.receiveForBinding(ctx, binding, callback)
}

func (c *ChannelCoordinator) ReceiveExternal(ctx context.Context, callback ChannelCallback) (ChannelBinding, ChannelMessage, error) {
	c.mu.Lock()
	if err := c.refreshLocked(ctx); err != nil {
		c.mu.Unlock()
		return ChannelBinding{}, ChannelMessage{}, err
	}
	binding, ok := c.bindings[callback.BindingID]
	c.mu.Unlock()
	if !ok {
		return ChannelBinding{}, ChannelMessage{}, ErrNotFound
	}
	if !binding.Enabled {
		return binding, ChannelMessage{}, channelError{code: "channel_disabled"}
	}
	return c.receiveForBinding(ctx, binding, callback)
}

func (c *ChannelCoordinator) receiveForBinding(ctx context.Context, binding ChannelBinding, callback ChannelCallback) (ChannelBinding, ChannelMessage, error) {
	adapter := c.adapterFor(binding.Channel)
	if adapter == nil {
		return ChannelBinding{}, ChannelMessage{}, errors.New("platform: channel adapter unavailable")
	}
	callback.Channel = binding.Channel
	callback.Credential = ChannelCredential{TenantID: binding.TenantID, Channel: binding.Channel, Secret: binding.Secret, Token: binding.Secret}
	callback.Scope = binding.SessionID
	message, err := adapter.Receive(ctx, callback)
	if err != nil {
		return binding, message, err
	}
	if message.ProviderSequence == 0 {
		return binding, ChannelMessage{}, channelError{code: "channel_callback_invalid"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	messageKey := binding.ID + "\x00" + message.MessageID
	_, duplicate := c.acceptedMessages[messageKey]
	if !duplicate && message.ProviderSequence <= c.lastSequences[binding.ID] {
		return binding, ChannelMessage{}, channelError{code: "channel_message_out_of_order"}
	}
	if message.ProviderSequence > c.lastSequences[binding.ID] {
		c.lastSequences[binding.ID] = message.ProviderSequence
	}
	if !duplicate {
		c.acceptedMessages[messageKey] = struct{}{}
	}
	message.AppID = binding.AppID
	message.SessionID = binding.SessionID
	if message.UserID == "" {
		message.UserID = binding.UserID
	}
	if message.ConversationID == "" {
		message.ConversationID = binding.ConversationID
	}
	message.ConversationType = binding.ConversationType
	return binding, message, nil
}

func (c *ChannelCoordinator) Send(ctx context.Context, binding ChannelBinding, reply ChannelReply) (ChannelDelivery, error) {
	adapter := c.adapterFor(binding.Channel)
	if adapter == nil {
		return ChannelDelivery{}, errors.New("platform: channel adapter unavailable")
	}
	if !binding.PlatformOwned {
		if _, ok, err := c.Binding(ctx, binding.TenantID, binding.ID); err != nil {
			return ChannelDelivery{}, err
		} else if !ok {
			return ChannelDelivery{}, ErrNotFound
		}
	}
	return adapter.Send(ctx, binding, reply)
}

func (c *ChannelCoordinator) adapterFor(channel string) ChannelAdapter {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if adapter, ok := c.adapters[channel]; ok {
		return adapter
	}
	if channel == ChannelMock {
		return c.adapter
	}
	return nil
}

type mockCallbackRequest struct {
	BindingID      string `json:"binding_id"`
	MessageID      string `json:"message_id"`
	Text           string `json:"text"`
	Sequence       uint64 `json:"sequence"`
	AttachmentName string `json:"attachment_name"`
	AttachmentSize int    `json:"attachment_size"`
}

type MockChannel struct {
	signature ChannelSignatureVerifier
	mu        sync.Mutex
	faults    map[string]MockFaultConfiguration
	counters  map[string]int
}

func NewMockChannel() *MockChannel {
	return &MockChannel{signature: HMACChannelSignature{}, faults: make(map[string]MockFaultConfiguration), counters: make(map[string]int)}
}

func (m *MockChannel) ConfigureFaults(tenantID, sessionID, scenario string) (MockFaultConfiguration, error) {
	config := defaultMockFaultConfiguration()
	switch scenario {
	case "none", "timeout", "retry", "rate_limit", "message_length", "attachment":
		config.Scenario = scenario
	default:
		return MockFaultConfiguration{}, errors.New("platform: invalid mock fault scenario")
	}
	switch scenario {
	case "message_length":
		config.MessageLengthLimit = 4
	case "rate_limit":
		config.RateLimit = 1
	case "attachment":
		config.AttachmentSizeLimit = 8
	case "timeout":
		config.TimeoutMilliseconds = 250
	case "retry":
		config.RetryLimit = 2
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults[mockFaultKey(tenantID, sessionID)] = config
	for key := range m.counters {
		if strings.HasPrefix(key, tenantID+"\x00") {
			delete(m.counters, key)
		}
	}
	return config, nil
}

func (m *MockChannel) Faults(tenantID, sessionID string) MockFaultConfiguration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if config, ok := m.faults[mockFaultKey(tenantID, sessionID)]; ok {
		return config
	}
	if sessionID != "" {
		if config, ok := m.faults[tenantID]; ok {
			return config
		}
	}
	return defaultMockFaultConfiguration()
}

func mockFaultKey(tenantID, sessionID string) string {
	if sessionID == "" {
		return tenantID
	}
	return tenantID + "\x00" + sessionID
}

func defaultMockFaultConfiguration() MockFaultConfiguration {
	return MockFaultConfiguration{
		Scenario: "none", MessageLengthLimit: 4096, AttachmentSizeLimit: 10 << 20,
		RateLimit: 100, TimeoutMilliseconds: 20, RetryLimit: 2,
	}
}

func (m *MockChannel) Receive(ctx context.Context, callback ChannelCallback) (ChannelMessage, error) {
	if callback.Channel != ChannelMock {
		return ChannelMessage{}, errors.New("platform: channel mismatch")
	}
	var request mockCallbackRequest
	decoder := json.NewDecoder(strings.NewReader(string(callback.Body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.BindingID == "" || callback.BindingID == "" || request.BindingID != callback.BindingID || request.MessageID == "" || request.Text == "" || !validIdempotencyKey("channel-"+request.MessageID) {
		return ChannelMessage{}, channelError{code: "channel_callback_invalid"}
	}
	if err := m.signature.Verify(ctx, callback.Credential, callback.Body, callback.Signature); err != nil {
		return ChannelMessage{}, channelError{code: "channel_signature_invalid"}
	}
	config := m.Faults(callback.Credential.TenantID, callback.Scope)
	if len([]rune(request.Text)) > config.MessageLengthLimit {
		return ChannelMessage{}, channelError{code: "channel_message_too_long"}
	}
	if request.AttachmentName != "" && (request.AttachmentSize <= 0 || request.AttachmentSize > config.AttachmentSizeLimit) {
		return ChannelMessage{}, channelError{code: "channel_attachment_rejected"}
	}
	if config.Scenario == "rate_limit" && m.increment("inbound", callback.Credential.TenantID, callback.BindingID, config.RateLimit) {
		return ChannelMessage{}, channelError{code: "channel_rate_limited"}
	}
	if config.Scenario == "timeout" {
		if err := waitMockTimeout(ctx, config.TimeoutMilliseconds); err != nil {
			return ChannelMessage{}, err
		}
	}
	return ChannelMessage{
		MessageID: request.MessageID, Text: request.Text, ProviderSequence: request.Sequence,
		AttachmentName: request.AttachmentName, AttachmentSize: request.AttachmentSize, ReceivedAt: time.Now().UTC(),
	}, nil
}

func (m *MockChannel) Send(ctx context.Context, binding ChannelBinding, reply ChannelReply) (ChannelDelivery, error) {
	if reply.MessageID == "" || reply.Text == "" {
		return ChannelDelivery{}, errors.New("platform: invalid channel reply")
	}
	return m.send(ctx, binding, reply)
}

func (m *MockChannel) send(ctx context.Context, binding ChannelBinding, reply ChannelReply) (ChannelDelivery, error) {
	config := m.Faults(binding.TenantID, binding.SessionID)
	if len([]rune(reply.Text)) > config.MessageLengthLimit {
		return ChannelDelivery{}, channelError{code: "channel_message_too_long"}
	}
	if config.Scenario == "rate_limit" && m.increment("outbound", binding.TenantID, binding.ID, config.RateLimit) {
		return ChannelDelivery{}, channelError{code: "channel_rate_limited"}
	}
	attempts := 1
	if config.Scenario == "retry" {
		for attempt := 0; attempt <= config.RetryLimit; attempt++ {
			attempts = attempt + 1
			if attempt > 0 {
				timer := time.NewTimer(time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ChannelDelivery{MessageID: reply.MessageID, Status: "failed", Attempts: attempts, LastAttempt: time.Now().UTC()}, channelError{code: "channel_timeout"}
				case <-timer.C:
				}
			}
		}
		return ChannelDelivery{MessageID: reply.MessageID, Status: "failed", Attempts: attempts, LastAttempt: time.Now().UTC()}, channelError{code: "channel_retry_exhausted"}
	}
	if config.Scenario == "timeout" {
		if err := waitMockTimeout(ctx, config.TimeoutMilliseconds); err != nil {
			return ChannelDelivery{MessageID: reply.MessageID, Status: "failed", Attempts: 1, LastAttempt: time.Now().UTC()}, err
		}
	}
	return ChannelDelivery{MessageID: reply.MessageID, Status: "delivered", Attempts: attempts, LastAttempt: time.Now().UTC()}, nil
}

func (m *MockChannel) increment(operation, tenantID, bindingID string, limit int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tenantID + "\x00" + bindingID + "\x00" + operation
	m.counters[key]++
	return m.counters[key] > limit
}

func waitMockTimeout(ctx context.Context, milliseconds int) error {
	timer := time.NewTimer(time.Duration(milliseconds) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return channelError{code: "channel_timeout"}
	case <-timer.C:
		return channelError{code: "channel_timeout"}
	}
}
