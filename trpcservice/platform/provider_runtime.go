package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

const (
	ProviderUnconfigured = "unconfigured"
	ProviderConnecting   = "connecting"
	ProviderConnected    = "connected"
	ProviderDisconnected = "disconnected"
	ProviderStopping     = "stopping"
)

var ErrProviderMessageIgnored = errors.New("platform: provider message ignored")

type BotConfig struct {
	TelegramUsername string
	TelegramToken    string
	WeComBotID       string
	WeComSecret      string
}

func LoadBotConfig(getenv func(string) string) BotConfig {
	if getenv == nil {
		getenv = os.Getenv
	}
	return BotConfig{
		TelegramUsername: getenv("TRPC_TELEGRAM_BOT_USERNAME"),
		TelegramToken:    getenv("TRPC_TELEGRAM_BOT_TOKEN"),
		WeComBotID:       getenv("TRPC_WECOM_BOT_ID"),
		WeComSecret:      getenv("TRPC_WECOM_BOT_SECRET"),
	}
}

type BotRoute struct {
	Provider         string `json:"provider"`
	ProviderAccount  string `json:"provider_account,omitempty"`
	ExternalSubject  string `json:"external_subject"`
	TenantID         string `json:"tenant_id"`
	AppID            string `json:"app_id"`
	ConversationType string `json:"conversation_type"`
	Enabled          bool   `json:"enabled"`
}

type BotTenantAllowlist struct {
	mu          sync.RWMutex
	routes      map[string]BotRoute
	persistPath string
	load        func(context.Context) (map[string]BotRoute, error)
	mutate      func(context.Context, func(map[string]BotRoute) error) (map[string]BotRoute, error)
}

// configurePersistence attaches the authoritative Control Plane adapter. A
// legacy in-memory route set is imported only when the shared store is empty;
// after that, callbacks always refresh from the shared store.
func (a *BotTenantAllowlist) configurePersistence(load func(context.Context) (map[string]BotRoute, error), mutate func(context.Context, func(map[string]BotRoute) error) (map[string]BotRoute, error)) error {
	a.mu.Lock()
	local := copyBotRoutes(a.routes)
	a.load, a.mutate = load, mutate
	a.mu.Unlock()
	if load == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	routes, err := load(ctx)
	if err != nil {
		return err
	}
	if len(routes) == 0 && len(local) > 0 && mutate != nil {
		routes, err = mutate(ctx, func(shared map[string]BotRoute) error {
			for key, route := range local {
				if previous, exists := shared[key]; exists && (previous.TenantID != route.TenantID || previous.AppID != route.AppID) {
					return errors.New("platform: ambiguous bot route")
				}
				shared[key] = route
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.routes = copyBotRoutes(routes)
	a.mu.Unlock()
	return nil
}

func copyBotRoutes(source map[string]BotRoute) map[string]BotRoute {
	result := make(map[string]BotRoute, len(source))
	for key, route := range source {
		result[key] = route
	}
	return result
}

func NewBotTenantAllowlist() *BotTenantAllowlist {
	return &BotTenantAllowlist{routes: make(map[string]BotRoute)}
}

func NewPersistentBotTenantAllowlist(path string) (*BotTenantAllowlist, error) {
	allowlist := &BotTenantAllowlist{routes: make(map[string]BotRoute), persistPath: path}
	if path == "" {
		return allowlist, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return allowlist, nil
	}
	if err != nil {
		return nil, err
	}
	var routes []BotRoute
	if err := json.Unmarshal(data, &routes); err != nil {
		return nil, errors.New("platform: invalid bot route store")
	}
	for _, route := range routes {
		if err := validateBotRoute(route); err != nil {
			return nil, errors.New("platform: invalid bot route store")
		}
		key := botRouteKey(route.Provider, route.ProviderAccount, route.ExternalSubject)
		if _, exists := allowlist.routes[key]; exists {
			return nil, errors.New("platform: duplicate bot route store entry")
		}
		allowlist.routes[key] = route
	}
	return allowlist, nil
}

func botRouteKey(provider, account, subject string) string {
	return provider + "\x00" + account + "\x00" + subject
}

func providerSessionID(provider, account, subject string) string {
	sum := sha256.Sum256([]byte(provider + "\x00" + account + "\x00" + subject))
	return "im-" + hex.EncodeToString(sum[:12])
}

func (a *BotTenantAllowlist) Upsert(route BotRoute) error {
	if route.ConversationType == "" {
		route.ConversationType = ConversationSingle
	}
	if err := validateBotRoute(route); err != nil {
		return err
	}
	route.Enabled = true
	key := botRouteKey(route.Provider, route.ProviderAccount, route.ExternalSubject)
	return a.mutateRoutes(context.Background(), func(routes map[string]BotRoute) error {
		storedKey, previous, ok := findBotRouteStorageKey(routes, route.Provider, route.ProviderAccount, route.ExternalSubject)
		if ok && (previous.TenantID != route.TenantID || previous.AppID != route.AppID) {
			return errors.New("platform: ambiguous bot route")
		}
		if ok && storedKey != key {
			delete(routes, storedKey)
		}
		routes[key] = route
		return nil
	})
}

func (a *BotTenantAllowlist) Resolve(provider, subject string) (BotRoute, bool) {
	route, ok, _ := a.ResolveContext(context.Background(), provider, subject)
	return route, ok && route.Enabled
}

func (a *BotTenantAllowlist) ResolveAccount(provider, account, subject string) (BotRoute, bool) {
	route, ok, _ := a.ResolveAccountContext(context.Background(), provider, account, subject)
	return route, ok && route.Enabled
}

func (a *BotTenantAllowlist) Lookup(provider, subject string) (BotRoute, bool) {
	route, ok, _ := a.ResolveContext(context.Background(), provider, subject)
	return route, ok
}

func (a *BotTenantAllowlist) ResolveContext(ctx context.Context, provider, subject string) (BotRoute, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.refreshLocked(ctx); err != nil {
		return BotRoute{}, false, err
	}
	return resolveBotRoute(a.routes, provider, "", subject)
}

func (a *BotTenantAllowlist) ResolveAccountContext(ctx context.Context, provider, account, subject string) (BotRoute, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.refreshLocked(ctx); err != nil {
		return BotRoute{}, false, err
	}
	return resolveBotRoute(a.routes, provider, account, subject)
}

func resolveBotRoute(routes map[string]BotRoute, provider, account, subject string) (BotRoute, bool, error) {
	if route, ok := routes[botRouteKey(provider, account, subject)]; ok {
		return route, true, nil
	}
	// An account-neutral route is an explicit fallback for installations that
	// operate a single provider account. Account-specific routes always win.
	if account != "" {
		if route, ok := routes[botRouteKey(provider, "", subject)]; ok {
			return route, true, nil
		}
	}
	var match BotRoute
	matches := 0
	for _, route := range routes {
		if route.Provider == provider && route.ExternalSubject == subject {
			match = route
			matches++
		}
	}
	if matches == 1 {
		return match, true, nil
	}
	return BotRoute{}, false, nil
}

func findBotRouteStorageKey(routes map[string]BotRoute, provider, account, subject string) (string, BotRoute, bool) {
	key := botRouteKey(provider, account, subject)
	if route, ok := routes[key]; ok {
		return key, route, true
	}
	for storedKey, route := range routes {
		if route.Provider == provider && route.ProviderAccount == account && route.ExternalSubject == subject {
			return storedKey, route, true
		}
	}
	return "", BotRoute{}, false
}

func (a *BotTenantAllowlist) List() []BotRoute {
	items, _ := a.ListContext(context.Background())
	return items
}

func (a *BotTenantAllowlist) ListContext(ctx context.Context) ([]BotRoute, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.refreshLocked(ctx); err != nil {
		return nil, err
	}
	items := make([]BotRoute, 0, len(a.routes))
	for _, route := range a.routes {
		items = append(items, route)
	}
	sort.Slice(items, func(i, j int) bool {
		return botRouteKey(items[i].Provider, items[i].ProviderAccount, items[i].ExternalSubject) < botRouteKey(items[j].Provider, items[j].ProviderAccount, items[j].ExternalSubject)
	})
	return items, nil
}

func (a *BotTenantAllowlist) Update(provider, subject string, replacement BotRoute) (BotRoute, error) {
	existing, ok := a.Lookup(provider, subject)
	if !ok {
		return BotRoute{}, ErrNotFound
	}
	return a.UpdateAccount(provider, existing.ProviderAccount, subject, replacement)
}

func (a *BotTenantAllowlist) UpdateAccount(provider, account, subject string, replacement BotRoute) (BotRoute, error) {
	key := botRouteKey(provider, account, subject)
	replacement.Provider = provider
	replacement.ProviderAccount = account
	replacement.ExternalSubject = subject
	if err := validateBotRoute(replacement); err != nil {
		return BotRoute{}, err
	}
	err := a.mutateRoutes(context.Background(), func(routes map[string]BotRoute) error {
		storedKey, _, ok := findBotRouteStorageKey(routes, provider, account, subject)
		if !ok {
			return ErrNotFound
		}
		if storedKey != key {
			delete(routes, storedKey)
		}
		routes[key] = replacement
		return nil
	})
	if err != nil {
		return BotRoute{}, err
	}
	return replacement, nil
}

func (a *BotTenantAllowlist) Delete(provider, subject string) error {
	existing, ok := a.Lookup(provider, subject)
	if !ok {
		return nil
	}
	return a.DeleteAccount(provider, existing.ProviderAccount, subject)
}

func (a *BotTenantAllowlist) DeleteAccount(provider, account, subject string) error {
	key := botRouteKey(provider, account, subject)
	return a.mutateRoutes(context.Background(), func(routes map[string]BotRoute) error {
		if storedKey, _, ok := findBotRouteStorageKey(routes, provider, account, subject); ok {
			delete(routes, storedKey)
		} else {
			delete(routes, key)
		}
		return nil
	})
}

func (a *BotTenantAllowlist) refreshLocked(ctx context.Context) error {
	if a.load == nil {
		return nil
	}
	routes, err := a.load(ctx)
	if err != nil {
		return err
	}
	a.routes = copyBotRoutes(routes)
	return nil
}

func (a *BotTenantAllowlist) mutateRoutes(ctx context.Context, mutate func(map[string]BotRoute) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.mutate != nil {
		routes, err := a.mutate(ctx, mutate)
		if err != nil {
			return err
		}
		a.routes = copyBotRoutes(routes)
		return nil
	}
	previous := copyBotRoutes(a.routes)
	if err := mutate(a.routes); err != nil {
		return err
	}
	if err := a.persistLocked(); err != nil {
		a.routes = previous
		return err
	}
	return nil
}

func validateBotRoute(route BotRoute) error {
	if (route.Provider != ChannelTelegram && route.Provider != ChannelEnterpriseWeChat) || route.ExternalSubject == "" || route.TenantID == "" || route.AppID == "" {
		return errors.New("platform: invalid bot route")
	}
	if route.ConversationType != ConversationSingle && route.ConversationType != ConversationGroup {
		return errors.New("platform: invalid bot route")
	}
	return nil
}

func (a *BotTenantAllowlist) replaceLocked(key string, route BotRoute) error {
	previous, existed := a.routes[key]
	a.routes[key] = route
	if err := a.persistLocked(); err != nil {
		if existed {
			a.routes[key] = previous
		} else {
			delete(a.routes, key)
		}
		return err
	}
	return nil
}

func (a *BotTenantAllowlist) persistLocked() error {
	if a.persistPath == "" {
		return nil
	}
	items := make([]BotRoute, 0, len(a.routes))
	for _, route := range a.routes {
		items = append(items, route)
	}
	sort.Slice(items, func(i, j int) bool {
		return botRouteKey(items[i].Provider, items[i].ProviderAccount, items[i].ExternalSubject) < botRouteKey(items[j].Provider, items[j].ProviderAccount, items[j].ExternalSubject)
	})
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	directory := filepath.Dir(a.persistPath)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".bot-routes-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, a.persistPath)
}

type BotStatus struct {
	Provider              string `json:"provider"`
	Status                string `json:"status"`
	LastErr               string `json:"last_error,omitempty"`
	CredentialSmokeStatus string `json:"credential_smoke_status"`
}

type ProviderDelivery struct {
	Provider        string    `json:"provider"`
	ExternalSubject string    `json:"external_subject"`
	TenantID        string    `json:"tenant_id"`
	AppID           string    `json:"app_id"`
	RequestID       string    `json:"request_id"`
	Status          string    `json:"status"`
	Code            string    `json:"code,omitempty"`
	Attempts        int       `json:"attempts"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type ProviderRuntime struct {
	config                 BotConfig
	routes                 *BotTenantAllowlist
	client                 *http.Client
	telegramBaseURL        string
	telegramRequestTimeout time.Duration
	wecomEndpoint          string
	wecomOrigin            string
	wecomRequestTimeout    time.Duration
	mu                     sync.RWMutex
	statuses               map[string]BotStatus
	deliveries             map[string]ProviderDelivery
	inboundMessages        map[string]int64
	lastSequences          map[string]int64
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	wecomConn              *websocket.Conn
	wecomSessionID         string
	wecomSessionDone       chan struct{}
	wecomPending           map[string]chan wecomWireFrame
	wecomWriteMu           sync.Mutex
	started                bool
	processor              func(context.Context, string, string, []byte) error
}

func NewProviderRuntime(config BotConfig, routes *BotTenantAllowlist, processor func(context.Context, string, string, []byte) error) *ProviderRuntime {
	if routes == nil {
		routes = NewBotTenantAllowlist()
	}
	return &ProviderRuntime{
		config: config, routes: routes, client: http.DefaultClient,
		telegramBaseURL: "https://api.telegram.org", telegramRequestTimeout: 15 * time.Second, wecomEndpoint: "wss://openws.work.weixin.qq.com", wecomOrigin: "https://open.work.weixin.qq.com", wecomRequestTimeout: 15 * time.Second,
		statuses: make(map[string]BotStatus), deliveries: make(map[string]ProviderDelivery), inboundMessages: make(map[string]int64), lastSequences: make(map[string]int64), processor: processor,
	}
}

func (p *ProviderRuntime) Statuses() []BotStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	items := make([]BotStatus, 0, len(p.statuses))
	for _, status := range p.statuses {
		items = append(items, status)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Provider < items[j].Provider })
	return items
}

func (p *ProviderRuntime) Routes() *BotTenantAllowlist { return p.routes }

func (p *ProviderRuntime) Deliveries(tenantID string) []ProviderDelivery {
	p.mu.RLock()
	defer p.mu.RUnlock()
	items := make([]ProviderDelivery, 0, len(p.deliveries))
	for _, delivery := range p.deliveries {
		if tenantID == "" || delivery.TenantID == tenantID {
			items = append(items, delivery)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	return items
}

func (p *ProviderRuntime) recordDelivery(binding ChannelBinding, reply ChannelReply, status, code string, attempts int) {
	delivery := ProviderDelivery{
		Provider: binding.Channel, ExternalSubject: binding.ConversationID, TenantID: binding.TenantID, AppID: binding.AppID,
		RequestID: reply.MessageID, Status: status, Code: code, Attempts: attempts, UpdatedAt: time.Now().UTC(),
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := botRouteKey(binding.Channel, binding.ProviderAccount, binding.ConversationID)
	if existing, ok := p.deliveries[key]; ok && existing.RequestID == delivery.RequestID && status == "accepted" && existing.Status != "accepted" {
		return
	}
	p.deliveries[key] = delivery
	if len(p.deliveries) <= 256 {
		return
	}
	var oldestKey string
	var oldest time.Time
	for candidateKey, candidate := range p.deliveries {
		if oldestKey == "" || candidate.UpdatedAt.Before(oldest) {
			oldestKey, oldest = candidateKey, candidate.UpdatedAt
		}
	}
	delete(p.deliveries, oldestKey)
}

func (p *ProviderRuntime) recordAccepted(route BotRoute, requestID string) {
	p.recordDelivery(ChannelBinding{Channel: route.Provider, ProviderAccount: route.ProviderAccount, ConversationID: route.ExternalSubject, TenantID: route.TenantID, AppID: route.AppID}, ChannelReply{MessageID: requestID}, "accepted", "", 0)
}

func (p *ProviderRuntime) recordRejected(provider, subject, requestID, code string, route BotRoute) {
	p.recordDelivery(ChannelBinding{Channel: provider, ProviderAccount: route.ProviderAccount, ConversationID: subject, TenantID: route.TenantID, AppID: route.AppID}, ChannelReply{MessageID: requestID}, "rejected", code, 0)
}

func (p *ProviderRuntime) recordTerminalIfPending(binding ChannelBinding, reply ChannelReply, code string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := botRouteKey(binding.Channel, binding.ProviderAccount, binding.ConversationID)
	existing, ok := p.deliveries[key]
	if !ok || existing.RequestID != reply.MessageID || (existing.Status != "accepted" && existing.Status != "retried") {
		return
	}
	existing.Status = "terminal_failed"
	existing.Code = code
	existing.UpdatedAt = time.Now().UTC()
	p.deliveries[key] = existing
}

func (p *ProviderRuntime) acceptInbound(route BotRoute, messageID string, sequence int64) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	routeKey := botRouteKey(route.Provider, route.ProviderAccount, route.ExternalSubject)
	messageKey := routeKey + "\x00" + messageID
	if _, exists := p.inboundMessages[messageKey]; exists {
		return "duplicate"
	}
	if sequence > 0 && sequence <= p.lastSequences[routeKey] {
		return "out_of_order"
	}
	p.inboundMessages[messageKey] = sequence
	if sequence > 0 {
		p.lastSequences[routeKey] = sequence
	}
	if len(p.inboundMessages) > 4096 {
		removed := 0
		for key := range p.inboundMessages {
			delete(p.inboundMessages, key)
			removed++
			if removed == 1024 {
				break
			}
		}
	}
	return ""
}

func (p *ProviderRuntime) releaseInbound(route BotRoute, messageID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	routeKey := botRouteKey(route.Provider, route.ProviderAccount, route.ExternalSubject)
	delete(p.inboundMessages, routeKey+"\x00"+messageID)
	var latest int64
	for key, sequence := range p.inboundMessages {
		if strings.HasPrefix(key, routeKey+"\x00") && sequence > latest {
			latest = sequence
		}
	}
	if latest == 0 {
		delete(p.lastSequences, routeKey)
	} else {
		p.lastSequences[routeKey] = latest
	}
}

func (p *ProviderRuntime) setStatus(provider, status string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value := BotStatus{Provider: provider, Status: status, CredentialSmokeStatus: "not_run"}
	if err != nil {
		value.LastErr = "provider connection unavailable"
	}
	p.statuses[provider] = value
}

func (p *ProviderRuntime) Start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		cancel()
		return
	}
	p.started = true
	p.cancel = cancel
	telegramEnabled := p.config.TelegramToken != "" && p.processor != nil
	wecomEnabled := p.config.WeComBotID != "" && p.config.WeComSecret != "" && p.processor != nil
	if !telegramEnabled {
		p.statuses[ChannelTelegram] = BotStatus{Provider: ChannelTelegram, Status: ProviderUnconfigured, CredentialSmokeStatus: "not_run"}
	}
	if !wecomEnabled {
		p.statuses[ChannelEnterpriseWeChat] = BotStatus{Provider: ChannelEnterpriseWeChat, Status: ProviderUnconfigured, CredentialSmokeStatus: "not_run"}
	}
	if telegramEnabled {
		p.wg.Add(1)
	}
	if wecomEnabled {
		p.wg.Add(1)
	}
	p.mu.Unlock()
	if telegramEnabled {
		go func() { defer p.wg.Done(); p.telegramLoop(ctx) }()
	}
	if wecomEnabled {
		go func() { defer p.wg.Done(); p.wecomLoop(ctx) }()
	}
}

func (p *ProviderRuntime) Close() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	for provider := range p.statuses {
		p.statuses[provider] = BotStatus{Provider: provider, Status: ProviderStopping, CredentialSmokeStatus: "not_run"}
	}
	cancel := p.cancel
	p.cancel = nil
	if p.wecomConn != nil {
		_ = p.wecomConn.Close()
		p.wecomConn = nil
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	p.wg.Wait()
}

func (p *ProviderRuntime) telegramLoop(ctx context.Context) {
	offset := int64(0)
	for attempt := 0; ctx.Err() == nil; attempt++ {
		p.setStatus(ChannelTelegram, ProviderConnecting, nil)
		if err := p.pollTelegram(ctx, &offset); err != nil && ctx.Err() == nil {
			p.setStatus(ChannelTelegram, ProviderDisconnected, errors.New("telegram polling failed"))
			if !waitProviderBackoff(ctx, attempt) {
				return
			}
		}
	}
}

func (p *ProviderRuntime) pollTelegram(ctx context.Context, offset *int64) error {
	for ctx.Err() == nil {
		endpoint := strings.TrimRight(p.telegramBaseURL, "/") + "/bot" + url.PathEscape(p.config.TelegramToken) + "/getUpdates?timeout=20&allowed_updates=%5B%22message%22%5D"
		if *offset > 0 {
			endpoint += "&offset=" + strconv.FormatInt(*offset, 10)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		response, err := p.client.Do(request)
		if err != nil {
			return err
		}
		var envelope struct {
			OK     bool              `json:"ok"`
			Result []json.RawMessage `json:"result"`
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&envelope)
		response.Body.Close()
		if err != nil || !envelope.OK {
			return errors.New("telegram polling failed")
		}
		p.setStatus(ChannelTelegram, ProviderConnected, nil)
		for _, raw := range envelope.Result {
			var update struct {
				UpdateID int64 `json:"update_id"`
			}
			if err := json.Unmarshal(raw, &update); err != nil {
				continue
			}
			if err := p.processor(ctx, ChannelTelegram, p.config.TelegramUsername, raw); err != nil && !errors.Is(err, ErrProviderMessageIgnored) {
				return errors.New("telegram update processing failed")
			}
			if update.UpdateID >= *offset {
				*offset = update.UpdateID + 1
			}
		}
	}
	return ctx.Err()
}

func (p *ProviderRuntime) sendTelegram(ctx context.Context, binding ChannelBinding, reply ChannelReply) error {
	payload, err := json.Marshal(map[string]string{"chat_id": binding.ConversationID, "text": reply.Text})
	if err != nil {
		p.recordDelivery(binding, reply, "terminal_failed", "provider_unavailable", 1)
		return err
	}
	endpoint := strings.TrimRight(p.telegramBaseURL, "/") + "/bot" + url.PathEscape(p.config.TelegramToken) + "/sendMessage"
	for attempt := 1; attempt <= 3; attempt++ {
		retryAfter, retryable, code, sendErr := p.sendTelegramAttempt(ctx, endpoint, payload)
		if sendErr == nil {
			p.recordDelivery(binding, reply, "delivered", "", attempt)
			return nil
		}
		if !retryable || attempt == 3 {
			if retryable && attempt == 3 {
				code = "retry_exhausted"
			}
			p.recordDelivery(binding, reply, "terminal_failed", code, attempt)
			return channelError{code: code}
		}
		p.recordDelivery(binding, reply, "retried", code, attempt)
		if retryAfter <= 0 {
			retryAfter = time.Duration(1<<(attempt-1)) * 100 * time.Millisecond
		}
		if retryAfter > time.Second {
			retryAfter = time.Second
		}
		timer := time.NewTimer(retryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			p.recordDelivery(binding, reply, "terminal_failed", "cancelled", attempt)
			return channelError{code: "cancelled"}
		case <-timer.C:
		}
	}
	return errors.New("telegram send failed")
}

func (p *ProviderRuntime) sendTelegramAttempt(ctx context.Context, endpoint string, payload []byte) (time.Duration, bool, string, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, p.telegramRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return 0, false, "provider_unavailable", err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := p.client.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return 0, false, "cancelled", err
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
			return 0, true, "timeout", err
		}
		return 0, true, "provider_unavailable", err
	}
	defer response.Body.Close()
	var envelope struct {
		OK         bool `json:"ok"`
		Parameters struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	decodeErr := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&envelope)
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices && decodeErr == nil && envelope.OK {
		return 0, false, "", nil
	}
	retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError
	code := "provider_unavailable"
	if response.StatusCode == http.StatusTooManyRequests {
		code = "rate_limited"
	}
	return time.Duration(envelope.Parameters.RetryAfter) * time.Second, retryable, code, errors.New("telegram send failed")
}

func (p *ProviderRuntime) wecomLoop(ctx context.Context) {
	for attempt := 0; ctx.Err() == nil; attempt++ {
		p.setStatus(ChannelEnterpriseWeChat, ProviderConnecting, nil)
		if err := p.connectWeCom(ctx); err != nil && ctx.Err() == nil {
			p.setStatus(ChannelEnterpriseWeChat, ProviderDisconnected, errors.New("wecom connection failed"))
			if !waitProviderBackoff(ctx, attempt) {
				return
			}
		}
	}
}

func (p *ProviderRuntime) connectWeCom(ctx context.Context) error {
	config, err := websocket.NewConfig(p.wecomEndpoint, p.wecomOrigin)
	if err != nil {
		return err
	}
	connection, err := config.DialContext(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	cancelWatchDone := make(chan struct{})
	defer close(cancelWatchDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-cancelWatchDone:
		}
	}()
	p.mu.Lock()
	p.wecomConn = connection
	p.wecomSessionID = newRequestID()
	p.wecomSessionDone = make(chan struct{})
	p.wecomPending = make(map[string]chan wecomWireFrame)
	done := p.wecomSessionDone
	sessionID := p.wecomSessionID
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		if p.wecomConn == connection {
			p.wecomConn = nil
			p.wecomSessionID = ""
			close(done)
			p.wecomSessionDone = nil
			p.wecomPending = nil
		}
		p.mu.Unlock()
	}()
	subscribeID := newRequestID()
	if err := p.writeWeCom(connection, wecomWireFrame{Command: "aibot_subscribe", Headers: wecomHeaders{RequestID: subscribeID}, Body: mustJSON(map[string]string{"bot_id": p.config.WeComBotID, "secret": p.config.WeComSecret})}); err != nil {
		return err
	}
	_ = connection.SetReadDeadline(time.Now().Add(p.wecomRequestTimeout))
	var subscribeResponse wecomWireFrame
	if err := websocket.JSON.Receive(connection, &subscribeResponse); err != nil {
		return err
	}
	_ = connection.SetReadDeadline(time.Time{})
	if subscribeResponse.Headers.RequestID != subscribeID || subscribeResponse.ErrorCode == nil || *subscribeResponse.ErrorCode != 0 {
		return errors.New("wecom authentication failed")
	}
	p.setStatus(ChannelEnterpriseWeChat, ProviderConnected, nil)
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)
	go p.wecomHeartbeat(ctx, connection, sessionID, done, heartbeatDone)
	for ctx.Err() == nil {
		var frame wecomWireFrame
		if err := websocket.JSON.Receive(connection, &frame); err != nil {
			return err
		}
		if frame.Command == "" && frame.ErrorCode != nil {
			p.deliverWeComResponse(frame)
			continue
		}
		if frame.Command == "aibot_event_callback" {
			var event struct {
				Event struct {
					Type string `json:"eventtype"`
				} `json:"event"`
			}
			if json.Unmarshal(frame.Body, &event) == nil && event.Event.Type == "disconnected_event" {
				return errors.New("wecom connection replaced")
			}
			continue
		}
		if frame.Command != "aibot_msg_callback" {
			return errors.New("unsupported wecom frame")
		}
		frame.ProviderSession = sessionID
		raw, _ := json.Marshal(frame)
		if err := p.processor(ctx, ChannelEnterpriseWeChat, p.config.WeComBotID, raw); err != nil && !errors.Is(err, ErrProviderMessageIgnored) {
			return errors.New("wecom frame processing failed")
		}
	}
	return ctx.Err()
}

type wecomHeaders struct {
	RequestID string `json:"req_id"`
}

type wecomWireFrame struct {
	Command         string          `json:"cmd,omitempty"`
	Headers         wecomHeaders    `json:"headers"`
	Body            json.RawMessage `json:"body,omitempty"`
	ErrorCode       *int            `json:"errcode,omitempty"`
	ErrorMessage    string          `json:"errmsg,omitempty"`
	ProviderSession string          `json:"provider_session_id,omitempty"`
}

func mustJSON(value any) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

func (p *ProviderRuntime) writeWeCom(connection *websocket.Conn, frame wecomWireFrame) error {
	p.wecomWriteMu.Lock()
	defer p.wecomWriteMu.Unlock()
	return websocket.JSON.Send(connection, frame)
}

func (p *ProviderRuntime) requestWeCom(ctx context.Context, providerSession, requestID, command string, body any) error {
	p.mu.Lock()
	connection := p.wecomConn
	done := p.wecomSessionDone
	if connection == nil || done == nil || (providerSession != "" && providerSession != p.wecomSessionID) {
		p.mu.Unlock()
		return errors.New("wecom not connected")
	}
	if _, exists := p.wecomPending[requestID]; exists {
		p.mu.Unlock()
		return errors.New("wecom request already pending")
	}
	response := make(chan wecomWireFrame, 1)
	p.wecomPending[requestID] = response
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		if p.wecomPending != nil {
			delete(p.wecomPending, requestID)
		}
		p.mu.Unlock()
	}()
	if err := p.writeWeCom(connection, wecomWireFrame{Command: command, Headers: wecomHeaders{RequestID: requestID}, Body: mustJSON(body)}); err != nil {
		return err
	}
	timer := time.NewTimer(p.wecomRequestTimeout)
	defer timer.Stop()
	select {
	case frame := <-response:
		if frame.ErrorCode == nil || *frame.ErrorCode != 0 {
			return errors.New("wecom request rejected")
		}
		return nil
	case <-done:
		return errors.New("wecom disconnected")
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("wecom request timeout")
	}
}

func (p *ProviderRuntime) deliverWeComResponse(frame wecomWireFrame) {
	p.mu.RLock()
	response := p.wecomPending[frame.Headers.RequestID]
	p.mu.RUnlock()
	if response != nil {
		select {
		case response <- frame:
		default:
		}
	}
}

func (p *ProviderRuntime) wecomHeartbeat(ctx context.Context, connection *websocket.Conn, providerSession string, sessionDone, stop <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := p.requestWeCom(ctx, providerSession, newRequestID(), "ping", nil); err != nil {
				_ = connection.Close()
				return
			}
		case <-sessionDone:
			return
		case <-stop:
			return
		}
	}
}

func (p *ProviderRuntime) sendWeCom(ctx context.Context, binding ChannelBinding, reply ChannelReply) error {
	if binding.ReplyReference == "" {
		p.recordDelivery(binding, reply, "terminal_failed", "provider_unavailable", 1)
		return errors.New("wecom reply reference missing")
	}
	body := map[string]any{"msgtype": "markdown", "markdown": map[string]string{"content": reply.Text}}
	for attempt := 1; attempt <= 3; attempt++ {
		err := p.requestWeCom(ctx, binding.ProviderSession, binding.ReplyReference, "aibot_respond_msg", body)
		if err == nil {
			p.recordDelivery(binding, reply, "delivered", "", attempt)
			return nil
		}
		code := "provider_unavailable"
		if errors.Is(err, context.Canceled) {
			code = "cancelled"
		} else if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "timeout") {
			code = "timeout"
		}
		if code == "cancelled" {
			p.recordDelivery(binding, reply, "terminal_failed", code, attempt)
			return channelError{code: code}
		}
		if attempt == 3 {
			p.recordDelivery(binding, reply, "terminal_failed", "retry_exhausted", attempt)
			return channelError{code: "retry_exhausted"}
		}
		p.recordDelivery(binding, reply, "retried", code, attempt)
		if !waitProviderBackoff(ctx, attempt-1) {
			p.recordDelivery(binding, reply, "terminal_failed", "cancelled", attempt)
			return channelError{code: "cancelled"}
		}
	}
	return channelError{code: "retry_exhausted"}
}

func waitProviderBackoff(ctx context.Context, attempt int) bool {
	if attempt > 5 {
		attempt = 5
	}
	timer := time.NewTimer(time.Duration(1<<attempt) * 100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *ProviderRuntime) TelegramTokenConfigured() bool { return p.config.TelegramToken != "" }
func (p *ProviderRuntime) WeComConfigured() bool {
	return p.config.WeComBotID != "" && p.config.WeComSecret != ""
}
