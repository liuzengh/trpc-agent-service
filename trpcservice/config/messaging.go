package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Role string

const (
	RoleGateway Role = "gateway"
	RoleWorker  Role = "worker"
	RoleServe   Role = "serve"
)

const (
	DefaultMessagingPrefix            = "trpc-agent-service:phase3"
	DefaultLeaseDuration              = 30 * time.Second
	DefaultHeartbeatInterval          = 10 * time.Second
	DefaultInitialBackoff             = time.Second
	DefaultMaxBackoff                 = 30 * time.Second
	DefaultMaxAttempts                = 3
	DefaultInboxRetention             = 24 * time.Hour
	DefaultReplyWaitTimeout           = 75 * time.Second
	DefaultSessionFencing             = "legacy"
	DefaultSessionLockDuration        = DefaultLeaseDuration
	DefaultSessionWaitBackoff         = 100 * time.Millisecond
	DefaultSessionWaitMaxBackoff      = 2 * time.Second
	DefaultMaxTurnEvents              = 512
	DefaultMaxTurnBytes               = 2 << 20
	DefaultShutdownTimeout            = 10 * time.Second
	DefaultOutboundMaxAttempts        = 5
	DefaultOutboundInitialBackoff     = time.Second
	DefaultOutboundMaxBackoff         = 30 * time.Second
	DefaultOutboundSendTimeout        = 10 * time.Second
	DefaultOutboundClaimIdle          = 30 * time.Second
	DefaultPersistenceTimeout         = 10 * time.Second
	DefaultPersistenceMaxAttempts     = 5
	DefaultPersistenceInitialBackoff  = time.Second
	DefaultPersistenceMaxBackoff      = 30 * time.Second
	DefaultPersistencePayloadMaxBytes = 4 << 20
	MaxMessagingPrefixBytes           = 128
	minMessagingDuration              = time.Millisecond
	minInboxRetention                 = time.Second
	maxMessagingAttempts              = 10
)

// MessagingConfig owns the platform-level Redis connection and reliability
// policy. It is independent from tenant Session/Memory storage profiles.
type MessagingConfig struct {
	RedisCredentialRef         string
	RedisURL                   string
	KeyPrefix                  string
	LeaseDuration              time.Duration
	HeartbeatInterval          time.Duration
	InitialBackoff             time.Duration
	MaxBackoff                 time.Duration
	MaxAttempts                int
	InboxRetention             time.Duration
	ReplyWaitTimeout           time.Duration
	SessionFencing             string
	SessionLockDuration        time.Duration
	SessionWaitBackoff         time.Duration
	SessionWaitMaxBackoff      time.Duration
	MaxTurnEvents              int
	MaxTurnBytes               int
	ShutdownTimeout            time.Duration
	OutboundMaxAttempts        int
	OutboundInitialBackoff     time.Duration
	OutboundMaxBackoff         time.Duration
	OutboundSendTimeout        time.Duration
	OutboundClaimIdle          time.Duration
	PersistenceTimeout         time.Duration
	PersistenceMaxAttempts     int
	PersistenceInitialBackoff  time.Duration
	PersistenceMaxBackoff      time.Duration
	PersistencePayloadMaxBytes int
}

func (c MessagingConfig) String() string {
	return fmt.Sprintf(
		"MessagingConfig{RedisCredentialRef:[REDACTED] RedisURL:[REDACTED] KeyPrefix:%q LeaseDuration:%s HeartbeatInterval:%s InitialBackoff:%s MaxBackoff:%s MaxAttempts:%d InboxRetention:%s ReplyWaitTimeout:%s SessionFencing:%s SessionLockDuration:%s SessionWaitBackoff:%s SessionWaitMaxBackoff:%s MaxTurnEvents:%d MaxTurnBytes:%d ShutdownTimeout:%s PersistenceTimeout:%s PersistenceMaxAttempts:%d PersistenceInitialBackoff:%s PersistenceMaxBackoff:%s PersistencePayloadMaxBytes:%d}",
		c.KeyPrefix, c.LeaseDuration,
		c.HeartbeatInterval, c.InitialBackoff, c.MaxBackoff, c.MaxAttempts,
		c.InboxRetention, c.ReplyWaitTimeout, c.SessionFencing,
		c.SessionLockDuration, c.SessionWaitBackoff, c.SessionWaitMaxBackoff,
		c.MaxTurnEvents, c.MaxTurnBytes, c.ShutdownTimeout,
		c.PersistenceTimeout, c.PersistenceMaxAttempts, c.PersistenceInitialBackoff,
		c.PersistenceMaxBackoff, c.PersistencePayloadMaxBytes,
	)
}

func (c MessagingConfig) GoString() string { return c.String() }

type messagingConfigFile struct {
	RedisCredentialRef         string `json:"redis_credential_ref"`
	KeyPrefix                  string `json:"key_prefix"`
	LeaseDuration              string `json:"lease_duration,omitempty"`
	HeartbeatInterval          string `json:"heartbeat_interval,omitempty"`
	InitialBackoff             string `json:"initial_backoff,omitempty"`
	MaxBackoff                 string `json:"max_backoff,omitempty"`
	MaxAttempts                int    `json:"max_attempts,omitempty"`
	InboxRetention             string `json:"inbox_retention,omitempty"`
	ReplyWaitTimeout           string `json:"reply_wait_timeout,omitempty"`
	SessionFencing             string `json:"session_fencing,omitempty"`
	SessionLockDuration        string `json:"session_lock_duration,omitempty"`
	SessionWaitBackoff         string `json:"session_wait_backoff,omitempty"`
	SessionWaitMaxBackoff      string `json:"session_wait_max_backoff,omitempty"`
	MaxTurnEvents              int    `json:"max_turn_events,omitempty"`
	MaxTurnBytes               int    `json:"max_turn_bytes,omitempty"`
	ShutdownTimeout            string `json:"shutdown_timeout,omitempty"`
	OutboundMaxAttempts        int    `json:"outbound_max_attempts,omitempty"`
	OutboundInitialBackoff     string `json:"outbound_initial_backoff,omitempty"`
	OutboundMaxBackoff         string `json:"outbound_max_backoff,omitempty"`
	OutboundSendTimeout        string `json:"outbound_send_timeout,omitempty"`
	OutboundClaimIdle          string `json:"outbound_claim_idle,omitempty"`
	PersistenceTimeout         string `json:"persistence_timeout,omitempty"`
	PersistenceMaxAttempts     int    `json:"persistence_max_attempts,omitempty"`
	PersistenceInitialBackoff  string `json:"persistence_initial_backoff,omitempty"`
	PersistenceMaxBackoff      string `json:"persistence_max_backoff,omitempty"`
	PersistencePayloadMaxBytes int    `json:"persistence_payload_max_bytes,omitempty"`
}

func parseMessagingFile(raw *messagingConfigFile, resolver CredentialResolver) (*MessagingConfig, error) {
	if raw == nil {
		return nil, nil
	}
	if resolver == nil {
		return nil, errors.New("messaging credential resolver is required")
	}
	if _, err := envNameFromCredentialRef(raw.RedisCredentialRef); err != nil {
		return nil, errors.New("messaging redis credential_ref must use env:<ENV_NAME>")
	}
	redisRaw, err := resolver.Resolve(raw.RedisCredentialRef)
	if err != nil {
		return nil, errors.New("messaging redis credential is unavailable")
	}
	redisURL, err := NormalizeRedisURL(redisRaw)
	if err != nil {
		return nil, errors.New("messaging redis credential is not a valid Redis URL")
	}
	config := MessagingConfig{
		RedisCredentialRef:         raw.RedisCredentialRef,
		RedisURL:                   redisURL,
		KeyPrefix:                  valueOrDefault(raw.KeyPrefix, DefaultMessagingPrefix),
		LeaseDuration:              DefaultLeaseDuration,
		HeartbeatInterval:          DefaultHeartbeatInterval,
		InitialBackoff:             DefaultInitialBackoff,
		MaxBackoff:                 DefaultMaxBackoff,
		MaxAttempts:                DefaultMaxAttempts,
		InboxRetention:             DefaultInboxRetention,
		ReplyWaitTimeout:           DefaultReplyWaitTimeout,
		SessionFencing:             DefaultSessionFencing,
		SessionLockDuration:        DefaultSessionLockDuration,
		SessionWaitBackoff:         DefaultSessionWaitBackoff,
		SessionWaitMaxBackoff:      DefaultSessionWaitMaxBackoff,
		MaxTurnEvents:              DefaultMaxTurnEvents,
		MaxTurnBytes:               DefaultMaxTurnBytes,
		ShutdownTimeout:            DefaultShutdownTimeout,
		OutboundMaxAttempts:        DefaultOutboundMaxAttempts,
		OutboundInitialBackoff:     DefaultOutboundInitialBackoff,
		OutboundMaxBackoff:         DefaultOutboundMaxBackoff,
		OutboundSendTimeout:        DefaultOutboundSendTimeout,
		OutboundClaimIdle:          DefaultOutboundClaimIdle,
		PersistenceTimeout:         DefaultPersistenceTimeout,
		PersistenceMaxAttempts:     DefaultPersistenceMaxAttempts,
		PersistenceInitialBackoff:  DefaultPersistenceInitialBackoff,
		PersistenceMaxBackoff:      DefaultPersistenceMaxBackoff,
		PersistencePayloadMaxBytes: DefaultPersistencePayloadMaxBytes,
	}
	if config.LeaseDuration, err = parseOptionalDuration(raw.LeaseDuration, config.LeaseDuration); err != nil {
		return nil, errors.New("messaging lease_duration is invalid")
	}
	if config.HeartbeatInterval, err = parseOptionalDuration(raw.HeartbeatInterval, config.HeartbeatInterval); err != nil {
		return nil, errors.New("messaging heartbeat_interval is invalid")
	}
	if config.InitialBackoff, err = parseOptionalDuration(raw.InitialBackoff, config.InitialBackoff); err != nil {
		return nil, errors.New("messaging initial_backoff is invalid")
	}
	if config.MaxBackoff, err = parseOptionalDuration(raw.MaxBackoff, config.MaxBackoff); err != nil {
		return nil, errors.New("messaging max_backoff is invalid")
	}
	if config.InboxRetention, err = parseOptionalDuration(raw.InboxRetention, config.InboxRetention); err != nil {
		return nil, errors.New("messaging inbox_retention is invalid")
	}
	if config.ReplyWaitTimeout, err = parseOptionalDuration(raw.ReplyWaitTimeout, config.ReplyWaitTimeout); err != nil {
		return nil, errors.New("messaging reply_wait_timeout is invalid")
	}
	if strings.TrimSpace(raw.SessionFencing) != "" {
		config.SessionFencing = strings.ToLower(strings.TrimSpace(raw.SessionFencing))
	}
	if config.SessionLockDuration, err = parseOptionalDuration(raw.SessionLockDuration, config.SessionLockDuration); err != nil {
		return nil, errors.New("messaging session_lock_duration is invalid")
	}
	if config.SessionWaitBackoff, err = parseOptionalDuration(raw.SessionWaitBackoff, config.SessionWaitBackoff); err != nil {
		return nil, errors.New("messaging session_wait_backoff is invalid")
	}
	if config.SessionWaitMaxBackoff, err = parseOptionalDuration(raw.SessionWaitMaxBackoff, config.SessionWaitMaxBackoff); err != nil {
		return nil, errors.New("messaging session_wait_max_backoff is invalid")
	}
	if config.ShutdownTimeout, err = parseOptionalDuration(raw.ShutdownTimeout, config.ShutdownTimeout); err != nil {
		return nil, errors.New("messaging shutdown_timeout is invalid")
	}
	if config.OutboundInitialBackoff, err = parseOptionalDuration(raw.OutboundInitialBackoff, config.OutboundInitialBackoff); err != nil {
		return nil, errors.New("messaging outbound_initial_backoff is invalid")
	}
	if config.OutboundMaxBackoff, err = parseOptionalDuration(raw.OutboundMaxBackoff, config.OutboundMaxBackoff); err != nil {
		return nil, errors.New("messaging outbound_max_backoff is invalid")
	}
	if config.OutboundSendTimeout, err = parseOptionalDuration(raw.OutboundSendTimeout, config.OutboundSendTimeout); err != nil {
		return nil, errors.New("messaging outbound_send_timeout is invalid")
	}
	if config.OutboundClaimIdle, err = parseOptionalDuration(raw.OutboundClaimIdle, config.OutboundClaimIdle); err != nil {
		return nil, errors.New("messaging outbound_claim_idle is invalid")
	}
	if config.PersistenceTimeout, err = parseOptionalDuration(raw.PersistenceTimeout, config.PersistenceTimeout); err != nil {
		return nil, errors.New("messaging persistence_timeout is invalid")
	}
	if config.PersistenceInitialBackoff, err = parseOptionalDuration(raw.PersistenceInitialBackoff, config.PersistenceInitialBackoff); err != nil {
		return nil, errors.New("messaging persistence_initial_backoff is invalid")
	}
	if config.PersistenceMaxBackoff, err = parseOptionalDuration(raw.PersistenceMaxBackoff, config.PersistenceMaxBackoff); err != nil {
		return nil, errors.New("messaging persistence_max_backoff is invalid")
	}
	if raw.MaxTurnEvents != 0 {
		config.MaxTurnEvents = raw.MaxTurnEvents
	}
	if raw.MaxTurnBytes != 0 {
		config.MaxTurnBytes = raw.MaxTurnBytes
	}
	if raw.MaxAttempts != 0 {
		config.MaxAttempts = raw.MaxAttempts
	}
	if raw.OutboundMaxAttempts != 0 {
		config.OutboundMaxAttempts = raw.OutboundMaxAttempts
	}
	if raw.PersistenceMaxAttempts != 0 {
		config.PersistenceMaxAttempts = raw.PersistenceMaxAttempts
	}
	if raw.PersistencePayloadMaxBytes != 0 {
		config.PersistencePayloadMaxBytes = raw.PersistencePayloadMaxBytes
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func legacyMessaging(redisURL, redisPrefix string) MessagingConfig {
	return MessagingConfig{
		RedisCredentialRef:         legacyRedisCredential,
		RedisURL:                   redisURL,
		KeyPrefix:                  strings.TrimRight(valueOrDefault(redisPrefix, DefaultRedisPrefix), ":") + ":messaging",
		LeaseDuration:              DefaultLeaseDuration,
		HeartbeatInterval:          DefaultHeartbeatInterval,
		InitialBackoff:             DefaultInitialBackoff,
		MaxBackoff:                 DefaultMaxBackoff,
		MaxAttempts:                DefaultMaxAttempts,
		InboxRetention:             DefaultInboxRetention,
		ReplyWaitTimeout:           DefaultReplyWaitTimeout,
		SessionFencing:             DefaultSessionFencing,
		SessionLockDuration:        DefaultSessionLockDuration,
		SessionWaitBackoff:         DefaultSessionWaitBackoff,
		SessionWaitMaxBackoff:      DefaultSessionWaitMaxBackoff,
		MaxTurnEvents:              DefaultMaxTurnEvents,
		MaxTurnBytes:               DefaultMaxTurnBytes,
		ShutdownTimeout:            DefaultShutdownTimeout,
		OutboundMaxAttempts:        DefaultOutboundMaxAttempts,
		OutboundInitialBackoff:     DefaultOutboundInitialBackoff,
		OutboundMaxBackoff:         DefaultOutboundMaxBackoff,
		OutboundSendTimeout:        DefaultOutboundSendTimeout,
		OutboundClaimIdle:          DefaultOutboundClaimIdle,
		PersistenceTimeout:         DefaultPersistenceTimeout,
		PersistenceMaxAttempts:     DefaultPersistenceMaxAttempts,
		PersistenceInitialBackoff:  DefaultPersistenceInitialBackoff,
		PersistenceMaxBackoff:      DefaultPersistenceMaxBackoff,
		PersistencePayloadMaxBytes: DefaultPersistencePayloadMaxBytes,
	}
}

func (c MessagingConfig) Validate() error {
	if c.SessionFencing == "" {
		// Config literals from Phase 3 predate the field.
		c.SessionFencing = DefaultSessionFencing
	}
	if c.SessionFencing != "legacy" && c.SessionFencing != "strong" {
		return errors.New("messaging session_fencing must be legacy or strong")
	}
	lockDuration := c.SessionLockDuration
	if lockDuration == 0 {
		lockDuration = c.LeaseDuration
	}
	waitBackoff := c.SessionWaitBackoff
	if waitBackoff == 0 {
		waitBackoff = DefaultSessionWaitBackoff
	}
	waitMaxBackoff := c.SessionWaitMaxBackoff
	if waitMaxBackoff == 0 {
		waitMaxBackoff = DefaultSessionWaitMaxBackoff
	}
	maxTurnEvents := c.MaxTurnEvents
	if maxTurnEvents == 0 {
		maxTurnEvents = DefaultMaxTurnEvents
	}
	maxTurnBytes := c.MaxTurnBytes
	if maxTurnBytes == 0 {
		maxTurnBytes = DefaultMaxTurnBytes
	}
	shutdownTimeout := c.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}
	outboundAttempts := c.OutboundMaxAttempts
	if outboundAttempts == 0 {
		outboundAttempts = DefaultOutboundMaxAttempts
	}
	outboundInitial := c.OutboundInitialBackoff
	if outboundInitial == 0 {
		outboundInitial = DefaultOutboundInitialBackoff
	}
	outboundMax := c.OutboundMaxBackoff
	if outboundMax == 0 {
		outboundMax = DefaultOutboundMaxBackoff
	}
	outboundSendTimeout := c.OutboundSendTimeout
	if outboundSendTimeout == 0 {
		outboundSendTimeout = DefaultOutboundSendTimeout
	}
	outboundClaimIdle := c.OutboundClaimIdle
	if outboundClaimIdle == 0 {
		outboundClaimIdle = DefaultOutboundClaimIdle
	}
	persistenceTimeout := c.PersistenceTimeout
	if persistenceTimeout == 0 {
		persistenceTimeout = DefaultPersistenceTimeout
	}
	persistenceAttempts := c.PersistenceMaxAttempts
	if persistenceAttempts == 0 {
		persistenceAttempts = DefaultPersistenceMaxAttempts
	}
	persistenceInitial := c.PersistenceInitialBackoff
	if persistenceInitial == 0 {
		persistenceInitial = DefaultPersistenceInitialBackoff
	}
	persistenceMax := c.PersistenceMaxBackoff
	if persistenceMax == 0 {
		persistenceMax = DefaultPersistenceMaxBackoff
	}
	persistencePayloadMax := c.PersistencePayloadMaxBytes
	if persistencePayloadMax == 0 {
		persistencePayloadMax = DefaultPersistencePayloadMaxBytes
	}
	prefix := strings.TrimRight(strings.TrimSpace(c.KeyPrefix), ":")
	if prefix == "" || len(prefix) > MaxMessagingPrefixBytes {
		return fmt.Errorf("messaging key_prefix must be between 1 and %d bytes", MaxMessagingPrefixBytes)
	}
	for _, current := range []byte(prefix) {
		if current < 0x21 || current > 0x7e {
			return errors.New("messaging key_prefix must contain printable ASCII without spaces")
		}
	}
	if strings.TrimSpace(c.RedisURL) == "" {
		return errors.New("messaging redis URL is required")
	}
	if _, err := NormalizeRedisURL(c.RedisURL); err != nil {
		return errors.New("messaging redis URL is invalid")
	}
	for name, duration := range map[string]time.Duration{
		"lease_duration": c.LeaseDuration, "heartbeat_interval": c.HeartbeatInterval,
		"initial_backoff": c.InitialBackoff, "max_backoff": c.MaxBackoff,
		"inbox_retention": c.InboxRetention, "reply_wait_timeout": c.ReplyWaitTimeout,
	} {
		if duration < minMessagingDuration {
			return fmt.Errorf("messaging %s must be positive", name)
		}
	}
	if c.HeartbeatInterval > c.LeaseDuration/3 {
		return errors.New("messaging heartbeat_interval must not exceed one third of lease_duration")
	}
	if c.InitialBackoff > c.MaxBackoff {
		return errors.New("messaging initial_backoff must not exceed max_backoff")
	}
	if c.InboxRetention < minInboxRetention {
		return errors.New("messaging inbox_retention must be at least one second")
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > maxMessagingAttempts {
		return fmt.Errorf("messaging max_attempts must be between 1 and %d", maxMessagingAttempts)
	}
	if lockDuration < c.LeaseDuration {
		return errors.New("messaging session_lock_duration must not be less than lease_duration")
	}
	if waitBackoff < minMessagingDuration || waitMaxBackoff < minMessagingDuration {
		return errors.New("messaging session wait backoff must be positive")
	}
	if waitBackoff > waitMaxBackoff {
		return errors.New("messaging session_wait_backoff must not exceed session_wait_max_backoff")
	}
	if maxTurnEvents < 1 || maxTurnEvents > 4096 {
		return errors.New("messaging max_turn_events must be between 1 and 4096")
	}
	if maxTurnBytes < 1 || maxTurnBytes > 16<<20 {
		return errors.New("messaging max_turn_bytes must be between 1 and 16777216")
	}
	if shutdownTimeout < minMessagingDuration {
		return errors.New("messaging shutdown_timeout must be positive")
	}
	if outboundAttempts < 1 || outboundAttempts > maxMessagingAttempts {
		return fmt.Errorf("messaging outbound_max_attempts must be between 1 and %d", maxMessagingAttempts)
	}
	if outboundInitial < minMessagingDuration || outboundMax < minMessagingDuration || outboundSendTimeout < minMessagingDuration || outboundClaimIdle < minMessagingDuration {
		return errors.New("messaging outbound durations must be positive")
	}
	if outboundInitial > outboundMax {
		return errors.New("messaging outbound_initial_backoff must not exceed outbound_max_backoff")
	}
	if persistenceTimeout < minMessagingDuration || persistenceInitial < minMessagingDuration || persistenceMax < minMessagingDuration {
		return errors.New("messaging persistence durations must be positive")
	}
	if persistenceInitial > persistenceMax {
		return errors.New("messaging persistence_initial_backoff must not exceed persistence_max_backoff")
	}
	if persistenceAttempts < 1 || persistenceAttempts > maxMessagingAttempts {
		return fmt.Errorf("messaging persistence_max_attempts must be between 1 and %d", maxMessagingAttempts)
	}
	if persistencePayloadMax < maxTurnBytes || persistencePayloadMax > 16<<20 {
		return errors.New("messaging persistence_payload_max_bytes must be at least max_turn_bytes and no more than 16777216")
	}
	return nil
}

func parseOptionalDuration(raw string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	return time.ParseDuration(strings.TrimSpace(raw))
}
