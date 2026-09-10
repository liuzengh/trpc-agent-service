// Package redis wires tenant-scoped Redis Session services.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	goredis "github.com/redis/go-redis/v9"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	redisprovider "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// SessionResolver caches framework Redis Session services selected by immutable
// application configuration. Its provider keeps event persistence synchronous.
type SessionResolver struct {
	secrets    platformsecret.SecretProvider
	defaultURL string
	mu         sync.Mutex
	closed     bool
	services   map[string]frameworksession.Service
}

// NewSessionResolver creates a Redis session resolver backed by a scoped
// secret provider.
func NewSessionResolver(secrets platformsecret.SecretProvider, defaultURLs ...string) (*SessionResolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if len(defaultURLs) > 1 {
		return nil, errors.New("only one default redis url is supported")
	}
	defaultURL := ""
	if len(defaultURLs) == 1 {
		defaultURL = strings.TrimSpace(defaultURLs[0])
	}
	return &SessionResolver{secrets: secrets, defaultURL: defaultURL, services: make(map[string]frameworksession.Service)}, nil
}

// ResolveSession returns a framework Redis Session service for one execution.
func (r *SessionResolver) ResolveSession(ctx context.Context, exec worker.Execution) (frameworksession.Service, error) {
	if r == nil || r.secrets == nil {
		return nil, errors.New("redis session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Session
	if ref.Kind != tenant.BackendRedis || ref.Provider != "redis" {
		return nil, fmt.Errorf("session backend %q must use redis provider", ref.Name)
	}
	key, err := sessionCacheKey(exec)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("redis session resolver is closed")
	}
	if service := r.services[key]; service != nil {
		return service, nil
	}
	url, err := r.resolveURL(ctx, exec)
	if err != nil {
		return nil, err
	}
	service, err := redisprovider.NewService(redisprovider.WithRedisClientURL(url))
	if err != nil {
		return nil, fmt.Errorf("create redis session service: %w", err)
	}
	r.services[key] = service
	return service, nil
}

// ListSessionKeys inventories both current HashIdx metadata and legacy ZSet
// session state. The SQL session_lane table is not authoritative for Redis,
// so migration must inspect Redis itself and fail closed on Redis errors.
func (r *SessionResolver) ListSessionKeys(ctx context.Context, exec worker.Execution) ([]frameworksession.Key, error) {
	if r == nil || r.secrets == nil {
		return nil, errors.New("redis session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Session
	if ref.Kind != tenant.BackendRedis || ref.Provider != "redis" {
		return nil, fmt.Errorf("session backend %q must use redis provider", ref.Name)
	}
	appName, err := exec.Tenant.Scope().Key("runner")
	if err != nil {
		return nil, fmt.Errorf("build session app name: %w", err)
	}
	url, err := r.resolveURL(ctx, exec)
	if err != nil {
		return nil, err
	}
	options, err := goredis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse session redis url: %w", err)
	}
	client := goredis.NewClient(options)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping session redis: %w", err)
	}

	keys := make(map[frameworksession.Key]struct{})
	if err := listLegacyZSetSessionKeys(ctx, client, appName, keys); err != nil {
		return nil, err
	}
	if err := listHashIdxSessionKeys(ctx, client, appName, keys); err != nil {
		return nil, err
	}
	result := make([]frameworksession.Key, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].AppName != result[j].AppName {
			return result[i].AppName < result[j].AppName
		}
		if result[i].UserID != result[j].UserID {
			return result[i].UserID < result[j].UserID
		}
		return result[i].SessionID < result[j].SessionID
	})
	return result, nil
}

func (r *SessionResolver) resolveURL(ctx context.Context, exec worker.Execution) (string, error) {
	ref := exec.Config.BackendConfig.Session
	url := r.defaultURL
	if ref.SecretRef != (tenant.SecretRef{}) {
		var err error
		url, err = r.secrets.ResolveSecret(ctx, exec.Tenant.Scope(), ref.SecretRef)
		if err != nil {
			return "", fmt.Errorf("resolve session url: %w", err)
		}
	}
	if strings.TrimSpace(url) == "" {
		return "", errors.New("session redis url is required")
	}
	return strings.TrimSpace(url), nil
}

func listLegacyZSetSessionKeys(
	ctx context.Context,
	client *goredis.Client,
	appName string,
	keys map[frameworksession.Key]struct{},
) error {
	iter := client.Scan(ctx, 0, "sess:{"+redisMatchLiteral(appName)+"}:*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		const prefix = "sess:{"
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		remainder := key[len(prefix):]
		closeBrace := strings.Index(remainder, "}:")
		if closeBrace <= 0 || remainder[:closeBrace] != appName {
			continue
		}
		userID := remainder[closeBrace+2:]
		if userID == "" {
			continue
		}
		sessionIDs, err := client.HKeys(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("list legacy redis sessions: %w", err)
		}
		for _, sessionID := range sessionIDs {
			if sessionID != "" {
				keys[frameworksession.Key{AppName: appName, UserID: userID, SessionID: sessionID}] = struct{}{}
			}
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan legacy redis sessions: %w", err)
	}
	return nil
}

func listHashIdxSessionKeys(
	ctx context.Context,
	client *goredis.Client,
	appName string,
	keys map[frameworksession.Key]struct{},
) error {
	const prefix = "hashidx:meta:"
	iter := client.Scan(ctx, 0, prefix+redisMatchLiteral(appName)+":*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		if !strings.HasPrefix(key, prefix+appName+":") {
			continue
		}
		payload, err := client.Get(ctx, key).Bytes()
		if errors.Is(err, goredis.Nil) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read redis session metadata: %w", err)
		}
		var meta struct {
			ID      string `json:"id"`
			AppName string `json:"appName"`
			UserID  string `json:"userID"`
		}
		if err := json.Unmarshal(payload, &meta); err != nil {
			return fmt.Errorf("decode redis session metadata: %w", err)
		}
		if meta.AppName != appName || meta.UserID == "" || meta.ID == "" {
			return errors.New("redis session metadata scope is invalid")
		}
		keys[frameworksession.Key{AppName: appName, UserID: meta.UserID, SessionID: meta.ID}] = struct{}{}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan redis session metadata: %w", err)
	}
	return nil
}

// Close closes cached Session services.
func redisMatchLiteral(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `*`, `\*`)
	value = strings.ReplaceAll(value, `?`, `\?`)
	value = strings.ReplaceAll(value, `[`, `\[`)
	return value
}

func (r *SessionResolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := r.services
	r.services = nil
	r.mu.Unlock()
	var errs []error
	for _, service := range services {
		if err := service.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func sessionCacheKey(exec worker.Execution) (string, error) {
	ref := exec.Config.BackendConfig.Session
	parts := []string{exec.Tenant.ConfigVersion, exec.Config.BackendConfig.Name, ref.Provider, ref.Name}
	if ref.SecretRef != (tenant.SecretRef{}) {
		parts = append(parts, ref.SecretRef.Name, ref.SecretRef.Version)
	}
	return exec.Tenant.Scope().Key("session-service", parts...)
}
