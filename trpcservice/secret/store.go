// Package secret resolves credential references without exposing values to
// control-plane records, logs or traces.
package secret

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var ErrNotFound = errors.New("secret not found")
var ErrForbidden = errors.New("secret access forbidden")

const (
	Model           = "model"
	MCPServer       = "mcp_server"
	Session         = "session"
	Memory          = "memory"
	Artifact        = "artifact"
	Knowledge       = "knowledge"
	Embedding       = "embedding"
	TelegramWebhook = "telegram_webhook"
	TelegramBot     = "telegram_bot"
	TelegramMedia   = "telegram_media"
	WeComCallback   = "wecom_callback"
	WeComAES        = "wecom_aes"
	WeComApp        = "wecom_app"
	WeComMCPRead    = "wecom_mcp_read"
	WeComMCPSend    = "wecom_mcp_send"
)

// Grant is deployment-owned authorization, never tenant-editable metadata.
type Grant struct {
	TenantID  string `json:"tenant_id"`
	Purpose   string `json:"purpose"`
	Reference string `json:"reference"`
}

type Authorizer interface {
	Authorize(ctx context.Context, tenantID, purpose, reference string) error
}

type Store interface {
	Resolve(ctx context.Context, tenantID, purpose, reference string) (string, error)
}

// EnvStore resolves only exact, explicitly granted env://VARIABLE references.
// The zero value denies all access, including when a variable exists.
type EnvStore struct{ grants map[Grant]bool }

var tenantPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var referencePattern = regexp.MustCompile(`^env://[A-Z][A-Z0-9_]{0,127}$`)

func ValidPurpose(value string) bool {
	switch value {
	case Model, MCPServer, Session, Memory, Artifact, Knowledge, Embedding, TelegramWebhook, TelegramBot, TelegramMedia, WeComCallback, WeComAES, WeComApp, WeComMCPRead, WeComMCPSend:
		return true
	}
	return false
}

func NewEnvStore(grants []Grant) (*EnvStore, error) {
	store := &EnvStore{grants: make(map[Grant]bool)}
	for _, grant := range grants {
		if !tenantPattern.MatchString(grant.TenantID) || !ValidPurpose(grant.Purpose) ||
			!referencePattern.MatchString(grant.Reference) || store.grants[grant] {
			return nil, errors.New("invalid or duplicate secret grant")
		}
		store.grants[grant] = true
	}
	return store, nil
}

func (s EnvStore) Authorize(ctx context.Context, tenantID, purpose, reference string) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if !s.grants[Grant{TenantID: tenantID, Purpose: purpose, Reference: reference}] {
		return ErrForbidden
	}
	return nil
}

func (s EnvStore) Resolve(ctx context.Context, tenantID, purpose, reference string) (string, error) {
	if err := s.Authorize(ctx, tenantID, purpose, reference); err != nil {
		return "", err
	}
	const prefix = "env://"
	if !strings.HasPrefix(reference, prefix) {
		return "", fmt.Errorf("unsupported secret reference: %w", ErrNotFound)
	}
	name := strings.TrimSpace(strings.TrimPrefix(reference, prefix))
	if name == "" {
		return "", fmt.Errorf("empty secret environment name")
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", ErrNotFound
	}
	return value, nil
}

// StaticStore is used by tests and controlled local fixtures.
type StaticStore map[string]string

func (s StaticStore) Resolve(ctx context.Context, _, _, reference string) (string, error) {
	if ctx != nil && ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	value, ok := s[reference]
	if !ok {
		return "", fmt.Errorf("secret reference %q: %w", reference, ErrNotFound)
	}
	return value, nil
}
