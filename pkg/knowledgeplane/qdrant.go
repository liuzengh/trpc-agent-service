package knowledgeplane

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"

	qdrantstore "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

var (
	ErrInvalidQdrantConfig = errors.New("invalid Qdrant configuration")
	ErrInsecureQdrant      = errors.New("plaintext Qdrant is allowed only for explicit loopback development")
	qdrantCollection       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
)

// QdrantConfig is operator-owned. APIKey must come from a secret resolver;
// tenant JSON stores only a profile reference.
type QdrantConfig struct {
	Host          string
	Port          int
	APIKey        string
	TLS           bool
	AllowInsecure bool
	Collection    string
	Dimension     int
}

func (c QdrantConfig) Validate() error {
	host := strings.TrimSpace(c.Host)
	if host == "" || len(host) > 253 || strings.ContainsAny(host, "/\\:@\x00\r\n") ||
		c.Port < 1 || c.Port > 65535 || c.Dimension < 1 || c.Dimension > 65536 ||
		!qdrantCollection.MatchString(c.Collection) {
		return ErrInvalidQdrantConfig
	}
	if !c.TLS {
		if !c.AllowInsecure || !isLoopbackHost(host) {
			return ErrInsecureQdrant
		}
	}
	return nil
}

func NewQdrantScopedStore(ctx context.Context, tenantID, agentAppID string, config QdrantConfig) (*ScopedStore, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	options := []qdrantstore.Option{
		qdrantstore.WithHost(strings.TrimSpace(config.Host)),
		qdrantstore.WithPort(config.Port),
		qdrantstore.WithTLS(config.TLS),
		qdrantstore.WithCollectionName(config.Collection),
		qdrantstore.WithDimension(config.Dimension),
	}
	if config.APIKey != "" {
		options = append(options, qdrantstore.WithAPIKey(config.APIKey))
	}
	backend, err := qdrantstore.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("initialize Qdrant knowledge profile: %w", err)
	}
	store, err := NewScopedStore(tenantID, agentAppID, backend)
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	return store, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}
