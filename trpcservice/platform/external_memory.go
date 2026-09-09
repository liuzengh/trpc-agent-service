package platform

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/mem0"
)

type ExternalMemoryConfig struct {
	Host       string `json:"host,omitempty"`
	APIKeyEnv  string `json:"api_key_env,omitempty"`
	SelfHosted bool   `json:"self_hosted,omitempty"`
}

func (s *compositeStore) configureExternalMemory(config ExternalMemoryConfig) error {
	if config.Host == "" {
		return nil
	}
	options := []mem0.ServiceOpt{mem0.WithHost(config.Host), mem0.WithAsyncMode(false), mem0.WithIngestInference(false)}
	if config.SelfHosted {
		options = append(options, mem0.WithSelfHostedOSS())
	} else {
		key := os.Getenv(config.APIKeyEnv)
		if key == "" {
			return errors.New("external memory credentials unavailable")
		}
		options = append(options, mem0.WithAPIKey(key))
	}
	service, err := mem0.NewService(options...)
	if err != nil {
		return err
	}
	s.externalMemory = service
	return nil
}
func (s *compositeStore) syncExternalMemory(ctx context.Context, item MemoryRecord) error {
	if s.externalMemory == nil {
		return nil
	}
	payload, _ := json.Marshal(map[string]string{"input": item.Key + "=" + item.Value})
	sess := frameworkSession(item.TenantID, "memory", item.SessionID, []SessionEvent{{ID: item.ID, Type: "message.input", Payload: payload, OccurredAt: item.UpdatedAt}})
	return s.externalMemory.IngestSession(ctx, sess)
}

// ContextMemory preserves the selected backend's read-after-write guarantee.
// Mem0 contributes eventually visible facts; it never replaces authoritative keys.
func (s *compositeStore) ContextMemory(ctx context.Context, tenant, session string) ([]MemoryRecord, error) {
	records, err := s.ListMemory(ctx, tenant, session)
	if err != nil || s.externalMemory == nil {
		return records, err
	}
	scope := memory.UserKey{AppName: tenant + ":memory", UserID: tenant + ":" + session}
	entries, err := s.externalMemory.ReadMemories(ctx, scope, 20)
	if err != nil {
		return records, nil
	}
	for _, entry := range entries {
		if entry == nil || entry.Memory == nil || entry.AppName != scope.AppName || entry.UserID != scope.UserID {
			continue
		}
		records = append(records, MemoryRecord{ID: entry.ID, TenantID: tenant, SessionID: session, Key: "external:" + entry.ID, Value: entry.Memory.Memory, UpdatedAt: entry.UpdatedAt})
	}
	return records, nil
}
