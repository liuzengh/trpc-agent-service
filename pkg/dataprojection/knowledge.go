package dataprojection

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

const knowledgeKeyPrefix = "knowledge/v1/"

var ErrInvalidProjection = errors.New("invalid data projection")

type knowledgeIdentity struct {
	AgentAppID string `json:"agent_app_id"`
	DocumentID string `json:"document_id"`
}

type knowledgePayload struct {
	Document  *document.Document `json:"document"`
	Embedding []float64          `json:"embedding"`
}

// KnowledgeStoreResolver resolves an operator-owned, tenant-scoped vector
// store. Raw Qdrant credentials never cross this boundary.
type KnowledgeStoreResolver func(context.Context, string, string) (*knowledgeplane.ScopedStore, error)

// KnowledgeProjector applies canonical migration records to a real vector
// store. Applying the same record more than once is safe because the physical
// point ID is deterministic and Qdrant Add has upsert semantics.
type KnowledgeProjector struct{ resolve KnowledgeStoreResolver }

func NewKnowledgeProjector(resolve KnowledgeStoreResolver) (*KnowledgeProjector, error) {
	if resolve == nil {
		return nil, fmt.Errorf("%w: knowledge resolver", ErrInvalidProjection)
	}
	return &KnowledgeProjector{resolve: resolve}, nil
}

func (p *KnowledgeProjector) Domain() datamigration.Domain { return datamigration.DomainKnowledge }

func (p *KnowledgeProjector) Apply(ctx context.Context, tenantID string, record datamigration.Record) error {
	if p == nil || p.resolve == nil {
		return fmt.Errorf("%w: knowledge projector", ErrInvalidProjection)
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fmt.Errorf("%w: tenant", ErrInvalidProjection)
	}
	if err := record.Validate(); err != nil {
		return err
	}
	identity, err := decodeKnowledgeIdentity(record.Key)
	if err != nil {
		return err
	}
	store, err := p.resolve(nonNilProjectionContext(ctx), tenantID, identity.AgentAppID)
	if err != nil {
		return fmt.Errorf("resolve knowledge projection store: %w", err)
	}
	if store == nil {
		return fmt.Errorf("%w: nil knowledge store", ErrInvalidProjection)
	}
	if record.Deleted {
		return store.Delete(nonNilProjectionContext(ctx), identity.DocumentID)
	}
	var payload knowledgePayload
	if err := decodeStrictJSON(record.Payload, &payload); err != nil || payload.Document == nil || payload.Document.ID != identity.DocumentID {
		return fmt.Errorf("%w: knowledge payload identity", ErrInvalidProjection)
	}
	if err := validateEmbedding(payload.Embedding); err != nil {
		return err
	}
	if err := store.Add(nonNilProjectionContext(ctx), payload.Document, payload.Embedding); err != nil {
		return fmt.Errorf("project knowledge document: %w", err)
	}
	return nil
}

// NewKnowledgeRecord produces the stable wire representation consumed by
// KnowledgeProjector. The identity is carried in the key so tombstones remain
// replayable even though deleted records have an empty payload.
func NewKnowledgeRecord(agentAppID string, doc *document.Document, embedding []float64, version int64) (datamigration.Record, error) {
	if err := tenant.ValidateAgentAppName(agentAppID); err != nil || doc == nil || strings.TrimSpace(doc.ID) == "" {
		return datamigration.Record{}, fmt.Errorf("%w: knowledge identity", ErrInvalidProjection)
	}
	if err := validateEmbedding(embedding); err != nil {
		return datamigration.Record{}, err
	}
	key, err := encodeKnowledgeIdentity(knowledgeIdentity{AgentAppID: agentAppID, DocumentID: doc.ID})
	if err != nil {
		return datamigration.Record{}, err
	}
	payload, err := json.Marshal(knowledgePayload{Document: doc.Clone(), Embedding: append([]float64(nil), embedding...)})
	if err != nil {
		return datamigration.Record{}, fmt.Errorf("%w: encode knowledge payload: %v", ErrInvalidProjection, err)
	}
	record := datamigration.Record{Key: key, Version: version, Payload: payload, Hash: projectionHash(payload)}
	if err := record.Validate(); err != nil {
		return datamigration.Record{}, err
	}
	return record, nil
}

func NewKnowledgeTombstone(agentAppID, documentID string, version int64) (datamigration.Record, error) {
	key, err := encodeKnowledgeIdentity(knowledgeIdentity{AgentAppID: agentAppID, DocumentID: documentID})
	if err != nil {
		return datamigration.Record{}, err
	}
	record := datamigration.Record{Key: key, Version: version, Payload: []byte{}, Hash: projectionHash(nil), Deleted: true}
	if err := record.Validate(); err != nil {
		return datamigration.Record{}, err
	}
	return record, nil
}

func encodeKnowledgeIdentity(identity knowledgeIdentity) (string, error) {
	if err := tenant.ValidateAgentAppName(identity.AgentAppID); err != nil || strings.TrimSpace(identity.DocumentID) == "" || strings.ContainsAny(identity.DocumentID, "\x00\r\n") {
		return "", fmt.Errorf("%w: knowledge identity", ErrInvalidProjection)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("%w: encode knowledge identity", ErrInvalidProjection)
	}
	return knowledgeKeyPrefix + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeKnowledgeIdentity(key string) (knowledgeIdentity, error) {
	if !strings.HasPrefix(key, knowledgeKeyPrefix) {
		return knowledgeIdentity{}, fmt.Errorf("%w: knowledge record key", ErrInvalidProjection)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(key, knowledgeKeyPrefix))
	if err != nil {
		return knowledgeIdentity{}, fmt.Errorf("%w: knowledge record key", ErrInvalidProjection)
	}
	var identity knowledgeIdentity
	if err := decodeStrictJSON(raw, &identity); err != nil {
		return knowledgeIdentity{}, fmt.Errorf("%w: knowledge record key", ErrInvalidProjection)
	}
	canonical, err := encodeKnowledgeIdentity(identity)
	if err != nil || canonical != key {
		return knowledgeIdentity{}, fmt.Errorf("%w: non-canonical knowledge record key", ErrInvalidProjection)
	}
	return identity, nil
}

func validateEmbedding(values []float64) error {
	if len(values) == 0 || len(values) > 65536 {
		return fmt.Errorf("%w: embedding dimension", ErrInvalidProjection)
	}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%w: non-finite embedding", ErrInvalidProjection)
		}
	}
	return nil
}

func decodeStrictJSON(value []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func projectionHash(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func nonNilProjectionContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
