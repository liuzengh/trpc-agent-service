package knowledgeplane

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/proto"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

type DocumentIdentity struct {
	AppName    string
	DocumentID string
}

// ScanQdrantDocuments uses Qdrant's point cursor, independent of row counts.
// Both the server filter and returned payload are checked for tenant scope.
func ScanQdrantDocuments(ctx context.Context, tenantID string, config QdrantConfig, cursor string, limit int) ([]DocumentIdentity, string, bool, error) {
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return nil, "", false, ErrScopeViolation
	}
	if err := config.Validate(); err != nil {
		return nil, "", false, err
	}
	if limit < 1 || limit > 1000 {
		return nil, "", false, ErrScopeViolation
	}
	var offset *qdrant.PointId
	if cursor != "" {
		encoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(encoded) > 256 {
			return nil, "", false, ErrScopeViolation
		}
		offset = &qdrant.PointId{}
		if err := proto.Unmarshal(encoded, offset); err != nil || offset.PointIdOptions == nil {
			return nil, "", false, ErrScopeViolation
		}
	}
	client, err := qdrant.NewClient(&qdrant.Config{Host: config.Host, Port: config.Port, APIKey: config.APIKey, UseTLS: config.TLS})
	if err != nil {
		return nil, "", false, err
	}
	defer client.Close()
	points, next, err := client.ScrollAndOffset(ctx, &qdrant.ScrollPoints{
		CollectionName: config.Collection, Offset: offset, Limit: qdrant.PtrOf(uint32(limit)),
		Filter:      &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatch("metadata."+metadataTenantID, tenantID)}},
		WithPayload: qdrant.NewWithPayload(true), WithVectors: qdrant.NewWithVectors(false),
	})
	if err != nil {
		return nil, "", false, err
	}
	identities := make([]DocumentIdentity, 0, len(points))
	for _, point := range points {
		if point == nil {
			return nil, "", false, ErrScopeViolation
		}
		fields := point.Payload["metadata"].GetStructValue().GetFields()
		identity := DocumentIdentity{AppName: fields[metadataAgentAppID].GetStringValue(), DocumentID: fields[metadataDocumentID].GetStringValue()}
		if fields[metadataTenantID].GetStringValue() != tenantID || tenant.ValidateAgentAppName(identity.AppName) != nil || identity.DocumentID == "" {
			return nil, "", false, ErrScopeViolation
		}
		identities = append(identities, identity)
	}
	if next == nil {
		return identities, "", true, nil
	}
	encoded, err := proto.Marshal(next)
	if err != nil {
		return nil, "", false, fmt.Errorf("encode knowledge cursor: %w", err)
	}
	return identities, base64.RawURLEncoding.EncodeToString(encoded), false, nil
}
