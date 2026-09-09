package milvus

import (
	"context"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/milvus-io/milvus/client/v2/entity"
	sdk "github.com/milvus-io/milvus/client/v2/milvusclient"
)

// InspectIdentities reads strictly controlled scalar identity fields for the
// given server-owned document IDs. The tenant predicate is derived from the
// trusted context; no vector and no content are ever read. Malformed backend
// rows fail closed instead of leaking into results.
func (s *Store) InspectIdentities(ctx context.Context, documentIDs []string) (map[string]vector.DocumentIdentity, error) {
	result := make(map[string]vector.DocumentIdentity, len(documentIDs))
	if len(documentIDs) == 0 {
		return result, nil
	}
	if len(documentIDs) > maxInspectBatch {
		return nil, vector.ErrInvalidFilter
	}
	var literalIDs []string
	for _, id := range documentIDs {
		literal, err := strictStringLiteral(id)
		if err != nil {
			return nil, vector.ErrInvalidFilter
		}
		literalIDs = append(literalIDs, literal)
	}
	err := s.withCall(ctx, s.cfg.OperationTimeout, false, func(callCtx context.Context, client sdkClient) error {
		if _, err := vector.TrustedTenantContext(ctx); err != nil {
			return err
		}
		expression := fieldID + " in [" + strings.Join(literalIDs, ", ") + "] && " + tenantFilterExpression()
		option := sdk.NewQueryOption(s.cfg.Collection).WithFilter(expression).
			WithTemplateParam(tenantTemplateName, mustTenantID(ctx)).
			WithOutputFields(append(searchOutputFields(), fieldID)...)
		set, err := client.Query(callCtx, option)
		if err != nil {
			return classify(err)
		}
		identities, err := mapQueryIdentities(set, mustTenantID(ctx))
		if err != nil {
			return err
		}
		for _, identity := range identities {
			result[identity.DocumentID] = identity
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListIdentities returns up to limit identities of the trusted tenant in
// document-ID order for bounded orphan observation.
func (s *Store) ListIdentities(ctx context.Context, limit int) ([]vector.DocumentIdentity, error) {
	if limit < 1 || limit > maxInspectBatch {
		return nil, vector.ErrInvalidFilter
	}
	var identities []vector.DocumentIdentity
	err := s.withCall(ctx, s.cfg.OperationTimeout, false, func(callCtx context.Context, client sdkClient) error {
		if _, err := vector.TrustedTenantContext(ctx); err != nil {
			return err
		}
		option := sdk.NewQueryOption(s.cfg.Collection).WithFilter(tenantFilterExpression()).
			WithTemplateParam(tenantTemplateName, mustTenantID(ctx)).
			WithOutputFields(searchOutputFields()...).
			WithLimit(limit)
		set, err := client.Query(callCtx, option)
		if err != nil {
			return classify(err)
		}
		identities, err = mapQueryIdentities(set, mustTenantID(ctx))
		return err
	})
	if err != nil {
		return nil, err
	}
	return identities, nil
}

const maxInspectBatch = 200

func mustTenantID(ctx context.Context) string {
	tenantContext, err := vector.TrustedTenantContext(ctx)
	if err != nil {
		return ""
	}
	return tenantContext.TenantID
}

func mapQueryIdentities(set sdk.ResultSet, tenantID string) ([]vector.DocumentIdentity, error) {
	if set.Err != nil {
		return nil, classify(set.Err)
	}
	count := 0
	if len(set.Fields) > 0 {
		count = set.Fields[0].Len()
	}
	if count < 0 {
		return nil, vector.ErrUnknown
	}
	identities := make([]vector.DocumentIdentity, 0, count)
	for i := 0; i < count; i++ {
		idValue := fieldsColumn(set.Fields, fieldID)
		if idValue == nil || idValue.Type() != entity.FieldTypeVarChar {
			return nil, vector.ErrInvalidSchema
		}
		rawID, idErr := idValue.Get(i)
		if idErr != nil {
			return nil, vector.ErrUnknown
		}
		documentID, ok := rawID.(string)
		if !ok || documentID == "" {
			return nil, vector.ErrInvalidSchema
		}
		values, err := readSearchRow(set.Fields, i)
		if err != nil {
			return nil, err
		}
		if values.tenantID != tenantID || values.operation != string(vector.OperationUpsert) {
			return nil, vector.ErrInvalidTenant
		}
		identities = append(identities, vector.DocumentIdentity{
			TenantID: tenantID, DocumentID: documentID, SourceType: values.sourceType,
			SourceID: values.sourceID, ProjectionScope: values.projection,
			SourceVersion: values.sourceVersion, SourceSequence: values.sourceSequence,
			ContentHash: values.contentHash, Operation: vector.OperationUpsert,
			Model: values.model, ModelVersion: values.modelVersion,
			Dimension: values.dimension, SchemaVersion: values.schemaVersion,
		})
	}
	return identities, nil
}
