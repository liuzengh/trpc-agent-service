package postgresadapter

import (
	"context"
	"encoding/json"
	controlruntimev1 "github.com/liuzengh/trpc-agent-service/api/runtime/control/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
)

func (s *Store) ManifestExportUpper(ctx context.Context) (*controlruntimev1.ExportPosition, error) {
	var raw []byte
	err := s.db.QueryRow(ctx, `SELECT COALESCE((SELECT jsonb_build_object('created_at',created_at,'tenant_id',tenant_id,'event_id',id) FROM control_outbox WHERE event_type='RuntimeManifestPublished.v1' AND aggregate_type='deployment' ORDER BY created_at DESC,tenant_id COLLATE "C" DESC,id COLLATE "C" DESC LIMIT 1),'null'::jsonb)`).Scan(&raw)
	if err != nil {
		return nil, application.ErrManifestDistributionUnavailable
	}
	var p *controlruntimev1.ExportPosition
	if json.Unmarshal(raw, &p) != nil {
		return nil, application.ErrManifestDistributionUnavailable
	}
	return p, nil
}
func (s *Store) ManifestExportRows(ctx context.Context, after, upper *controlruntimev1.ExportPosition, limit int) ([]application.ManifestExportRow, error) {
	if upper == nil || limit < 1 || limit > controlruntimev1.MaxExportPageSize+1 {
		return nil, application.ErrManifestExportCursor
	}
	var afterTime, afterTenant, afterID any
	if after != nil {
		afterTime = after.CreatedAt
		afterTenant = after.TenantID
		afterID = after.EventID
	}
	var raw []byte
	err := s.db.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('position',jsonb_build_object('created_at',created_at,'tenant_id',tenant_id,'event_id',id),'payload',payload_jsonb,'digest',payload_digest) ORDER BY created_at,tenant_id COLLATE "C",id COLLATE "C"),'[]'::jsonb)
 FROM (SELECT created_at,tenant_id,id,payload_jsonb,payload_digest FROM control_outbox WHERE event_type='RuntimeManifestPublished.v1' AND aggregate_type='deployment'
 AND (created_at,tenant_id COLLATE "C",id COLLATE "C")<=($1::timestamptz,$2::text COLLATE "C",$3::text COLLATE "C")
 AND ($4::timestamptz IS NULL OR (created_at,tenant_id COLLATE "C",id COLLATE "C")>($4::timestamptz,$5::text COLLATE "C",$6::text COLLATE "C"))
 ORDER BY created_at,tenant_id COLLATE "C",id COLLATE "C" LIMIT $7) events`, upper.CreatedAt, upper.TenantID, upper.EventID, afterTime, afterTenant, afterID, limit).Scan(&raw)
	if err != nil {
		return nil, application.ErrManifestDistributionUnavailable
	}
	var rows []application.ManifestExportRow
	if json.Unmarshal(raw, &rows) != nil {
		return nil, application.ErrManifestDistributionUnavailable
	}
	return rows, nil
}
