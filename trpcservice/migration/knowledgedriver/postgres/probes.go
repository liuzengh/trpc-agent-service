package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// ProbeSource reads the durable, verified Knowledge probes used by shadow
// verification. The migration watermark is accepted as an explicit fence; its
// value belongs to the exact configured Qdrant snapshot and is never inferred
// from a backend response.
type ProbeSource struct{ db *sql.DB }

func NewProbeSource(db *sql.DB) *ProbeSource { return &ProbeSource{db: db} }

func (s *ProbeSource) Probes(ctx context.Context, tenantID, watermark string) ([]knowledgedriver.Probe, error) {
	if s == nil || s.db == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if tenantID == "" || watermark == "" {
		return nil, runtime.ErrInvariantViolation
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.probe_id,p.knowledge_id,p.knowledge_version,p.query,p.expected_chunks,p.min_recall_ppm
FROM public.knowledge_probe AS p
JOIN public.knowledge_manifest AS m
  ON m.tenant_id=p.tenant_id AND m.knowledge_id=p.knowledge_id AND m.version=p.knowledge_version
WHERE p.tenant_id=$1 AND p.verified=true AND m.state IN ('verifying','published')
ORDER BY p.probe_id`, tenantID)
	if err != nil {
		return nil, mapProbeError(err)
	}
	defer rows.Close()
	result := make([]knowledgedriver.Probe, 0)
	for rows.Next() {
		var probe knowledgedriver.Probe
		var expected []byte
		if err := rows.Scan(&probe.ProbeID, &probe.KnowledgeID, &probe.KnowledgeVersion, &probe.Query, &expected, &probe.MinRecallPPM); err != nil {
			return nil, mapProbeError(err)
		}
		// knowledge_probe identity is scoped to a Knowledge version, while the
		// migration verifier requires probe IDs unique across the whole tenant.
		// Preserve that full durable coordinate in evidence rather than rejecting
		// valid versions that both happen to name a sample "smoke".
		probe.ProbeID = probe.KnowledgeID + ":" + strconv.FormatInt(probe.KnowledgeVersion, 10) + ":" + probe.ProbeID
		var chunks []string
		if err := json.Unmarshal(expected, &chunks); err != nil || len(chunks) == 0 {
			return nil, runtime.ErrInvariantViolation
		}
		probe.TenantID = tenantID
		probe.Expected = make([]knowledgedriver.ChunkKey, 0, len(chunks))
		for _, chunkID := range chunks {
			if chunkID == "" {
				return nil, runtime.ErrInvariantViolation
			}
			probe.Expected = append(probe.Expected, knowledgedriver.ChunkKey{TenantID: tenantID, KnowledgeID: probe.KnowledgeID, KnowledgeVersion: probe.KnowledgeVersion, ChunkID: chunkID})
		}
		result = append(result, probe)
	}
	if err := rows.Err(); err != nil {
		return nil, mapProbeError(err)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ProbeID < result[j].ProbeID })
	return result, nil
}

func mapProbeError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrNotFound
	}
	return err
}

var _ knowledgedriver.ProbeSource = (*ProbeSource)(nil)
