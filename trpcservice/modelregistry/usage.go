package modelregistry

import "context"

type AppUsage struct {
	AppID               string `json:"app_id"`
	Name                string `json:"name"`
	Stable              bool   `json:"stable"`
	Canary              bool   `json:"canary"`
	Draft               bool   `json:"draft"`
	HistoricalRevisions int64  `json:"historical_revisions"`
}

// Usage returns references, not conversations or plaintext draft contents.
// Historical versions may still be pinned by conversations; listing one is
// not evidence that it currently has traffic. No applications are auto-edited.
func (s *Store) Usage(ctx context.Context, tenant, id, after string) ([]AppUsage, string, error) {
	items := []AppUsage{}
	if _, err := s.Get(ctx, tenant, id); err != nil {
		return nil, "", err
	}
	rows, err := s.db.QueryContext(ctx, `
WITH refs AS (
 SELECT r.app_id,
        bool_or(a.stable_revision_id=r.revision_id) AS stable,
        bool_or(a.rollout_policy->>'canary_revision_id'=r.revision_id) AS canary,
        false AS draft,count(*) AS historical_revisions
 FROM agent_revision r JOIN agent_app a ON a.tenant_id=r.tenant_id AND a.app_id=r.app_id
 WHERE r.tenant_id=$1 AND lower(btrim(r.model_config->>'source'))='connection'
       AND r.model_config->>'connection_id'=$2
 GROUP BY r.app_id
 UNION ALL
 SELECT d.app_id,false,false,true,0 FROM agent_draft d
 WHERE d.tenant_id=$1 AND d.expires_at>now()
       AND lower(btrim(d.data#>>'{config,model_config,source}'))='connection'
       AND d.data#>>'{config,model_config,connection_id}'=$2
)
SELECT a.app_id,a.name,COALESCE(bool_or(r.stable),false),COALESCE(bool_or(r.canary),false),
       bool_or(r.draft),sum(r.historical_revisions)::bigint
FROM refs r JOIN agent_app a ON a.tenant_id=$1 AND a.app_id=r.app_id
WHERE a.app_id>$3 GROUP BY a.app_id,a.name ORDER BY a.app_id LIMIT 101`, tenant, id, after)
	if err != nil {
		return nil, "", mapError(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item AppUsage
		if err := rows.Scan(&item.AppID, &item.Name, &item.Stable, &item.Canary, &item.Draft, &item.HistoricalRevisions); err != nil {
			return nil, "", mapError(err)
		}
		if len(items) == 100 {
			return items, items[99].AppID, nil
		}
		items = append(items, item)
	}
	return items, "", mapError(rows.Err())
}
