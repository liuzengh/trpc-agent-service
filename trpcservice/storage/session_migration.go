package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	redisclient "github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// ImportSessionSummaries persists an already-generated summary snapshot
// without invoking a model. It is intentionally a migration-only operation;
// runtime summary generation continues through the upstream Session service.
func (router *Router) ImportSessionSummaries(ctx context.Context, tenantID, appID string, route tenant.BackendConfig, key session.Key, summaries map[string]*session.Summary) error {
	if router == nil || ctx == nil || tenantID == "" || appID == "" {
		return errors.New("storage: session summary import scope is required")
	}
	appName, err := tenant.CanonicalAppName(tenantID, appID)
	if err != nil || key.AppName != appName || key.UserID == "" || key.SessionID == "" {
		return errors.New("storage: session summary import scope is invalid")
	}
	if len(summaries) == 0 {
		return nil
	}
	normalized := cloneSummaries(summaries)
	route = route.Clone()
	route.MigrationTarget = nil
	switch route.Type {
	case tenant.BackendPostgres:
		target, err := router.ResolveForScope(ctx, tenantID, appID, route)
		if err != nil {
			return errors.New("storage: PostgreSQL summary target unavailable")
		}
		return importPostgresSummaries(ctx, target.DB, key, normalized)
	case tenant.BackendRedis:
		rawURL, prefix, err := router.resolveRedisSessionRoute(ctx, tenantID, appID, route)
		if err != nil {
			return errors.New("storage: Redis summary target unavailable")
		}
		return importRedisSummaries(ctx, rawURL, prefix, key, normalized)
	default:
		return errors.New("storage: session summary target is unsupported")
	}
}

func cloneSummaries(source map[string]*session.Summary) map[string]*session.Summary {
	result := make(map[string]*session.Summary, len(source))
	for filter, summary := range source {
		if summary == nil {
			continue
		}
		cloned := summary.Clone()
		cloned.UpdatedAt = cloned.UpdatedAt.UTC()
		result[filter] = cloned
	}
	return result
}

func importPostgresSummaries(ctx context.Context, db *sql.DB, key session.Key, summaries map[string]*session.Summary) error {
	if db == nil {
		return errors.New("storage: PostgreSQL summary target unavailable")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return errors.New("storage: begin PostgreSQL summary import failed")
	}
	defer tx.Rollback()
	var sessionCreatedAt time.Time
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM runtime_session_states
		WHERE app_name=$1 AND user_id=$2 AND session_id=$3 AND deleted_at IS NULL`, key.AppName, key.UserID, key.SessionID).Scan(&sessionCreatedAt); err != nil {
		return errors.New("storage: PostgreSQL summary session is unavailable")
	}
	for filter, summary := range summaries {
		payload, err := json.Marshal(summary)
		if err != nil {
			return errors.New("storage: encode session summary failed")
		}
		updatedAt := summary.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = time.Unix(0, 0).UTC()
		}
		visibilityAt := updatedAt
		if sessionCreatedAt.After(visibilityAt) {
			visibilityAt = sessionCreatedAt
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_session_summaries
			(app_name,user_id,session_id,filter_key,summary,updated_at,expires_at,deleted_at)
			VALUES ($1,$2,$3,$4,$5,$6,NULL,NULL)
			ON CONFLICT (app_name,user_id,session_id,filter_key) WHERE deleted_at IS NULL
			DO UPDATE SET summary=EXCLUDED.summary,updated_at=EXCLUDED.updated_at,expires_at=NULL
			WHERE runtime_session_summaries.updated_at<=EXCLUDED.updated_at`, key.AppName, key.UserID, key.SessionID, filter, payload, visibilityAt); err != nil {
			var postgresError *pgconn.PgError
			if errors.As(err, &postgresError) {
				return fmt.Errorf("storage: import PostgreSQL session summary failed (%s)", postgresError.Code)
			}
			return errors.New("storage: import PostgreSQL session summary failed")
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.New("storage: commit PostgreSQL summary import failed")
	}
	return nil
}

func importRedisSummaries(ctx context.Context, rawURL, prefix string, key session.Key, summaries map[string]*session.Summary) error {
	opts, err := redisclient.ParseURL(rawURL)
	if err != nil {
		return errors.New("storage: Redis summary target configuration is invalid")
	}
	client := redisclient.NewClient(opts)
	defer client.Close()
	redisKey := fmt.Sprintf("%s:hashidx:sesssum:%s:{%s}:%s", prefix, key.AppName, key.UserID, key.SessionID)
	for attempts := 0; attempts < 4; attempts++ {
		err = client.Watch(ctx, func(tx *redisclient.Tx) error {
			current := make(map[string]*session.Summary)
			payload, getErr := tx.Get(ctx, redisKey).Bytes()
			if getErr != nil && !errors.Is(getErr, redisclient.Nil) {
				return getErr
			}
			if len(payload) > 0 && json.Unmarshal(payload, &current) != nil {
				return errors.New("invalid existing summary")
			}
			for filter, incoming := range summaries {
				existing := current[filter]
				if existing == nil || !existing.UpdatedAt.After(incoming.UpdatedAt) {
					current[filter] = incoming.Clone()
				}
			}
			encoded, encodeErr := json.Marshal(current)
			if encodeErr != nil {
				return encodeErr
			}
			_, pipelineErr := tx.TxPipelined(ctx, func(pipe redisclient.Pipeliner) error {
				pipe.SetArgs(ctx, redisKey, encoded, redisclient.SetArgs{KeepTTL: true})
				return nil
			})
			return pipelineErr
		}, redisKey)
		if err == nil {
			return nil
		}
		if !errors.Is(err, redisclient.TxFailedErr) {
			return errors.New("storage: import Redis session summary failed")
		}
	}
	return errors.New("storage: Redis session summary changed during import")
}
