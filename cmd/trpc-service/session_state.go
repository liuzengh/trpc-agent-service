package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// runSessionState prints one session's committed state from both sides: the
// MySQL snapshot (the authority) and the Redis projection (allowed to lag,
// never to lead). It exists to make that ordering checkable by an operator
// instead of a matter of trust — the projection is a cache, and this command
// is how "the cache is behind by at most one commit" gets eyes on it.
func runSessionState(cfg *config.Config, tenantID string, sessionPK int64) int {
	if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
		fmt.Fprintln(os.Stderr, "session-state: control_plane.mode must be mysql")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := tasmysql.Open(ctx, cfg.ControlPlane.MySQLDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "session-state: open: %v\n", err)
		return 1
	}
	defer db.Close()
	cdp := controlplane.NewDB(db)
	scope, err := cdp.Scope(tenantID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "session-state: %v\n", err)
		return 1
	}

	var (
		stateJSON []byte
		summary   sql.NullString
		version   uint64
	)
	row, err := scope.QueryRow(ctx, `
		SELECT state, summary, session_version FROM sessions
		WHERE tenant_id = ? AND session_pk = ?`, tenantID, sessionPK)
	if err != nil {
		fmt.Fprintf(os.Stderr, "session-state: %v\n", err)
		return 1
	}
	switch err := row.Scan(&stateJSON, &summary, &version); {
	case errors.Is(err, sql.ErrNoRows):
		fmt.Fprintf(os.Stderr, "session-state: no session %d in tenant %q\n", sessionPK, tenantID)
		return 1
	case err != nil:
		fmt.Fprintf(os.Stderr, "session-state: read session: %v\n", err)
		return 1
	}
	stateKeys := 0
	if len(stateJSON) > 0 {
		var state map[string]any
		if err := json.Unmarshal(stateJSON, &state); err != nil {
			fmt.Fprintf(os.Stderr, "session-state: decode state: %v\n", err)
			return 1
		}
		stateKeys = len(state)
	}

	projected := map[string]any{"present": false}
	if cfg.Storage.Session.Backend == config.BackendRedis {
		coord, err := coordination.NewRedis(cfg.Storage.Session.RedisURL, cfg.Storage.Session.KeyPrefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "session-state: projection store: %v\n", err)
			return 1
		}
		defer coord.Close()
		proj := coordination.NewSessionProjection(coord, 0)
		if p, ok, err := proj.Read(ctx, tenantID, sessionPK); err != nil {
			fmt.Fprintf(os.Stderr, "session-state: read projection: %v\n", err)
			return 1
		} else if ok {
			projected = map[string]any{
				"present": true, "version": p.Version,
				"summary": p.Summary, "state_keys": len(p.State),
			}
		}
	}

	out := map[string]any{
		"tenant":     tenantID,
		"session_pk": sessionPK,
		"mysql": map[string]any{
			"version": version, "summary": summary.String, "state_keys": stateKeys,
		},
		"projection": projected,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintf(os.Stderr, "session-state: encode: %v\n", err)
		return 1
	}
	return 0
}
