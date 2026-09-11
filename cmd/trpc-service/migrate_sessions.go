package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	trpcGoredis "github.com/redis/go-redis/v9"

	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/redis"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// runMigrateSessions copies every session from the legacy Redis backend into
// the MySQL control plane, one transaction per session. Only sessions whose
// channel_type resolves to one of the platform's first-class channels
// (webchat, wecom, wechat_kf) are migrated; sessions on unknown channels,
// whose binding disappeared, or whose channel_type is not in the sessions
// ENUM are skipped and printed so the operator can decide.
func runMigrateSessions(cfg *config.Config, dryRun bool) int {
	if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
		fmt.Fprintln(os.Stderr, "migrate-sessions: control_plane.mode must be mysql")
		return 1
	}
	if cfg.Storage.Session.Backend != config.BackendRedis {
		fmt.Fprintln(os.Stderr, "migrate-sessions: storage.session.backend must be redis")
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := tasmysql.Open(ctx, cfg.ControlPlane.MySQLDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate-sessions: open mysql: %v\n", err)
		return 1
	}
	defer db.Close()
	cdp := controlplane.NewDB(db)

	opts, err := trpcGoredis.ParseURL(cfg.Storage.Session.RedisURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate-sessions: redis url: %v\n", err)
		return 1
	}
	rdb := trpcGoredis.NewClient(opts)
	defer rdb.Close()

	prefix := cfg.Storage.Session.KeyPrefix
	if prefix != "" {
		prefix += ":"
	}
	pattern := prefix + "event:*"

	var total, skipped int
	cursor := uint64(0)
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, 5000).Result()
		if err != nil {
			fmt.Fprintf(os.Stderr, "migrate-sessions: scan: %v\n", err)
			return 1
		}
		for _, key := range keys {
			sk, err := parseRedisEventKey(prefix, key)
			if err != nil {
				slog.Warn("migrate-sessions: skipping unparseable key", "key", key, "err", err)
				skipped++
				continue
			}
			if err := migrateOneSession(ctx, cdp, &sk, cfg, dryRun); err != nil {
				slog.Warn("migrate-sessions: skipping session after error",
					"app", sk.AppName, "user", sk.UserID, "session", sk.SessionID, "err", err)
				skipped++
				continue
			}
			total++
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("migrate-sessions: %d migrated, %d skipped, %d total redis keys\n", total, skipped, total+skipped)
	if dryRun {
		fmt.Println("dry-run: no writes were made")
	}
	return 0
}

// migrateOneSession reads one legacy session via the framework API and writes
// it into MySQL.
func migrateOneSession(ctx context.Context, cdp *controlplane.DB, sk *session.Key, cfg *config.Config, dryRun bool) error {
	// Connect a framework session service to read the Redis data. A
	// fresh client per session is wasteful but correct; the real
	// optimization is that we do not build one until we have a key.
	svc, err := redis.NewService(
		redis.WithRedisClientURL(cfg.Storage.Session.RedisURL),
		redis.WithKeyPrefix(cfg.Storage.Session.KeyPrefix))
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}
	defer svc.Close()

	sess, err := svc.GetSession(ctx, *sk)
	if err != nil {
		return fmt.Errorf("get session: %w", err)
	}
	if sess == nil {
		return nil // existed between scan and get
	}

	// Parse the framework session id → tenant / channel / actor.
	parts := strings.SplitN(sk.SessionID, ":", 3)
	if len(parts) < 3 {
		return fmt.Errorf("unparseable session id %q", sk.SessionID)
	}
	tenantID, channelType, actorKey := parts[0], parts[1], parts[2]

	if !isKnownChannel(channelType) {
		return fmt.Errorf("channel_type %q is not in the sessions ENUM", channelType)
	}

	scope, err := cdp.Scope(tenantID)
	if err != nil {
		return err
	}

	// Resolve binding → app → current revision.
	var (
		bindingID             int64
		appID                 int64
		revisionID            int64
		modelProfileID        int64
		backendProfileID      int64
		modelProfileVersion   uint32
		backendProfileVersion uint32
	)
	row, err := scope.QueryRow(ctx, `
		SELECT cb.binding_id, aa.app_id, ar.revision_id, mp.profile_id, mp.version, bp.profile_id, bp.version
		FROM channel_bindings cb
		JOIN agent_apps aa ON aa.tenant_id = cb.tenant_id AND aa.app_id = cb.app_id
		JOIN agent_revisions ar ON ar.revision_id = aa.current_revision_id AND ar.app_id = aa.app_id
		JOIN model_profiles mp ON mp.profile_id = ar.model_profile_id
		JOIN backend_profiles bp ON bp.profile_id = ar.backend_profile_id
		WHERE cb.tenant_id = ? AND cb.channel_type = ? AND cb.status = 'active'
		ORDER BY cb.binding_id LIMIT 1`, tenantID, channelType)
	if err != nil {
		return err
	}
	switch err := row.Scan(&bindingID, &appID, &revisionID, &modelProfileID, &modelProfileVersion, &backendProfileID, &backendProfileVersion); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("no active %s binding for tenant %q", channelType, tenantID)
	case err != nil:
		return fmt.Errorf("resolve target: %w", err)
	}

	// Check idempotency: a session with the same identity exists already.
	var existing int64
	idemRow, err := scope.QueryRow(ctx,
		"SELECT session_pk FROM sessions WHERE tenant_id=? AND app_id=? AND binding_id=? AND actor_key=? AND generation=1",
		tenantID, appID, bindingID, actorKey)
	if err != nil {
		return err
	}
	switch err := idemRow.Scan(&existing); {
	case errors.Is(err, sql.ErrNoRows):
		// OK, new session; proceed.
	case err != nil:
		return err
	default:
		return fmt.Errorf("session already exists (session_pk=%d)", existing)
	}

	events := sess.Events
	inSeq := uint32(len(events))
	headSeq := inSeq + 1
	gen := uint32(1)

	stateJSON, _ := json.Marshal(sess.State)

	summary := ""
	if sess.Summaries != nil {
		if s, ok := sess.Summaries["all"]; ok && s != nil {
			summary = s.Summary
		}
	}

	if dryRun {
		fmt.Printf("  [dry-run] would migrate session: tenant=%q actor=%q channel=%s events=%d\n",
			tenantID, actorKey, channelType, inSeq)
		return nil
	}

	// Single-transaction write.
	return scope.WithTx(ctx, func(tx *controlplane.TxScope) error {
		res, err := tx.Exec(ctx, `
			INSERT INTO sessions
				(tenant_id, app_id, channel_type, binding_id, actor_key, generation,
				 revision_id, model_profile_version, backend_profile_version,
				 in_seq, head_seq, state, session_version, summary)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`,
			tenantID, appID, channelType, bindingID, actorKey, gen,
			revisionID, modelProfileVersion, backendProfileVersion,
			inSeq, headSeq, nullableJSON(stateJSON), summary)
		if err != nil {
			return fmt.Errorf("insert session: %w", err)
		}
		sessionPK, err := res.LastInsertId()
		if err != nil {
			return err
		}
		for i, ev := range events {
			payload, err := json.Marshal(ev)
			if err != nil {
				return fmt.Errorf("marshal event %d: %w", i, err)
			}
			execID := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
			if _, err := tx.Exec(ctx, `
				INSERT INTO session_events
					(tenant_id, session_pk, seq, event_id, execution_id, author, payload)
				VALUES (?, ?, ?, ?, ?, ?, ?)`,
				tenantID, sessionPK, i+1, ev.ID, execID, ev.Author, nullableJSON(payload)); err != nil {
				return fmt.Errorf("insert event %d: %w", i, err)
			}
		}
		return nil
	})
}

// parseRedisEventKey extracts the framework session key from a Redis key
// like "tas:event:{trpc-agent-service}:ext-user:acme:chat:ext-user".
func parseRedisEventKey(prefix, key string) (session.Key, error) {
	rest := strings.TrimPrefix(key, prefix+"event:")
	if rest == key {
		return session.Key{}, fmt.Errorf("key %q does not match event pattern", key)
	}
	// rest = {appname}:userid:sessionid — the session id contains colons.
	closeBrace := strings.IndexByte(rest, '}')
	if closeBrace < 0 {
		return session.Key{}, fmt.Errorf("missing hash tag close brace in %q", rest)
	}
	appName := rest[1:closeBrace] // strip { and }
	tail := rest[closeBrace+1:]   // ":user:tenant:channel:actor..."
	if tail == "" || tail[0] != ':' {
		return session.Key{}, fmt.Errorf("expected colon after hash tag in %q", rest)
	}
	tail = tail[1:] // drop ':'
	colon := strings.IndexByte(tail, ':')
	if colon < 0 {
		return session.Key{}, fmt.Errorf("no colon separating user and session id in %q", rest)
	}
	userID := tail[:colon]
	sessionID := tail[colon+1:]
	if userID == "" || sessionID == "" {
		return session.Key{}, fmt.Errorf("empty user or session id in %q", rest)
	}
	return session.Key{AppName: appName, UserID: userID, SessionID: sessionID}, nil
}

// isKnownChannel returns true for channel types that are in the sessions
// ENUM column.
func isKnownChannel(ct string) bool {
	switch ct {
	case "webchat", "wecom", "wechat_kf":
		return true
	}
	return false
}

// nullableJSON returns a json.RawMessage if b is non-empty, else nil.
func nullableJSON(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}
