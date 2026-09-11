// Command migrate copies tenant session/event state and user memory between
// storage backends using the framework Service API only (no backend-private
// schema), so any backend pair supported by the platform can be migrated:
// session inmemory/mysql/redis, memory inmemory/mysql/redis.
//
// Scope of one run is a single tenant (AppName) and an explicit user list
// (-users) or every user of a MySQL source (-auto). The target side is
// overwritten per session/memory key (delete-then-create), which makes a
// repeated run idempotent.
//
// Fidelity boundaries (documented in docs/跨后端数据迁移操作手册.md):
//   - Session state and the events returned by GetSession are copied. The
//     framework (v1.11.2) exposes no event-pagination API, so each backend
//     returns its recent-event window; very old events of a MySQL source can
//     be copied with the SQL template in the manual.
//   - Session summaries/tracks are not migrated (no Service write API); the
//     target rebuilds them as conversations continue.
//
// Usage:
//
//	go run ./cmd/migrate \
//	  -src-session  redis://localhost:6379/0 \
//	  -dst-session  mysql://user:pass@tcp(localhost:3306)/agent?parseTime=true \
//	  -src-memory   redis://localhost:6379/0 \
//	  -dst-memory   redis://localhost:6379/1 \
//	  -tenant t1 -users u1,u2
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type options struct {
	srcSession string
	dstSession string
	srcMemory  string
	dstMemory  string
	tenant     string
	users      string
	auto       bool
}

func main() {
	var o options
	flag.StringVar(&o.srcSession, "src-session", "", "source session spec: inmemory | redis://URL | mysql://DSN")
	flag.StringVar(&o.dstSession, "dst-session", "", "target session spec (same syntax)")
	flag.StringVar(&o.srcMemory, "src-memory", "", "source memory spec: inmemory | redis://URL (optional)")
	flag.StringVar(&o.dstMemory, "dst-memory", "", "target memory spec (optional)")
	flag.StringVar(&o.tenant, "tenant", "", "tenant id (AppName) to migrate")
	flag.StringVar(&o.users, "users", "", "comma-separated user ids to migrate")
	flag.BoolVar(&o.auto, "auto", false, "enumerate all users of the tenant from a MySQL source")
	flag.Parse()

	if err := run(context.Background(), o); err != nil {
		log.Fatalf("migrate: %v", err)
	}
}

// spec is a parsed backend location.
type spec struct {
	backend storage.Backend
	dsn     string // raw DSN/URL, empty for inmemory
}

// parseSpec parses "inmemory", "redis://..." or "mysql://..."/"mysql:...".
func parseSpec(s string) (spec, error) {
	switch {
	case s == "":
		return spec{}, fmt.Errorf("empty backend spec")
	case s == "inmemory":
		return spec{backend: storage.BackendInMemory}, nil
	case strings.HasPrefix(s, "redis://"):
		return spec{backend: storage.BackendRedis, dsn: s}, nil
	case strings.HasPrefix(s, "mysql://"):
		return spec{backend: storage.BackendMySQL, dsn: strings.TrimPrefix(s, "mysql://")}, nil
	case strings.HasPrefix(s, "mysql:"):
		return spec{backend: storage.BackendMySQL, dsn: strings.TrimPrefix(s, "mysql:")}, nil
	default:
		return spec{}, fmt.Errorf("unknown backend spec %q (want inmemory | redis://URL | mysql://DSN)", s)
	}
}

func sessionService(sp spec) (session.Service, error) {
	w, err := storage.NewSessions(storage.SessionConfig{
		Backend:  sp.backend,
		MySQLDSN: sp.dsn,
		RedisURL: sp.dsn,
	})
	if err != nil {
		return nil, err
	}
	return w.Service(), nil
}

func memoryService(sp spec) (memory.Service, error) {
	// MySQL memory is a supported platform backend (storage.backendTable), so a
	// tenant can be pinned to it at runtime — the migration tool has to be able
	// to read from and write to it too, otherwise a tenant that was switched to
	// MySQL memory becomes unmigratable.
	w, err := storage.NewMemories(storage.MemoryConfig{
		Backend:  sp.backend,
		RedisURL: sp.dsn,
		MySQLDSN: sp.dsn,
	})
	if err != nil {
		return nil, err
	}
	return w.Service(), nil
}

// listUsers returns the users to migrate: explicit list, or — with -auto and
// a MySQL session source — every distinct user of the tenant.
func listUsers(ctx context.Context, o options, src spec) ([]string, error) {
	if o.users != "" {
		var out []string
		for _, u := range strings.Split(o.users, ",") {
			if u = strings.TrimSpace(u); u != "" {
				out = append(out, u)
			}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no users parsed from %q", o.users)
		}
		return out, nil
	}
	if !o.auto {
		return nil, fmt.Errorf("either -users or -auto is required")
	}
	if src.backend != storage.BackendMySQL {
		return nil, fmt.Errorf("-auto enumerates users via the MySQL session schema; mysql source required")
	}
	// OpenMySQL (not a bare sql.Open) so the migration reads timestamps with the
	// same session time zone the platform writes them in.
	db, err := storage.OpenMySQL(src.dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT user_id FROM session_states WHERE app_name = ? AND deleted_at IS NULL`, o.tenant)
	if err != nil {
		return nil, fmt.Errorf("enumerate users: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func run(ctx context.Context, o options) error {
	if o.tenant == "" {
		return fmt.Errorf("-tenant is required")
	}
	src, err := parseSpec(o.srcSession)
	if err != nil {
		return fmt.Errorf("-src-session: %w", err)
	}
	dst, err := parseSpec(o.dstSession)
	if err != nil {
		return fmt.Errorf("-dst-session: %w", err)
	}
	srcSvc, err := sessionService(src)
	if err != nil {
		return fmt.Errorf("source session backend: %w", err)
	}
	dstSvc, err := sessionService(dst)
	if err != nil {
		return fmt.Errorf("target session backend: %w", err)
	}

	var srcMem, dstMem memory.Service
	if o.srcMemory != "" {
		sp, err := parseSpec(o.srcMemory)
		if err != nil {
			return fmt.Errorf("-src-memory: %w", err)
		}
		if srcMem, err = memoryService(sp); err != nil {
			return fmt.Errorf("source memory backend: %w", err)
		}
	}
	if o.dstMemory != "" {
		sp, err := parseSpec(o.dstMemory)
		if err != nil {
			return fmt.Errorf("-dst-memory: %w", err)
		}
		if dstMem, err = memoryService(sp); err != nil {
			return fmt.Errorf("target memory backend: %w", err)
		}
	}
	if (o.srcMemory == "") != (o.dstMemory == "") {
		slog.Warn("only one memory spec set; memory migration skipped", "src_memory", o.srcMemory != "", "dst_memory", o.dstMemory != "")
	}

	users, err := listUsers(ctx, o, src)
	if err != nil {
		return err
	}

	var nSessions, nEvents, nMemories int
	for _, userID := range users {
		ns, ne, nm, err := copyUser(ctx, srcSvc, dstSvc, srcMem, dstMem, o.tenant, userID)
		if err != nil {
			return err
		}
		nSessions += ns
		nEvents += ne
		nMemories += nm
	}
	slog.Info("migration complete",
		"tenant", o.tenant, "users", len(users),
		"sessions", nSessions, "events", nEvents, "memories", nMemories)
	return nil
}

// maxRecentEvents asks backends for a very large recent-event window when
// copying a session. Each backend still caps the window at its own limit (the
// framework exposes no full-history paging API, see docs/跨后端数据迁移操作手册.md).
const maxRecentEvents = 1 << 30

// copyUser migrates every session and memory entry of one tenant user from
// src to dst. Sessions are replaced delete-then-create (idempotent reruns);
// memories are cleared then re-added on the target.
func copyUser(ctx context.Context, srcSvc, dstSvc session.Service, srcMem, dstMem memory.Service, tenant, userID string) (nSessions, nEvents, nMemories int, err error) {
	sessUK := session.UserKey{AppName: tenant, UserID: userID}
	memUK := memory.UserKey{AppName: tenant, UserID: userID}

	list, err := srcSvc.ListSessions(ctx, sessUK)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list sessions for %q: %w", userID, err)
	}
	for _, s := range list {
		if s == nil || s.ID == "" {
			continue
		}
		sKey := session.Key{AppName: tenant, UserID: userID, SessionID: s.ID}
		full, err := srcSvc.GetSession(ctx, sKey, session.WithEventNum(maxRecentEvents))
		if err != nil {
			return 0, 0, 0, fmt.Errorf("get session %q: %w", s.ID, err)
		}
		if full == nil {
			continue
		}
		// Delete-then-create keeps reruns idempotent and replaces any
		// target-side stale state for the same key.
		if err := dstSvc.DeleteSession(ctx, sKey); err != nil && !isNotFound(err) {
			return 0, 0, 0, fmt.Errorf("delete target session %q: %w", s.ID, err)
		}
		created, err := dstSvc.CreateSession(ctx, sKey, full.State)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("create target session %q: %w", s.ID, err)
		}
		for i := range full.Events {
			if err := dstSvc.AppendEvent(ctx, created, &full.Events[i]); err != nil {
				return 0, 0, 0, fmt.Errorf("append event to %q: %w", s.ID, err)
			}
			nEvents++
		}
		nSessions++
		slog.Info("session migrated", "tenant", tenant, "user", userID, "session", s.ID, "events", len(full.Events))
	}

	if srcMem != nil && dstMem != nil {
		if err := dstMem.ClearMemories(ctx, memUK); err != nil {
			return 0, 0, 0, fmt.Errorf("clear target memories for %q: %w", userID, err)
		}
		entries, err := srcMem.ReadMemories(ctx, memUK, 100000)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("read memories for %q: %w", userID, err)
		}
		for _, e := range entries {
			if e == nil || e.Memory == nil {
				continue
			}
			err := dstMem.AddMemory(ctx, memUK, e.Memory.Memory, e.Memory.Topics, memory.WithMetadata(&memory.Metadata{
				Kind:         e.Memory.Kind,
				EventTime:    e.Memory.EventTime,
				Participants: e.Memory.Participants,
				Location:     e.Memory.Location,
			}))
			if err != nil {
				return 0, 0, 0, fmt.Errorf("add memory for %q: %w", userID, err)
			}
			nMemories++
		}
		slog.Info("memory migrated", "tenant", tenant, "user", userID, "entries", len(entries))
	}
	return nSessions, nEvents, nMemories, nil
}

// isNotFound detects session-not-found errors across backends. Backends return
// (nil, nil) from GetSession for a missing key; deletes of missing sessions
// may surface an error that is safe to ignore for the delete-then-create flow.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist")
}
