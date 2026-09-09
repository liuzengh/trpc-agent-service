package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Migrator executes tenant backend migrations, one row of
// storage_migration at a time:
//
//	dual_write → backfilling → observing → done
//
// Dual write is live from row creation (the assembler fans writes out for
// tenants with an active migration). The Migrator drives the rest: copy the
// tenant's sessions from the old backend to the new (batched, resumable by
// cursor), run the consistency check (the two journals must agree event ID by
// event ID — a count match is not agreement), flip tenant.storage_config
// (read switch), keep dual write through the observation window, then finish.
// Any failure parks the row in "failed" with the reason — reads never switch
// on a failed check.
type Migrator struct {
	pool     *pgxpool.Pool
	rdb      *redis.Client // enumerate + publish invalidation on read switch
	backends map[string]session.Service

	// ObserveWindow is how long dual write continues after the read switch.
	// Interval is the tick cadence; BatchSize caps sessions copied per tick.
	ObserveWindow time.Duration
	Interval      time.Duration
	BatchSize     int
}

// migrationProgress is the progress jsonb payload.
type migrationProgress struct {
	SessionsTotal int `json:"sessions_total"`
	SessionsDone  int `json:"sessions_done"`
	// CursorApp/Cursor are the two halves of the pagination position: the
	// (app_id, session_key) of the last session copied. session_key alone is
	// not unique — uk_session_app_key is (app_id, session_key) — so a
	// single-dimension cursor steps over the second of two apps that share a
	// session_key and never migrates it. A progress row carrying only Cursor
	// restarts enumeration from the beginning — safe, because copySession
	// reconciles by event ID and appends only what the target journal is
	// still missing.
	CursorApp    string    `json:"cursor_app,omitempty"`
	Cursor       string    `json:"cursor,omitempty"`
	Mismatches   []string  `json:"mismatches,omitempty"` // consistency check failures
	ObserveUntil time.Time `json:"observe_until,omitempty"`
}

// NewMigrator creates the executor. backends maps "redis"/"postgres" to the
// process's session services (the same instances the assembler routes to).
func NewMigrator(pool *pgxpool.Pool, rdb *redis.Client, backends map[string]session.Service, observeWindow time.Duration) *Migrator {
	if observeWindow <= 0 {
		observeWindow = 24 * time.Hour
	}
	return &Migrator{
		pool: pool, rdb: rdb, backends: backends,
		ObserveWindow: observeWindow, Interval: 5 * time.Second, BatchSize: 50,
	}
}

// Run ticks until ctx is canceled.
func (m *Migrator) Run(ctx context.Context) {
	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.Tick(ctx); err != nil {
				plog.Errorf("migration tick: %v", err)
			}
		}
	}
}

// Tick advances every active migration one step, inside one transaction that
// claims the rows with FOR UPDATE SKIP LOCKED: replicas of the
// worker role run the same Migrator, so each tick must claim a disjoint set
// instead of two replicas double-advancing one migration (double backfill
// batches, read switches racing the consistency check). The phase/progress
// writes ride the same transaction, so a crashed tick rolls back cleanly and
// the next claimant re-advances from the last committed state. Exported for
// tests.
func (m *Migrator) Tick(ctx context.Context) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration tick: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`SELECT id, tenant_id, resource, from_backend, to_backend, phase, progress
		 FROM storage_migration
		 WHERE phase NOT IN ('done', 'failed', 'aborted') ORDER BY created_at
		 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return fmt.Errorf("query migrations: %w", err)
	}
	var migs []migrationRow
	for rows.Next() {
		var r migrationRow
		var progress []byte
		if err := rows.Scan(&r.ID, &r.TenantID, &r.Resource, &r.From, &r.To, &r.Phase, &progress); err != nil {
			rows.Close()
			return fmt.Errorf("scan migration: %w", err)
		}
		if len(progress) > 0 {
			_ = json.Unmarshal(progress, &r.Progress)
		}
		migs = append(migs, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate migrations: %w", err)
	}
	rows.Close()

	for _, mig := range migs {
		if err := m.advance(ctx, tx, mig); err != nil {
			plog.Errorf("migration %s (%s) failed: %v", mig.ID, mig.Phase, err)
			m.fail(ctx, tx, mig, err)
		}
	}
	return tx.Commit(ctx)
}

// migrationRow is one storage_migration row.
type migrationRow struct {
	ID, TenantID, Resource, From, To, Phase string
	Progress                                migrationProgress
}

func (m *Migrator) advance(ctx context.Context, tx pgx.Tx, mig migrationRow) error {
	switch mig.Phase {
	case tenant.PhaseDualWrite:
		// Dual write has been live since the row was created; start copying.
		return m.setPhase(ctx, tx, mig.ID, tenant.PhaseBackfilling, mig.Progress)
	case tenant.PhaseBackfilling:
		return m.backfill(ctx, tx, mig)
	case tenant.PhaseObserving:
		if time.Now().After(mig.Progress.ObserveUntil) {
			plog.Infof("migration %s: observation window passed, done", mig.ID)
			return m.setPhase(ctx, tx, mig.ID, tenant.PhaseDone, mig.Progress)
		}
	}
	return nil
}

// backfill copies one batch of sessions from the source backend to the
// target; when all are copied, the consistency check gates the read switch.
// Session data moves on the pool (independent transactions per session);
// only the migration row's own phase/progress rides the claim tx.
func (m *Migrator) backfill(ctx context.Context, tx pgx.Tx, mig migrationRow) error {
	src, dst := m.backends[mig.From], m.backends[mig.To]
	if src == nil || dst == nil {
		return fmt.Errorf("backend pair %s→%s not both available", mig.From, mig.To)
	}

	sessions, err := m.enumerate(ctx, mig, cursorOf(mig.Progress))
	if err != nil {
		return err
	}
	for _, key := range sessions {
		if err := m.copySession(ctx, mig, src, dst, key); err != nil {
			return fmt.Errorf("copy session %s: %w", key.SessionID, err)
		}
		mig.Progress.CursorApp = key.AppName
		mig.Progress.Cursor = key.SessionID
		mig.Progress.SessionsDone++
	}
	if len(sessions) > 0 || mig.Progress.SessionsTotal == 0 {
		// Refresh the total estimate once per batch.
		total, err := m.countTenantSessions(ctx, mig)
		if err == nil {
			mig.Progress.SessionsTotal = total
		}
	}

	if int64(len(sessions)) == int64(m.BatchSize) {
		// Probably more to come; persist progress and continue next tick.
		return m.setPhase(ctx, tx, mig.ID, tenant.PhaseBackfilling, mig.Progress)
	}

	// Everything copied: the consistency check gates the read switch; both
	// journals must agree event by event.
	mismatches, err := m.checkConsistency(ctx, mig)
	if err != nil {
		return err
	}
	mig.Progress.Mismatches = mismatches
	if len(mismatches) > 0 {
		return fmt.Errorf("consistency check failed for %d sessions: %v", len(mismatches), mismatches)
	}
	return m.readSwitch(ctx, tx, mig)
}

// readSwitch points the tenant's storage_config at the new backend, notifies
// workers through the invalidation channel, and starts the observation window.
// Both row updates ride the claim tx; the broadcast is best-effort and may
// fire a beat before the commit — a worker that refreshes early just sees the
// old config and waits for the TTL.
func (m *Migrator) readSwitch(ctx context.Context, tx pgx.Tx, mig migrationRow) error {
	backendJSON, err := json.Marshal(map[string]string{"type": mig.To})
	if err != nil {
		return fmt.Errorf("encode backend config: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE tenant SET storage_config = jsonb_set(COALESCE(storage_config, '{}'), '{session}', $2::jsonb),
		 updated_at = now() WHERE id = $1`,
		mig.TenantID, backendJSON); err != nil {
		return fmt.Errorf("switch storage_config: %w", err)
	}
	mig.Progress.ObserveUntil = time.Now().Add(m.ObserveWindow)
	if err := m.setPhase(ctx, tx, mig.ID, tenant.PhaseObserving, mig.Progress); err != nil {
		return err
	}
	if err := tenant.PublishInvalidation(ctx, m.rdb); err != nil {
		plog.Warnf("migration %s: invalidation broadcast failed (TTL fallback): %v", mig.ID, err)
	}
	plog.Infof("migration %s: reads switched %s→%s for tenant %s, observing until %s",
		mig.ID, mig.From, mig.To, mig.TenantID, mig.Progress.ObserveUntil.Format(time.RFC3339))
	return nil
}

// enumCursor is the pagination position: the (app_id, session_key) of the last
// session copied. Both halves are needed because session_key is only unique
// per app, so ordering by it alone leaves ties whose second member a
// "session_key > cursor" page steps over permanently.
type enumCursor struct{ appID, sessionKey string }

func cursorOf(p migrationProgress) enumCursor {
	return enumCursor{appID: p.CursorApp, sessionKey: p.Cursor}
}

// after reports whether k sorts strictly after the cursor in the
// (app_id, session_key) order both enumerators page by.
func (c enumCursor) after(k session.Key) bool {
	if k.AppName != c.appID {
		return k.AppName > c.appID
	}
	return k.SessionID > c.sessionKey
}

// keyOrder is the same order as a comparator, for sorting the Redis scan
// (SCAN returns keys in no particular order, so pagination needs a total one).
func keyOrder(a, b session.Key) int {
	if c := strings.Compare(a.AppName, b.AppName); c != 0 {
		return c
	}
	return strings.Compare(a.SessionID, b.SessionID)
}

// enumerate lists one batch of the tenant's sessions on the source backend,
// after the cursor.
func (m *Migrator) enumerate(ctx context.Context, mig migrationRow, cursor enumCursor) ([]session.Key, error) {
	if mig.From == "postgres" {
		return m.enumeratePG(ctx, mig.TenantID, cursor)
	}
	return m.enumerateRedis(ctx, mig.TenantID, cursor)
}

// zeroAppID sorts below every real app_id, so it is the first page's cursor.
// Binding "" instead would not merely sort low: app_id is a uuid and Postgres
// rejects the empty string outright.
const zeroAppID = "00000000-0000-0000-0000-000000000000"

// enumeratePG pages the session table by (app_id, session_key); the row
// comparison and the ORDER BY must agree, or the page boundary skips or
// repeats rows.
func (m *Migrator) enumeratePG(ctx context.Context, tenantID string, cursor enumCursor) ([]session.Key, error) {
	cursorApp := cursor.appID
	if cursorApp == "" {
		cursorApp = zeroAppID
	}
	rows, err := m.pool.Query(ctx,
		`SELECT app_id, session_key, user_id FROM session
		 WHERE tenant_id = $1 AND (app_id, session_key) > ($2, $3)
		 ORDER BY app_id, session_key LIMIT $4`,
		tenantID, cursorApp, cursor.sessionKey, m.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("enumerate pg sessions: %w", err)
	}
	defer rows.Close()
	var keys []session.Key
	for rows.Next() {
		var k session.Key
		if err := rows.Scan(&k.AppName, &k.SessionID, &k.UserID); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// enumerateRedis scans the framework's hashidx session keys
// (hashidx:meta:{app}:{user}:{sess}, the default storage layout of
// session/redis v1.11) for the tenant's apps. The cursor is the last copied
// session; enumeration re-scans and skips everything up to it.
func (m *Migrator) enumerateRedis(ctx context.Context, tenantID string, cursor enumCursor) ([]session.Key, error) {
	appIDs, err := m.tenantApps(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	var all []session.Key
	for _, appID := range appIDs {
		prefix := fmt.Sprintf("hashidx:meta:%s:", appID)
		iter := m.rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()
		for iter.Next(ctx) {
			// Strip "hashidx:meta:{app}:" → "{user}:{sess}".
			rest := iter.Val()[len(prefix):]
			end := strings.IndexByte(rest, '}')
			if !strings.HasPrefix(rest, "{") || end < 0 || len(rest) <= end+2 {
				continue // unknown key shape; never ours
			}
			all = append(all, session.Key{
				AppName: appID, UserID: rest[1:end], SessionID: rest[end+2:],
			})
		}
		if err := iter.Err(); err != nil {
			return nil, fmt.Errorf("scan redis sessions: %w", err)
		}
	}
	// Deterministic order + cursor skip, in the same (app_id, session_key)
	// order enumeratePG pages by: the two backends must agree, or a migration
	// resumes at a different place than it stopped.
	slices.SortFunc(all, keyOrder)
	var out []session.Key
	for _, k := range all {
		if cursor.after(k) {
			out = append(out, k)
		}
	}
	if len(out) > m.BatchSize {
		out = out[:m.BatchSize]
	}
	return out, nil
}

// copySession copies one session, reconciling by event ID so it is idempotent
// and safe against dual write: writes are already fanned out to the target
// while the backfill runs, so the target may hold the source's live tail
// before this copy delivers the prefix — position-based skipping cannot
// describe that state, but "an event is either on the target or it is not"
// does, on both backends.
func (m *Migrator) copySession(ctx context.Context, mig migrationRow, src, dst session.Service, key session.Key) error {
	srcSess, err := src.GetSession(ctx, key)
	if err != nil {
		return fmt.Errorf("read source: %w", err)
	}
	if srcSess == nil {
		return nil // vanished between enumerate and copy
	}
	// The journal must not be read through GetSession on either backend. A
	// postgres source truncates at the summary cursor and the archive
	// boundary; a redis source caps at sessionEventLimit (1000 by default)
	// and then anchors the head to the first user message — copying through
	// either view silently leaves history behind while the copy still looks
	// complete to anything that counts it.
	events := srcSess.Events
	switch mig.From {
	case "postgres":
		if pg, ok := src.(*PGSessionService); ok {
			if events, err = pg.FullJournal(ctx, key); err != nil {
				return fmt.Errorf("read source journal: %w", err)
			}
		}
	case "redis":
		if events, err = m.fullJournalRedis(ctx, key); err != nil {
			return err
		}
	}

	if mig.To == "postgres" {
		return m.writeSessionToPG(ctx, mig.TenantID, srcSess, events)
	}
	// Redis target: create (idempotent), then append exactly the events the
	// target does not already hold, identified by event ID. A positional skip
	// ("append source events from len(target events) onward") is wrong: with
	// dual write live the target's newest events are the source's NEWEST
	// events, so the skip would discard the prefix. Redis storage is ID-keyed
	// (hash field = event ID, zset member = event ID), so an event already
	// present cannot be duplicated anyway, and the zset is scored by
	// timestamp: append order does not decide read order. AppendEvent mutates
	// the carrier session's event list, so the destination's own session
	// object is the carrier.
	dstSess, err := dst.CreateSession(ctx, key, srcSess.State)
	if err != nil {
		return fmt.Errorf("create target session: %w", err)
	}
	have := make(map[string]bool)
	targetIDs, err := m.journalIDs(ctx, mig.To, key)
	if err != nil {
		return err
	}
	for _, id := range targetIDs {
		have[id] = true
	}
	for i := range events {
		if have[events[i].ID] {
			continue
		}
		evt := events[i]
		if err := dst.AppendEvent(ctx, dstSess, &evt); err != nil {
			return fmt.Errorf("append event %s: %w", evt.ID, err)
		}
	}
	return nil
}

// redisEventKeys are the framework's hashidx event keys (session/redis v1.11
// default layout, the same one enumerateRedis scans): evtidx:time is a zset of
// event ID scored by timestamp — the order the framework reads events in —
// and evtdata a hash of event ID → event JSON, both written atomically by the
// append script.
func redisEventKeys(k session.Key) (idxKey, dataKey string) {
	return fmt.Sprintf("hashidx:evtidx:time:%s:{%s}:%s", k.AppName, k.UserID, k.SessionID),
		fmt.Sprintf("hashidx:evtdata:%s:{%s}:%s", k.AppName, k.UserID, k.SessionID)
}

// redisHMGetChunk bounds one HMGET so a very long journal does not turn into
// one unbounded redis request.
const redisHMGetChunk = 1000

// fullJournalRedis reads one session's complete journal straight from the
// hashidx layout. GetSession cannot serve this read: beyond the 1000-event
// cap it also drops the head up to the first user message, and there is no
// option to widen on the service the migrator is handed — the raw keys are
// the only full-journal view a redis backend has.
func (m *Migrator) fullJournalRedis(ctx context.Context, key session.Key) ([]event.Event, error) {
	idxKey, dataKey := redisEventKeys(key)
	ids, err := m.rdb.ZRange(ctx, idxKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read redis event index: %w", err)
	}
	var out []event.Event
	for start := 0; start < len(ids); start += redisHMGetChunk {
		end := min(start+redisHMGetChunk, len(ids))
		vals, err := m.rdb.HMGet(ctx, dataKey, ids[start:end]...).Result()
		if err != nil {
			return nil, fmt.Errorf("read redis events: %w", err)
		}
		for i, v := range vals {
			raw, ok := v.(string)
			if !ok {
				// The event vanished between the two reads; the consistency
				// check re-reads both journals and reports the gap.
				continue
			}
			var evt event.Event
			if err := json.Unmarshal([]byte(raw), &evt); err != nil {
				return nil, fmt.Errorf("decode redis event %s: %w", ids[start+i], err)
			}
			out = append(out, evt)
		}
	}
	return out, nil
}

// writeSessionToPG makes the target journal exactly the source journal,
// reconciled by event ID rather than by position. The events are a parameter
// rather than sess.Events because the caller may have read them past
// GetSession's summary/archive truncation.
//
// Position cannot work: the fanout may append the live tail to an empty
// target, so the target's seqs no longer form the source's prefix.
//
// The reconcile runs under the session row lock (the same FOR UPDATE
// AppendEvent's ensureSession takes), so a dual-write append racing this copy
// either committed before it — its event is part of the plan: a row matched
// to its source position, or an extra parked beyond the prefix — or waits for
// the commit and takes the next seq. Rows are matched on event->>'id', the
// framework's event identity, which dual write preserves because the fanout
// hands both backends the same event; a re-run therefore plans no moves and
// no inserts. No row is ever deleted: ids stay stable for
// summary.covered_event_id, and an event the source never saw (its primary
// write failed) stays on the target as a visible extra the consistency check
// reports instead of a silently vanished row.
func (m *Migrator) writeSessionToPG(ctx context.Context, tenantID string, sess *session.Session, events []event.Event) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	stateJSON, err := encodeState(sess.State)
	if err != nil {
		return err
	}
	sessID, err := m.lockTargetSession(ctx, tx, tenantID, sess, stateJSON)
	if err != nil {
		return err
	}

	// The target's journal by event ID, in seq order.
	type targetRow struct {
		rowID   string
		seq     int64
		eventID string
	}
	var (
		byID     = make(map[string]targetRow, len(events)) // event ID → row
		seqOrder []targetRow                               // every row, seq-ordered
	)
	rows, err := tx.Query(ctx,
		`SELECT id, event_seq, COALESCE(event->>'id', '') FROM session_event
		 WHERE session_id = $1 ORDER BY event_seq`, sessID)
	if err != nil {
		return fmt.Errorf("read target journal: %w", err)
	}
	for rows.Next() {
		var r targetRow
		if err := rows.Scan(&r.rowID, &r.seq, &r.eventID); err != nil {
			rows.Close()
			return fmt.Errorf("scan target event: %w", err)
		}
		if r.eventID != "" {
			byID[r.eventID] = r // last row wins: an ID held twice keeps one, the other becomes an extra
		}
		seqOrder = append(seqOrder, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate target journal: %w", err)
	}
	rows.Close()

	// Plan. Each source event claims its 1-based position; the row holding it
	// moves there if it is not already there, and a missing event is inserted
	// at it. Target rows no source event claims (the dual-written tail whose
	// primary write had not landed yet, or a duplicate ID) are parked beyond
	// the prefix in their current order — they are by construction newer than
	// everything in the source snapshot, so the end is their chronological
	// place.
	finalSeq := make(map[string]int64, len(seqOrder))
	var missing []struct {
		evt event.Event
		seq int64
	}
	for i := range events {
		pos := int64(i + 1)
		if r, ok := byID[events[i].ID]; ok {
			finalSeq[r.rowID] = pos
			continue
		}
		missing = append(missing, struct {
			evt event.Event
			seq int64
		}{events[i], pos})
	}
	next := int64(len(events) + 1)
	for _, r := range seqOrder {
		if _, claimed := finalSeq[r.rowID]; !claimed {
			finalSeq[r.rowID] = next
			next++
		}
	}

	// Place. A mover's destination is always free (each position is claimed by
	// at most one row, and non-movers already sit on theirs), but a mover's
	// ORIGIN may still be occupied by another mover's destination, so every
	// mover first vacates to the negation of its current seq — distinct seqs
	// negate to distinct seqs, and the negative range is unreachable for the
	// append path, which only ever takes max+1.
	var movers []string
	for _, r := range seqOrder {
		if finalSeq[r.rowID] != r.seq {
			movers = append(movers, r.rowID)
		}
	}
	if len(movers) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE session_event SET event_seq = -event_seq
			 WHERE session_id = $1 AND id = ANY($2)`, sessID, movers); err != nil {
			return fmt.Errorf("vacate target events: %w", err)
		}
		for rowID, seq := range finalSeq {
			if _, err := tx.Exec(ctx,
				`UPDATE session_event SET event_seq = $3 WHERE session_id = $1 AND id = $2`,
				sessID, rowID, seq); err != nil {
				return fmt.Errorf("move target event to seq %d: %w", seq, err)
			}
		}
	}
	// Missing events insert at their source positions. No ON CONFLICT: every
	// conflicting position was vacated above, so a collision here means the
	// plan is wrong, and failing the session loudly beats storing a scrambled
	// journal a count check would later bless.
	for _, miss := range missing {
		raw, err := json.Marshal(miss.evt)
		if err != nil {
			return fmt.Errorf("marshal event %s: %w", miss.evt.ID, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_event (session_id, event_seq, event) VALUES ($1, $2, $3)`,
			sessID, miss.seq, raw); err != nil {
			return fmt.Errorf("insert event %s at seq %d: %w", miss.evt.ID, miss.seq, err)
		}
	}
	return tx.Commit(ctx)
}

// lockTargetSession returns the target session's UUID under the row lock the
// append path takes, inserting the row when absent. The lock is what makes
// the rewrite atomic against dual write: an AppendEvent for this session
// either waits for the reconcile to commit and then takes max(seq)+1, or
// committed first and its event is part of the plan.
func (m *Migrator) lockTargetSession(ctx context.Context, tx pgx.Tx, tenantID string, sess *session.Session, stateJSON []byte) (string, error) {
	var sessID string
	err := tx.QueryRow(ctx,
		`SELECT id FROM session WHERE app_id = $1 AND session_key = $2 FOR UPDATE`,
		sess.AppName, sess.ID).Scan(&sessID)
	if err == nil {
		return sessID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("lock target session: %w", err)
	}
	// State is only seeded on insert: an existing row's snapshot was written
	// by dual write's AppendEvent, which stores the runner's complete state —
	// newer than anything this copy read. The conflict arm exists for a
	// concurrent first insert; ON CONFLICT DO UPDATE takes the row lock, so
	// the returned id is locked too.
	err = tx.QueryRow(ctx,
		`INSERT INTO session (tenant_id, app_id, session_key, user_id, channel, state)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (app_id, session_key) DO UPDATE SET session_key = EXCLUDED.session_key
		 RETURNING id`,
		tenantID, sess.AppName, sess.ID, sess.UserID, channelOf(sess.ID), stateJSON).Scan(&sessID)
	if err != nil {
		return "", fmt.Errorf("upsert target session: %w", err)
	}
	return sessID, nil
}

// checkConsistency compares the two backends' full journals, session by
// session, as event-ID multisets, and returns the mismatching sessions.
//
// A multiset, not a count: a dual-write tail that reached the target before
// the backfill copied the prefix leaves both sides holding the same NUMBER of
// events — the target's first k seqs hold the source's last k events — so
// only a content comparison catches it. It is a multiset rather than an
// ordered list because the redis journal's order is (timestamp, member): two
// events inside one timestamp are ordered by ID there but by arrival on PG,
// and that framework-level tie order is not something a migration should fail
// on. A multiset still catches every failure this pipeline can produce —
// missing, extra and duplicated events.
func (m *Migrator) checkConsistency(ctx context.Context, mig migrationRow) ([]string, error) {
	keys, err := m.enumerateAll(ctx, mig)
	if err != nil {
		return nil, err
	}
	var mismatches []string
	for _, key := range keys {
		srcIDs, err := m.journalIDs(ctx, mig.From, key)
		if err != nil {
			return nil, err
		}
		dstIDs, err := m.journalIDs(ctx, mig.To, key)
		if err != nil {
			return nil, err
		}
		if missing, extra := idDiff(srcIDs, dstIDs); len(missing) > 0 || len(extra) > 0 {
			mismatches = append(mismatches, fmt.Sprintf("%s/%s(missing %s; extra %s)",
				key.AppName, key.SessionID, idList(missing), idList(extra)))
		}
	}
	return mismatches, nil
}

// journalIDs reads one backend's full journal as event IDs. The PG branch goes
// through FullJournalIDs (hot table plus archive, past the summary cursor);
// the redis branch reads the event index zset directly, because GetSession
// caps the journal and anchors its head at the first user message — a count
// over truncated views would miss missing history.
func (m *Migrator) journalIDs(ctx context.Context, backend string, key session.Key) ([]string, error) {
	if backend == "postgres" {
		pg, ok := m.backends[backend].(*PGSessionService)
		if !ok {
			return nil, fmt.Errorf("%s backend is not the platform session service", backend)
		}
		return pg.FullJournalIDs(ctx, key)
	}
	idxKey, _ := redisEventKeys(key)
	ids, err := m.rdb.ZRange(ctx, idxKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("read redis event index: %w", err)
	}
	return ids, nil
}

// idDiff reports the event IDs src holds that dst does not (missing) and the
// ones dst holds beyond src (extra), sorted for a deterministic message.
func idDiff(src, dst []string) (missing, extra []string) {
	count := make(map[string]int, len(src))
	for _, id := range src {
		count[id]++
	}
	for _, id := range dst {
		if count[id] > 0 {
			count[id]--
			continue
		}
		extra = append(extra, id)
	}
	for id, n := range count {
		if n > 0 {
			missing = append(missing, id)
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	return missing, extra
}

// idList renders a few IDs plus a count, so a badly divergent session stays a
// one-line mismatch entry.
func idList(ids []string) string {
	const show = 3
	if len(ids) == 0 {
		return "none"
	}
	if len(ids) <= show {
		return strings.Join(ids, ",")
	}
	return fmt.Sprintf("%s +%d more", strings.Join(ids[:show], ","), len(ids)-show)
}

// enumerateAll lists every session of the tenant (no batching): consistency
// comparison is full.
func (m *Migrator) enumerateAll(ctx context.Context, mig migrationRow) ([]session.Key, error) {
	var all []session.Key
	cursor := enumCursor{}
	for {
		batch, err := m.enumerate(ctx, mig, cursor)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return all, nil
		}
		all = append(all, batch...)
		last := batch[len(batch)-1]
		cursor = enumCursor{appID: last.AppName, sessionKey: last.SessionID}
	}
}

func (m *Migrator) countTenantSessions(ctx context.Context, mig migrationRow) (int, error) {
	if mig.From == "postgres" {
		var n int
		if err := m.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM session WHERE tenant_id = $1`, mig.TenantID).Scan(&n); err != nil {
			return 0, err
		}
		return n, nil
	}
	keys, err := m.enumerateRedis(ctx, mig.TenantID, enumCursor{})
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

// tenantApps returns the tenant's app IDs (redis session keys are per app).
func (m *Migrator) tenantApps(ctx context.Context, tenantID string) ([]string, error) {
	rows, err := m.pool.Query(ctx, `SELECT id FROM agent_app WHERE tenant_id = $1`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (m *Migrator) setPhase(ctx context.Context, tx pgx.Tx, id, phase string, progress migrationProgress) error {
	raw, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`UPDATE storage_migration SET phase = $2, progress = $3, updated_at = now() WHERE id = $1`,
		id, phase, raw)
	return err
}

// fail parks the migration with the error; reads stay on the old backend.
// Runs on the claim tx so the marking commits (or rolls back) with the tick —
// if the tx is already aborted the exec fails and the row is retried next
// tick, the log line keeps that visible.
func (m *Migrator) fail(ctx context.Context, tx pgx.Tx, mig migrationRow, cause error) {
	if _, err := tx.Exec(ctx,
		`UPDATE storage_migration SET phase = 'failed', error = $2, updated_at = now() WHERE id = $1`,
		mig.ID, cause.Error()); err != nil {
		plog.Errorf("migration %s: fail-marking failed: %v", mig.ID, err)
	}
}
