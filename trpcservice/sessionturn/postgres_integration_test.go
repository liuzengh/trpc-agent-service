package sessionturn

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const postgresTestDSNEnv = "TEST_POSTGRES_DSN"

func newPostgresIntegrationStore(t *testing.T, ensureSchema bool) *Postgres {
	t.Helper()
	dsn := os.Getenv(postgresTestDSNEnv)
	if dsn == "" {
		t.Skip(postgresTestDSNEnv + " is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open integration admin pool: %v", err)
	}
	schemaName := "sessionturn_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create integration schema: %v", err)
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse integration DSN: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatalf("open integration schema pool: %v", err)
	}
	store, err := NewPostgres(pool)
	if err != nil {
		pool.Close()
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop integration schema: %v", err)
		}
		admin.Close()
	})
	if ensureSchema {
		if err := store.EnsureSchema(ctx); err != nil {
			t.Fatalf("ensure integration schema: %v", err)
		}
	}
	return store
}

func testKey(suffix string) session.Key {
	return session.Key{AppName: "app", UserID: "user", SessionID: "session-" + suffix}
}

func TestPostgresIntegrationConcurrentSchemaInitialization(t *testing.T) {
	store := newPostgresIntegrationStore(t, false)
	ctx := context.Background()
	const replicas = 8
	start := make(chan struct{})
	errs := make(chan error, replicas)
	var wg sync.WaitGroup
	for range replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- store.EnsureSchema(ctx)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent EnsureSchema: %v", err)
		}
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("idempotent EnsureSchema: %v", err)
	}

	var tables int
	if err := store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.tables
		WHERE table_schema = current_schema()
			AND table_name IN (
				'session_turn_sessions', 'session_turns', 'session_turn_events',
				'session_turn_app_states', 'session_turn_user_states'
			)`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 5 {
		t.Fatalf("created tables = %d, want 5", tables)
	}
}

func TestPostgresIntegrationFrozenSnapshotImportIsIdempotentAndConflictSafe(t *testing.T) {
	store := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	key := testKey("migration-import")
	now := time.Now().UTC().Truncate(time.Microsecond)
	snapshot := MigrationSnapshot{
		Key:       key,
		State:     session.StateMap{"local": []byte("value")},
		Events:    []event.Event{*validSessionTurnEvent("migration-event", model.RoleUser, "hello")},
		AppState:  session.StateMap{"theme": []byte("dark")},
		UserState: session.StateMap{"locale": []byte("zh")},
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
	}
	if err := store.ImportFrozenSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.ImportFrozenSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("idempotent import: %v", err)
	}
	loaded, err := store.Load(ctx, key)
	if err != nil || loaded == nil || len(loaded.Events) != 1 || string(loaded.State["local"]) != "value" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	changed := snapshot
	changed.State = session.StateMap{"local": []byte("different")}
	if err := store.ImportFrozenSnapshot(ctx, changed); !errors.Is(err, ErrMigrationTargetConflict) {
		t.Fatalf("conflicting import error=%v", err)
	}
}

func TestPostgresIntegrationAtomicCommitAndReplay(t *testing.T) {
	store := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	key := testKey("replay")
	turnID, err := DeriveTurnID(key, "inbox-1")
	if err != nil {
		t.Fatal(err)
	}

	begun, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: turnID})
	if err != nil {
		t.Fatal(err)
	}
	if begun.Replayed || begun.Snapshot == nil {
		t.Fatalf("first Begin = %+v", begun)
	}
	if begun.Handle.ExpectedVersion != 0 || begun.Handle.FencingToken != 1 {
		t.Fatalf("first handle = %+v", begun.Handle)
	}
	if len(begun.Snapshot.State) != 0 || len(begun.Snapshot.Events) != 0 {
		t.Fatalf("initial snapshot = %+v", begun.Snapshot)
	}

	resumed, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: turnID})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Handle.ExpectedVersion != begun.Handle.ExpectedVersion ||
		resumed.Handle.FencingToken != begun.Handle.FencingToken+1 ||
		resumed.Replayed || resumed.Snapshot == nil {
		t.Fatalf("taken-over active turn = %+v, prior handle %+v", resumed, begun.Handle)
	}
	if _, err := store.Commit(ctx, CommitRequest{Handle: begun.Handle}); !errors.Is(err, ErrFenceLost) {
		t.Fatalf("Commit(pre-takeover handle) = %v, want ErrFenceLost", err)
	}

	events := []event.Event{
		{
			ID:           "event-1",
			InvocationID: "invocation-1",
			Author:       "user",
			Timestamp:    time.Now().UTC().Round(time.Microsecond),
			StateDelta:   map[string][]byte{"answer": []byte("42")},
		},
		{
			ID:           "event-2",
			InvocationID: "invocation-1",
			Author:       "agent",
			Timestamp:    time.Now().UTC().Round(time.Microsecond),
		},
	}
	state := session.StateMap{"answer": []byte("42")}
	replay := []byte(`{"message":"done"}`)
	committed, err := store.Commit(ctx, CommitRequest{
		Handle: resumed.Handle,
		Events: events,
		State:  state,
		Replay: replay,
	})
	if err != nil {
		t.Fatal(err)
	}
	if committed.Version != 1 || committed.Replayed || string(committed.Replay) != string(replay) {
		t.Fatalf("commit result = %+v", committed)
	}

	// Caller-owned buffers may be changed after Commit without changing replay.
	replay[0] = '!'
	state["answer"][0] = 'x'
	duplicateHandle := resumed.Handle
	duplicateHandle.ExpectedVersion = 99
	duplicateHandle.FencingToken = 99
	duplicate, err := store.Commit(ctx, CommitRequest{Handle: duplicateHandle})
	if err != nil {
		t.Fatalf("duplicate Commit: %v", err)
	}
	if !duplicate.Replayed || duplicate.Version != 1 || string(duplicate.Replay) != `{"message":"done"}` {
		t.Fatalf("duplicate commit result = %+v", duplicate)
	}

	replayed, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: turnID})
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Snapshot != nil || string(replayed.Replay) != `{"message":"done"}` {
		t.Fatalf("replayed Begin = %+v", replayed)
	}

	second, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: "turn-2"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Handle.ExpectedVersion != 1 || second.Handle.FencingToken != 3 {
		t.Fatalf("second handle = %+v", second.Handle)
	}
	if got := string(second.Snapshot.State["answer"]); got != "42" {
		t.Fatalf("snapshot state answer = %q", got)
	}
	if len(second.Snapshot.Events) != 2 || second.Snapshot.Events[0].ID != "event-1" || second.Snapshot.Events[1].ID != "event-2" {
		t.Fatalf("snapshot events = %+v", second.Snapshot.Events)
	}
	if second.Snapshot.LastEventSequence != 2 {
		t.Fatalf("last event sequence = %d, want 2", second.Snapshot.LastEventSequence)
	}
	if err := store.Abort(ctx, AbortRequest{
		Key: key, TurnID: turnID, FencingToken: resumed.Handle.FencingToken,
	}); !errors.Is(err, ErrTurnCommitted) {
		t.Fatalf("Abort(committed) = %v, want ErrTurnCommitted", err)
	}
}

func TestPostgresIntegrationFencingVersionAndAbort(t *testing.T) {
	store := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	key := testKey("fence")

	first, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: "turn-2"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Handle.FencingToken <= first.Handle.FencingToken {
		t.Fatalf("fences are not monotonic: first=%d second=%d", first.Handle.FencingToken, second.Handle.FencingToken)
	}
	if _, err := store.Commit(ctx, CommitRequest{Handle: first.Handle}); !errors.Is(err, ErrFenceLost) {
		t.Fatalf("Commit(stale fence) = %v, want ErrFenceLost", err)
	}

	wrongVersion := second.Handle
	wrongVersion.ExpectedVersion++
	if _, err := store.Commit(ctx, CommitRequest{Handle: wrongVersion}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("Commit(wrong version) = %v, want ErrVersionConflict", err)
	}
	wrongFence := second.Handle
	wrongFence.FencingToken++
	if _, err := store.Commit(ctx, CommitRequest{Handle: wrongFence}); !errors.Is(err, ErrFenceLost) {
		t.Fatalf("Commit(wrong fence) = %v, want ErrFenceLost", err)
	}
	if err := store.Abort(ctx, AbortRequest{
		Key: key, TurnID: second.Handle.TurnID, FencingToken: wrongFence.FencingToken,
	}); !errors.Is(err, ErrFenceLost) {
		t.Fatalf("Abort(wrong fence) = %v, want ErrFenceLost", err)
	}
	abort := AbortRequest{Key: key, TurnID: second.Handle.TurnID, FencingToken: second.Handle.FencingToken}
	if err := store.Abort(ctx, abort); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort(ctx, abort); err != nil {
		t.Fatalf("duplicate Abort: %v", err)
	}
	if _, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: second.Handle.TurnID}); !errors.Is(err, ErrTurnAborted) {
		t.Fatalf("Begin(aborted ID) = %v, want ErrTurnAborted", err)
	}
	if _, err := store.Commit(ctx, CommitRequest{Handle: second.Handle}); !errors.Is(err, ErrTurnAborted) {
		t.Fatalf("Commit(aborted ID) = %v, want ErrTurnAborted", err)
	}
}

func TestPostgresIntegrationConcurrentBeginAndDuplicateCommit(t *testing.T) {
	store := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	key := testKey("concurrent")
	const contenders = 12
	start := make(chan struct{})
	results := make(chan *BeginResult, contenders)
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			result, err := store.Begin(ctx, BeginRequest{
				Key: key, TurnID: "turn-" + string(rune('a'+index)),
			})
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Begin: %v", err)
	}

	seen := make(map[int64]bool, contenders)
	var winner *BeginResult
	for result := range results {
		if seen[result.Handle.FencingToken] {
			t.Fatalf("duplicate fencing token %d", result.Handle.FencingToken)
		}
		seen[result.Handle.FencingToken] = true
		if winner == nil || result.Handle.FencingToken > winner.Handle.FencingToken {
			winner = result
		}
	}
	if len(seen) != contenders || winner.Handle.FencingToken != contenders {
		t.Fatalf("fences = %v, winner = %+v", seen, winner)
	}

	commitStart := make(chan struct{})
	commits := make(chan *CommitResult, 2)
	commitErrs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-commitStart
			result, err := store.Commit(ctx, CommitRequest{
				Handle: winner.Handle,
				State:  session.StateMap{"winner": []byte("yes")},
				Replay: []byte("same-replay"),
			})
			if err != nil {
				commitErrs <- err
				return
			}
			commits <- result
		}()
	}
	close(commitStart)
	wg.Wait()
	close(commits)
	close(commitErrs)
	for err := range commitErrs {
		t.Fatalf("duplicate concurrent Commit: %v", err)
	}
	fresh, replayed := 0, 0
	for result := range commits {
		if result.Version != 1 || string(result.Replay) != "same-replay" {
			t.Fatalf("commit result = %+v", result)
		}
		if result.Replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != 1 {
		t.Fatalf("commit outcomes fresh=%d replayed=%d", fresh, replayed)
	}
}

func TestPostgresIntegrationFailuresRollBackWholeCommit(t *testing.T) {
	store := newPostgresIntegrationStore(t, true)
	ctx := context.Background()
	key := testKey("rollback")
	begun, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	badEvent := event.Event{
		ID:         "bad",
		Extensions: map[string]json.RawMessage{"bad": json.RawMessage(`{`)},
	}
	if _, err := store.Commit(ctx, CommitRequest{
		Handle: begun.Handle,
		Events: []event.Event{badEvent},
		State:  session.StateMap{"must-not": []byte("persist")},
	}); err == nil {
		t.Fatal("Commit with invalid event JSON unexpectedly succeeded")
	}
	resumed, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: "turn-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Handle.ExpectedVersion != begun.Handle.ExpectedVersion ||
		resumed.Handle.FencingToken != begun.Handle.FencingToken+1 ||
		resumed.Snapshot.Version != 0 || len(resumed.Snapshot.State) != 0 {
		t.Fatalf("state changed after encoding failure: %+v", resumed)
	}

	// Fail the final turn-row update after both event insertion and session
	// update. PostgreSQL must roll the entire commit back, not expose a partial
	// event/state write.
	if _, err := store.pool.Exec(ctx, `
		CREATE FUNCTION reject_test_turn_commit() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.turn_id = 'turn-1' AND NEW.status = 'committed' THEN
				RAISE EXCEPTION 'injected commit failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_test_turn_commit
			BEFORE UPDATE ON session_turns
			FOR EACH ROW EXECUTE FUNCTION reject_test_turn_commit();`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, CommitRequest{
		Handle: resumed.Handle,
		Events: []event.Event{{ID: "must-roll-back", Author: "agent"}},
		State:  session.StateMap{"must-not": []byte("persist")},
		Replay: []byte("must-not-replay"),
	}); err == nil {
		t.Fatal("Commit with injected database failure unexpectedly succeeded")
	}
	var version, lastSequence, eventCount int64
	var stateJSON []byte
	var status string
	if err := store.pool.QueryRow(ctx, `
		SELECT s.version, s.last_event_sequence, s.state,
			(SELECT count(*) FROM session_turn_events AS e
			 WHERE e.app_name = s.app_name AND e.user_id = s.user_id AND e.session_id = s.session_id),
			t.status
		FROM session_turn_sessions AS s
		JOIN session_turns AS t USING (app_name, user_id, session_id)
		WHERE s.app_name = $1 AND s.user_id = $2 AND s.session_id = $3 AND t.turn_id = 'turn-1'`,
		key.AppName, key.UserID, key.SessionID).Scan(
		&version, &lastSequence, &stateJSON, &eventCount, &status,
	); err != nil {
		t.Fatal(err)
	}
	if version != 0 || lastSequence != 0 || eventCount != 0 || string(stateJSON) != `{}` || status != turnStatusActive {
		t.Fatalf("partial commit escaped rollback: version=%d sequence=%d events=%d state=%s status=%s",
			version, lastSequence, eventCount, stateJSON, status)
	}
	if _, err := store.pool.Exec(ctx, `
		DROP TRIGGER reject_test_turn_commit ON session_turns;
		DROP FUNCTION reject_test_turn_commit();`); err != nil {
		t.Fatal(err)
	}

	if _, err := store.pool.Exec(ctx, `
		UPDATE session_turn_sessions SET state = '"not-an-object"'::jsonb
		WHERE app_name = $1 AND user_id = $2 AND session_id = $3`,
		key.AppName, key.UserID, key.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(ctx, BeginRequest{Key: key, TurnID: "turn-1"}); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("Begin(corrupt state) = %v, want ErrCorruptData", err)
	}
}
