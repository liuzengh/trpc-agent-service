package sessionturn

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestSessionServiceIntegrationConcurrentCreateIsAtomic(t *testing.T) {
	serviceA, store := newIntegrationSessionService(t)
	serviceB, err := NewSessionService(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serviceB.Close() })
	ctx := context.Background()
	key := testKey("service-concurrent-create")

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, service := range []*SessionService{serviceA, serviceB} {
		wg.Add(1)
		go func(candidate *SessionService) {
			defer wg.Done()
			<-start
			_, err := candidate.CreateSession(ctx, key, session.StateMap{"created": []byte("yes")})
			errs <- err
		}(service)
	}
	close(start)
	wg.Wait()
	close(errs)

	succeeded, existed := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrSessionExists):
			existed++
		default:
			t.Fatalf("concurrent CreateSession: %v", err)
		}
	}
	if succeeded != 1 || existed != 1 {
		t.Fatalf("concurrent CreateSession outcomes success=%d exists=%d", succeeded, existed)
	}
	snapshot, err := store.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || snapshot.Version != 0 || string(snapshot.State["created"]) != "yes" {
		t.Fatalf("created snapshot = %+v", snapshot)
	}
}

func newIntegrationSessionService(t *testing.T) (*SessionService, *Postgres) {
	t.Helper()
	store := newPostgresIntegrationStore(t, true)
	service, err := NewSessionService(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Errorf("close session service: %v", err)
		}
	})
	return service, store
}

func TestSessionServiceIntegrationStrictCommitReplayAndOverlay(t *testing.T) {
	service, store := newIntegrationSessionService(t)
	ctx := context.Background()
	key := testKey("service-strict")
	userKey := session.UserKey{AppName: key.AppName, UserID: key.UserID}

	if err := service.UpdateAppState(ctx, key.AppName, session.StateMap{
		session.StateAppPrefix + "config": []byte("enabled"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateUserState(ctx, userKey, session.StateMap{
		session.StateUserPrefix + "theme": []byte("dark"),
	}); err != nil {
		t.Fatal(err)
	}

	turnCtx, turn, err := service.BeginTurn(ctx, key, "inbox-stable-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := turn.Replay(); ok {
		t.Fatal("fresh turn unexpectedly reported replay")
	}
	sess, err := service.GetSession(turnCtx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := sess.GetState(session.StateAppPrefix + "config"); string(got) != "enabled" {
		t.Fatalf("application overlay = %q, want enabled", got)
	}
	if got, _ := sess.GetState(session.StateUserPrefix + "theme"); string(got) != "dark" {
		t.Fatalf("user overlay = %q, want dark", got)
	}

	evt := validSessionTurnEvent("original-event", model.RoleUser, "hello")
	evt.StateDelta = session.StateMap{"from_event": []byte("original")}
	if err := service.AppendEvent(turnCtx, sess, evt); err != nil {
		t.Fatal(err)
	}
	// Mutating every caller-owned field after AppendEvent must not alter the
	// event or state eventually committed by the turn.
	evt.ID = "mutated-event"
	evt.StateDelta["from_event"][0] = 'X'
	evt.Response.Choices[0].Message.Content = "mutated"
	if err := service.UpdateSessionState(turnCtx, key, session.StateMap{
		"direct": []byte("state"),
	}); err != nil {
		t.Fatal(err)
	}

	replayInput := []byte(`{"request_id":"canonical"}`)
	replay, replayed, err := turn.Commit(ctx, replayInput)
	if err != nil {
		t.Fatal(err)
	}
	if replayed || string(replay) != string(replayInput) {
		t.Fatalf("Commit replay=%q replayed=%v", replay, replayed)
	}
	replayInput[0] = '!'

	raw, err := store.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil || raw.Version != 1 || len(raw.Events) != 1 {
		t.Fatalf("raw snapshot = %+v", raw)
	}
	if raw.Events[0].ID != "original-event" ||
		raw.Events[0].Response.Choices[0].Message.Content != "hello" {
		t.Fatalf("persisted event changed through caller: %+v", raw.Events[0])
	}
	if got := string(raw.State["from_event"]); got != "original" {
		t.Fatalf("persisted event state = %q, want original", got)
	}
	if _, ok := raw.State[session.StateAppPrefix+"config"]; ok {
		t.Fatal("application overlay leaked into session-level state")
	}
	if _, ok := raw.State[session.StateUserPrefix+"theme"]; ok {
		t.Fatal("user overlay leaked into session-level state")
	}

	loaded, err := service.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := loaded.GetState("direct"); string(got) != "state" {
		t.Fatalf("direct session state = %q, want state", got)
	}
	if got, _ := loaded.GetState(session.StateAppPrefix + "config"); string(got) != "enabled" {
		t.Fatalf("loaded application overlay = %q, want enabled", got)
	}

	if err := service.AppendEvent(
		context.WithoutCancel(turnCtx),
		sess,
		validSessionTurnEvent("late", model.RoleAssistant, "late"),
	); !errors.Is(err, ErrTurnScopeClosed) {
		t.Fatalf("late AppendEvent = %v, want ErrTurnScopeClosed", err)
	}

	_, duplicate, err := turn.Commit(ctx, []byte("different"))
	if err != nil || !duplicate {
		t.Fatalf("duplicate local Commit replayed=%v err=%v", duplicate, err)
	}
	replayCtx, replayTurn, err := service.BeginTurn(ctx, key, "inbox-stable-1")
	if err != nil {
		t.Fatal(err)
	}
	canonical, ok := replayTurn.Replay()
	if !ok || string(canonical) != `{"request_id":"canonical"}` {
		t.Fatalf("BeginTurn replay = %q, %v", canonical, ok)
	}
	if _, err := service.GetSession(replayCtx, key); !errors.Is(err, ErrTurnScopeClosed) {
		t.Fatalf("GetSession(replay context) = %v, want ErrTurnScopeClosed", err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TABLE replay_participant_log (replay bytea NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	atomicReplay, ok := replayTurn.(AtomicTurn)
	if !ok {
		t.Fatal("replayed staged turn does not expose AtomicTurn")
	}
	committedReplay, duplicate, err := atomicReplay.CommitWithParticipant(
		ctx,
		[]byte("losing-local-replay"),
		func(participantCtx context.Context, tx pgx.Tx, replay []byte) error {
			_, err := tx.Exec(participantCtx, `INSERT INTO replay_participant_log (replay) VALUES ($1)`, replay)
			return err
		},
	)
	if err != nil || !duplicate || string(committedReplay) != `{"request_id":"canonical"}` {
		t.Fatalf("atomic replay completion = %q duplicate=%v err=%v", committedReplay, duplicate, err)
	}
	var participantReplay []byte
	if err := store.pool.QueryRow(ctx, `SELECT replay FROM replay_participant_log`).Scan(&participantReplay); err != nil {
		t.Fatal(err)
	}
	if string(participantReplay) != `{"request_id":"canonical"}` {
		t.Fatalf("participant received replay %q", participantReplay)
	}
}

func TestSessionServiceIntegrationFencingAndLocalAbort(t *testing.T) {
	serviceA, store := newIntegrationSessionService(t)
	serviceB, err := NewSessionService(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serviceB.Close() })
	ctx := context.Background()
	key := testKey("service-fence")

	ctxA, turnA, err := serviceA.BeginTurn(ctx, key, "logical-a")
	if err != nil {
		t.Fatal(err)
	}
	sessA, err := serviceA.GetSession(ctxA, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := serviceA.AppendEvent(
		ctxA,
		sessA,
		validSessionTurnEvent("event-a", model.RoleUser, "a"),
	); err != nil {
		t.Fatal(err)
	}
	_, turnB, err := serviceB.BeginTurn(ctx, key, "logical-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := turnA.Commit(ctx, []byte("stale")); !errors.Is(err, ErrFenceLost) {
		t.Fatalf("stale Commit = %v, want ErrFenceLost", err)
	}
	if _, _, err := turnB.Commit(ctx, []byte("winner")); err != nil {
		t.Fatal(err)
	}

	// Abort is strictly local: retrying the same logical ID takes over the
	// persisted active row instead of encountering Postgres.ErrTurnAborted.
	abortCtx, aborted, err := serviceA.BeginTurn(ctx, key, "retry-after-local-abort")
	if err != nil {
		t.Fatal(err)
	}
	abortSess, err := serviceA.GetSession(abortCtx, key)
	if err != nil {
		t.Fatal(err)
	}
	aborted.Abort()
	if err := serviceA.AppendEvent(
		context.WithoutCancel(abortCtx),
		abortSess,
		validSessionTurnEvent("too-late", model.RoleUser, "late"),
	); !errors.Is(err, ErrTurnScopeClosed) {
		t.Fatalf("AppendEvent after Abort = %v, want ErrTurnScopeClosed", err)
	}
	_, retry, err := serviceB.BeginTurn(ctx, key, "retry-after-local-abort")
	if err != nil {
		t.Fatalf("BeginTurn after local Abort: %v", err)
	}
	if _, ok := retry.Replay(); ok {
		t.Fatal("retry after local Abort unexpectedly replayed")
	}
	if _, _, err := retry.Commit(ctx, []byte("retried")); err != nil {
		t.Fatal(err)
	}
}

func TestSessionServiceIntegrationCRUDFilteringAndScopedFailure(t *testing.T) {
	service, store := newIntegrationSessionService(t)
	ctx := context.Background()
	key := testKey("service-crud")
	userKey := session.UserKey{AppName: key.AppName, UserID: key.UserID}

	created, err := service.CreateSession(ctx, key, session.StateMap{"created": []byte("yes")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateSession(ctx, key, nil); !errors.Is(err, ErrSessionExists) {
		t.Fatalf("duplicate CreateSession = %v, want ErrSessionExists", err)
	}
	if err := service.UpdateSessionState(ctx, key, session.StateMap{"updated": []byte("yes")}); err != nil {
		t.Fatal(err)
	}
	for i, role := range []model.Role{model.RoleUser, model.RoleAssistant, model.RoleUser} {
		if err := service.AppendEvent(
			ctx,
			created,
			validSessionTurnEvent("crud-event-"+string(rune('1'+i)), role, "content"),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.UpdateAppState(ctx, key.AppName, session.StateMap{"app:key": []byte("app")}); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateUserState(ctx, userKey, session.StateMap{"user:key": []byte("user")}); err != nil {
		t.Fatal(err)
	}

	paged, err := service.GetSession(ctx, key, session.WithGetSessionEventPage(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(paged.Events) != 1 || paged.Events[0].ID != "crud-event-2" {
		t.Fatalf("paged events = %+v, want crud-event-2", paged.Events)
	}
	if got, _ := paged.GetState("app:key"); string(got) != "app" {
		t.Fatalf("application state overlay = %q, want app", got)
	}
	if got, _ := paged.GetState("user:key"); string(got) != "user" {
		t.Fatalf("user state overlay = %q, want user", got)
	}

	key2 := key
	key2.SessionID += "-second"
	if _, err := service.CreateSession(ctx, key2, nil); err != nil {
		t.Fatal(err)
	}
	listed, err := service.ListSessions(
		ctx,
		userKey,
		session.WithListSessionOnlyMeta(),
		session.WithListSessionPage(0, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || len(listed[0].Events) != 0 {
		t.Fatalf("metadata page = %+v, want one event-free session", listed)
	}

	strictCtx, strict, err := service.BeginTurn(ctx, key, "scoped-write-fails")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateAppState(strictCtx, key.AppName, session.StateMap{"x": []byte("y")}); !errors.Is(err, ErrScopedStateUnsupported) {
		t.Fatalf("UpdateAppState inside turn = %v, want ErrScopedStateUnsupported", err)
	}
	if _, _, err := strict.Commit(ctx, []byte("must-not-commit")); !errors.Is(err, ErrScopedStateUnsupported) {
		t.Fatalf("Commit after scoped write = %v, want sticky error", err)
	}
	rawBeforeRetry, err := store.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	versionBeforeRetry := rawBeforeRetry.Version
	_, retry, err := service.BeginTurn(ctx, key, "scoped-write-fails")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := retry.Commit(ctx, []byte("retry-ok")); err != nil {
		t.Fatal(err)
	}
	rawAfterRetry, err := store.Load(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if rawAfterRetry.Version != versionBeforeRetry+1 {
		t.Fatalf("retry version = %d, want %d", rawAfterRetry.Version, versionBeforeRetry+1)
	}

	if err := service.DeleteAppState(ctx, key.AppName, "app:key"); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteUserState(ctx, userKey, "user:key"); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	missing, err := service.GetSession(ctx, key)
	if err != nil || missing != nil {
		t.Fatalf("GetSession after delete = %+v, %v", missing, err)
	}
}
