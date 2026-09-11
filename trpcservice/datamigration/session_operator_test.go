package datamigration

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

type fixedSessionSource struct {
	objects []SessionObject
}

func (s fixedSessionSource) Scan(_ context.Context, cursor string, _ int) ([]SessionObject, string, bool, error) {
	if cursor != "" {
		return nil, "done", true, nil
	}
	return s.objects, "done", true, nil
}

type memorySessionTarget struct {
	objects map[session.Key]SessionObject
	hash    string
}

func (t *memorySessionTarget) Import(_ context.Context, object SessionObject) error {
	t.objects[object.Key] = object
	return nil
}

func (t *memorySessionTarget) Hash(_ context.Context, key session.Key) (string, bool, error) {
	object, ok := t.objects[key]
	if !ok {
		return "", false, nil
	}
	if t.hash != "" {
		return t.hash, true, nil
	}
	hash, err := HashSessionObject(object)
	return hash, true, err
}

type recordCollector struct{ records []ObjectRecord }

func (r *recordCollector) PutObject(_ context.Context, record ObjectRecord) error {
	r.records = append(r.records, record)
	return nil
}

type gateFunc func(context.Context, Job, Phase) error

func (f gateFunc) Check(ctx context.Context, job Job, phase Phase) error { return f(ctx, job, phase) }

type routingStub struct{}

func (routingStub) Cutover(context.Context, Job) error  { return nil }
func (routingStub) Rollback(context.Context, Job) error { return nil }

func TestSessionOperatorCopiesAndRecordsVerifiedHash(t *testing.T) {
	object := SessionObject{
		Key:   session.Key{AppName: "app", UserID: "user", SessionID: "session"},
		State: session.StateMap{"key": []byte("value")}, AppState: session.StateMap{}, UserState: session.StateMap{},
		SourceVer: "v1",
	}
	target := &memorySessionTarget{objects: make(map[session.Key]SessionObject)}
	recorder := &recordCollector{}
	operator, err := NewSessionOperator(
		fixedSessionSource{objects: []SessionObject{object}}, target, recorder,
		gateFunc(func(context.Context, Job, Phase) error { return nil }), routingStub{}, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := operator.ExecutePhase(context.Background(), PhaseSnapshot, testJob())
	if err != nil || !result.Done || result.Copied != 1 || result.Mismatches != 0 {
		t.Fatalf("copy result=%+v err=%v", result, err)
	}
	if len(recorder.records) != 1 || recorder.records[0].Status != ObjectCopied || recorder.records[0].SourceHash != recorder.records[0].TargetHash {
		t.Fatalf("record=%+v", recorder.records)
	}
}

func TestSessionOperatorFailsClosedWhenFreezeIsLost(t *testing.T) {
	operator, err := NewSessionOperator(
		fixedSessionSource{}, &memorySessionTarget{objects: make(map[session.Key]SessionObject)}, &recordCollector{},
		gateFunc(func(context.Context, Job, Phase) error { return ErrSourceNotFrozen }), routingStub{}, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operator.ExecutePhase(context.Background(), PhaseSnapshot, testJob()); !errors.Is(err, ErrSourceNotFrozen) {
		t.Fatalf("freeze loss error=%v", err)
	}
}

type freezeState struct {
	id string
	at time.Time
}

func (f freezeState) TenantFreeze(context.Context, string, string) (string, time.Time, bool, error) {
	return f.id, f.at, f.id != "", nil
}

func TestDistributedMaintenanceGateRequiresOwnershipAndDrain(t *testing.T) {
	now := time.Now().UTC()
	gate, err := NewDistributedMaintenanceGate(freezeState{id: "migration-1", at: now.Add(-time.Minute)}, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	gate.now = func() time.Time { return now }
	if err := gate.Check(context.Background(), testJob(), PhaseSnapshot); !errors.Is(err, ErrSourceNotFrozen) {
		t.Fatalf("undrained gate error=%v", err)
	}
	gate.drain = 30 * time.Second
	if err := gate.Check(context.Background(), testJob(), PhaseSnapshot); err != nil {
		t.Fatalf("drained gate error=%v", err)
	}
	gate.control = freezeState{id: "another", at: now.Add(-time.Hour)}
	if err := gate.Check(context.Background(), testJob(), PhaseSnapshot); !errors.Is(err, ErrSourceNotFrozen) {
		t.Fatalf("wrong owner gate error=%v", err)
	}
}

func TestRoutedMaintenanceGateRejectsWrongBackendForPhase(t *testing.T) {
	backend := "sql"
	gate, err := NewRoutedMaintenanceGate(
		gateFunc(func(context.Context, Job, Phase) error { return nil }),
		func() string { return backend },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Check(context.Background(), testJob(), PhaseSnapshot); !errors.Is(err, ErrTargetActive) {
		t.Fatalf("snapshot SQL route error=%v", err)
	}
	if err := gate.Check(context.Background(), testJob(), PhaseCutover); err != nil {
		t.Fatalf("cutover SQL route error=%v", err)
	}
	backend = "redis"
	if err := gate.Check(context.Background(), testJob(), PhaseCutover); !errors.Is(err, ErrRoutingNotReady) {
		t.Fatalf("cutover Redis route error=%v", err)
	}
}

func TestRoutedMaintenanceGateRejectsWrongDatabaseIdentity(t *testing.T) {
	job := testJob()
	job.Source = "redis-env:SOURCE_REDIS;prefix-b64:c291cmNl"
	job.Target = "postgres-env:TARGET_POSTGRES"
	gate, err := NewRoutedMaintenanceGate(
		gateFunc(func(context.Context, Job, Phase) error { return nil }),
		func() string { return "postgres-env:OTHER_POSTGRES" },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Check(context.Background(), job, PhaseCutover); !errors.Is(err, ErrRoutingNotReady) {
		t.Fatalf("wrong target database route error=%v", err)
	}
}
