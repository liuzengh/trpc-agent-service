package inmemory

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

func TestReplicaIdempotencyNoRegressionAndFingerprint(t *testing.T) {
	ctx := context.Background()
	replica := NewReplica()
	key := sessionstore.SessionKey{TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a"}
	newer := replicaImage(key, 2)
	newerDigest, err := sessiondriver.SnapshotDigest(newer)
	if err != nil {
		t.Fatal(err)
	}
	request := sessiondriver.ApplyRequest{TenantID: key.TenantID, MigrationID: "migration-a",
		MutationID: "mutation-new", Epoch: 1, Image: newer, SnapshotDigest: newerDigest}
	first, err := replica.ApplySessionSnapshot(ctx, request)
	if err != nil || first.SessionVersion != 2 || first.SnapshotDigest != newerDigest {
		t.Fatalf("apply=%+v err=%v", first, err)
	}
	if replay, err := replica.ApplySessionSnapshot(ctx, request); err != nil || replay != first {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	collision := request
	collision.Epoch = 2
	if _, err := replica.ApplySessionSnapshot(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision=%v", err)
	}
	older := replicaImage(key, 1)
	olderDigest, err := sessiondriver.SnapshotDigest(older)
	if err != nil {
		t.Fatal(err)
	}
	noRegression, err := replica.ApplySessionSnapshot(ctx, sessiondriver.ApplyRequest{TenantID: key.TenantID,
		MigrationID: "migration-a", MutationID: "mutation-old", Epoch: 1, Image: older, SnapshotDigest: olderDigest})
	if err != nil || noRegression.SessionVersion != 2 || noRegression.SnapshotDigest != newerDigest {
		t.Fatalf("old image regressed target=%+v err=%v", noRegression, err)
	}
	fingerprint, err := replica.Fingerprint(ctx, key.TenantID, "")
	if err != nil || fingerprint.Count != 1 || fingerprint.Watermark == "" {
		t.Fatalf("fingerprint=%+v err=%v", fingerprint, err)
	}
}

func replicaImage(key sessionstore.SessionKey, version int64) sessiondriver.SessionImage {
	clock := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	image := sessiondriver.SessionImage{Head: sessionstore.SessionHead{SessionKey: key, Version: version,
		NextInputSeq: 1, State: map[string]any{}}, SDK: &sessiondriver.SDKSessionImage{AppName: key.TenantID + "/" + key.AgentAppID,
		UserID: "user-a", SessionID: key.SessionID, State: []byte(`{"id":"session-a","state":{}}`), CreatedAt: clock, UpdatedAt: clock}}
	for current := int64(1); current <= version; current++ {
		image.Commits = append(image.Commits, sessiondriver.CommitRecord{CommitID: "commit-" + strconv.FormatInt(current, 10),
			RequestID: "request", RequestDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Stage: "waiting", InputSeq: 1, Fence: 1, Outcome: runtime.OutcomeWaitingConfirmation,
			SessionVersion: current, CreatedAt: clock.Add(time.Duration(current) * time.Second)})
	}
	return image
}
