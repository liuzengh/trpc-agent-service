//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
)

func TestSummaryPostgresDeferredProgressAndSessionGenerations(t *testing.T) {
	db := openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := uuid.NewString()
	tenantID, appID, versionID := "summary-generation-"+suffix, "app-"+suffix, "version-"+suffix
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, statement := range []string{
			`DELETE FROM summary_checkpoints WHERE tenant_id=$1`,
			`DELETE FROM summary_jobs WHERE tenant_id=$1`,
			`DELETE FROM agent_versions WHERE agent_app_id IN (SELECT id FROM agent_apps WHERE tenant_id=$1)`,
			`DELETE FROM agent_apps WHERE tenant_id=$1`,
			`DELETE FROM tenants WHERE id=$1`,
		} {
			if _, err := db.ExecContext(cleanupCtx, statement, tenantID); err != nil {
				t.Errorf("cleanup summary fixture: %v", err)
			}
		}
	})
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'summary generation','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_apps(id,tenant_id,name,status) VALUES($1,$2,'support','active')`, appID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_versions(id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by)
		VALUES($1,$2,1,'{}',$3,'published','integration')`, versionID, appID, fmt.Sprintf("%064x", 1)); err != nil {
		t.Fatal(err)
	}
	store := summarycoord.NewPostgresStore(db)
	peer := summarycoord.NewPostgresStore(db)
	sink := summarycoord.NewPostgresSink(db)
	key := summarycoord.Key{TenantID: tenantID, AgentAppID: appID, SessionOwnerID: "owner", SessionID: "same-session", SessionIncarnationID: uuid.NewString()}
	enqueue := func(target int64) summarycoord.Job {
		t.Helper()
		result, err := store.Enqueue(ctx, summarycoord.EnqueueRequest{Key: key, AgentVersionID: versionID, TargetEventSequence: target})
		if err != nil {
			t.Fatal(err)
		}
		return result.Job
	}
	claim := func(t *testing.T) summarycoord.Job {
		t.Helper()
		job, err := store.Claim(ctx, "summary-test", time.Minute)
		if err != nil || job.TenantID != tenantID {
			t.Fatalf("claim test job: %#v err=%v", job, err)
		}
		return job
	}
	complete := func(job summarycoord.Job, target int64, want summarycoord.Status) {
		t.Helper()
		got, err := store.Complete(ctx, job, target)
		if err != nil || got.Status != want || got.CompletedEventSequence != target {
			t.Fatalf("complete target %d: %#v err=%v", target, got, err)
		}
	}
	first := enqueue(998)
	complete(claim(t), 998, summarycoord.StatusCompleted)
	if deferred := enqueue(0); deferred.ID != first.ID || deferred.Status != summarycoord.StatusPending {
		t.Fatalf("deferred enqueue did not wake completed job: %#v", deferred)
	}
	working := claim(t)
	// Concurrent arrivals after the transcript read must survive this claim's
	// resolution and coalesce into one later pass.
	var group sync.WaitGroup
	failures := make(chan error, 12)
	for range cap(failures) {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := peer.Enqueue(ctx, summarycoord.EnqueueRequest{Key: key, AgentVersionID: versionID})
			failures <- err
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	resolved, err := store.ResolveTarget(ctx, working, 1100)
	if err != nil || resolved.TargetEventSequence != 1100 || resolved.TargetResolutionLeaseVersion <= working.LeaseVersion {
		t.Fatalf("concurrent deferred request lost: %#v err=%v", resolved, err)
	}
	complete(resolved, 1100, summarycoord.StatusPending)
	working = claim(t)
	resolved, err = store.ResolveTarget(ctx, working, 1200)
	if err != nil || resolved.TargetEventSequence != 1200 || resolved.TargetResolutionLeaseVersion != 0 {
		t.Fatalf("next claim did not resolve the pending prefix: %#v err=%v", resolved, err)
	}
	complete(resolved, 1200, summarycoord.StatusCompleted)
	if _, err := store.Claim(ctx, "summary-test", time.Minute); !errors.Is(err, summarycoord.ErrNoWork) {
		t.Fatalf("deferred requests did not drain: %v", err)
	}

	// An old generator may still hold its own valid lease after Session expiry.
	// Publication remains bound to that generation and cannot hydrate its replacement.
	enqueue(1300)
	oldClaim := claim(t)
	oldKey := key
	key.SessionIncarnationID = uuid.NewString()
	newJob := enqueue(2)
	if newJob.ID == first.ID || newJob.TargetEventSequence != 2 {
		t.Fatalf("new Session generation reused old job: %#v", newJob)
	}
	newClaim := claim(t)
	candidate := func(scope summarycoord.Key, target int64, text string) summarycoord.Candidate {
		return summarycoord.Candidate{Key: scope, EventSequence: target, Content: text,
			ContentSHA256: summarycoord.HashContent(text), CutoffAt: time.Now().UTC(), LastEventID: uuid.NewString()}
	}
	newCandidate := candidate(key, 2, "new conversation")
	if _, err := sink.PublishFenced(ctx, newCandidate, oldClaim); !errors.Is(err, summarycoord.ErrSummaryScope) {
		t.Fatalf("old lease published into replacement generation: %v", err)
	}
	if _, err := sink.PublishFenced(ctx, newCandidate, newClaim); err != nil {
		t.Fatal(err)
	}
	complete(newClaim, 2, summarycoord.StatusCompleted)
	if _, err := sink.PublishFenced(ctx, candidate(oldKey, 1300, "old conversation"), oldClaim); err != nil {
		t.Fatal(err)
	}
	complete(oldClaim, 1300, summarycoord.StatusCompleted)
	for scope, want := range map[summarycoord.Key]string{oldKey: "old conversation", key: "new conversation"} {
		checkpoint, found, err := sink.Get(ctx, scope)
		if err != nil || !found || checkpoint.Key != scope || checkpoint.Content != want {
			t.Fatalf("generation checkpoint = %#v found=%v err=%v", checkpoint, found, err)
		}
	}
	for _, test := range []struct {
		name     string
		deferred bool
		expired  bool
	}{
		{name: "known target after final failure"},
		{name: "deferred target after final failure", deferred: true},
		{name: "known target after final lease expiry", expired: true},
		{name: "deferred target after final lease expiry", deferred: true, expired: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scope := key
			scope.SessionID = uuid.NewString()
			request := summarycoord.EnqueueRequest{Key: scope, AgentVersionID: versionID, TargetEventSequence: 4, MaxAttempts: 1}
			if _, err := store.Enqueue(ctx, request); err != nil {
				t.Fatal(err)
			}
			lastAttempt := claim(t)
			request.TargetEventSequence = 9
			if test.deferred {
				request.TargetEventSequence = 0
			}
			if _, err := peer.Enqueue(ctx, request); err != nil {
				t.Fatal(err)
			}
			if test.expired {
				if _, err := db.ExecContext(ctx, `UPDATE summary_jobs SET lease_until=clock_timestamp()-INTERVAL '1 second' WHERE id=$1`, lastAttempt.ID); err != nil {
					t.Fatal(err)
				}
			} else if _, err := store.Fail(ctx, lastAttempt, errors.New("model unavailable"), time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			fresh, err := peer.Claim(ctx, "replacement-worker", time.Minute)
			if err != nil || fresh.Key != scope || fresh.Attempts != 1 || fresh.LeaseVersion <= lastAttempt.LeaseVersion {
				t.Fatalf("new work inherited exhausted retry budget: %#v err=%v", fresh, err)
			}
			if test.deferred {
				fresh, err = peer.ResolveTarget(ctx, fresh, 9)
				if err != nil {
					t.Fatal(err)
				}
			}
			if finished, err := peer.Complete(ctx, fresh, 9); err != nil || finished.Status != summarycoord.StatusCompleted {
				t.Fatalf("new target did not complete: %#v err=%v", finished, err)
			}
		})
	}
}
