package memorystore

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestMigrationCandidatePreservesRevisionAndCanonicalDigest(t *testing.T) {
	scope := Scope{TenantID: "tenant", ID: "scope"}
	for _, revision := range []uint64{0, 1, 9} {
		snapshot := Snapshot{Revision: revision, Entries: nil}
		candidate, body, hash, err := migrationCandidate(scope, snapshot, 1024)
		if err != nil {
			t.Fatal(err)
		}
		wantBase := uint64(0)
		if revision > 0 {
			wantBase = revision - 1
		}
		if candidate.BaseRevision != wantBase || digest(body) != hash {
			t.Fatal(revision, candidate, hash)
		}
		if verified, err := SnapshotDigest(scope, snapshot); err != nil || verified != hash {
			t.Fatal(verified, err)
		}
	}
	if _, _, _, err := migrationCandidate(Scope{}, Snapshot{}, 1024); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	invalid := Snapshot{Entries: []*memory.Entry{{}}}
	if _, err := SnapshotDigest(scope, invalid); !errors.Is(err, ErrIdentity) {
		t.Fatal("revision zero digest with content", err)
	}
	if _, _, _, err := migrationCandidate(scope, invalid, 1024); !errors.Is(err, ErrIdentity) {
		t.Fatal("revision zero with content", err)
	}
	if _, _, _, err := migrationCandidate(scope, Snapshot{}, 1); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	redis := &Redis{}
	if err := redis.ImportSnapshot(canceled, scope, Snapshot{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
