//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
)

type recoveryObjectStore struct {
	bodies          map[string][]byte
	failNextDeletes int
}

func (s *recoveryObjectStore) Put(_ context.Context, key, _ string, body []byte) error {
	s.bodies[key] = append([]byte(nil), body...)
	return nil
}

func (s *recoveryObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	body, ok := s.bodies[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	return append([]byte(nil), body...), nil
}

func (s *recoveryObjectStore) Delete(_ context.Context, key string) error {
	if s.failNextDeletes > 0 {
		s.failNextDeletes--
		return errors.New("injected object deletion failure")
	}
	delete(s.bodies, key)
	return nil
}

func TestArtifactDeletionResumesAfterCommittedTombstone(t *testing.T) {
	db := openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tenantID := "artifact-recovery-" + uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'artifact recovery test','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM artifact_versions WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})
	objects := &recoveryObjectStore{bodies: make(map[string][]byte)}
	service, err := artifactplane.NewService(tenantID, db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner", SessionID: "session"}
	for _, body := range []string{"first", "second"} {
		if _, err := service.SaveArtifact(ctx, info, "report.txt", &artifact.Artifact{Data: []byte(body), MimeType: "text/plain"}); err != nil {
			t.Fatal(err)
		}
	}
	objects.failNextDeletes = 1
	if err := service.DeleteArtifact(ctx, info, "report.txt"); err == nil {
		t.Fatal("first delete must report the injected failure")
	}
	if len(objects.bodies) != 1 {
		t.Fatalf("remaining bodies = %d, want only the failed object", len(objects.bodies))
	}
	if loaded, err := service.LoadArtifact(ctx, info, "report.txt", nil); err != nil || loaded != nil {
		t.Fatalf("tombstoned artifact is still visible: artifact=%#v error=%v", loaded, err)
	}
	if err := service.DeleteArtifact(ctx, info, "report.txt"); err != nil {
		t.Fatalf("retry deletion: %v", err)
	}
	if len(objects.bodies) != 0 {
		t.Fatal("retry did not delete the committed tombstone's object body")
	}
}
