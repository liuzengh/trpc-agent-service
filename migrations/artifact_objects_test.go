package migrations

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestArtifactObjectsEmbeddedLedger(t *testing.T) {
	body, err := ArtifactObjectsDDL()
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS artifact_blob_versions",
		"'upload_pending', 'ready', 'upload_failed', 'delete_pending', 'deleted'",
		"CREATE POLICY tenant_isolation ON artifact_blob_versions",
		"artifact_blob_versions_recovery_idx",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("embedded artifact migration is missing %q", required)
		}
	}
	decoded, err := hex.DecodeString(ArtifactObjectsChecksum())
	if err != nil || len(decoded) != 32 {
		t.Fatalf("artifact migration checksum is not SHA-256: %v", err)
	}
}

func TestVerifyArtifactObjectsAppliedAndMismatch(t *testing.T) {
	q := &verifierQuerier{row: verifierRow{checksum: ArtifactObjectsChecksum()}}
	if err := VerifyArtifactObjects(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.version != ArtifactObjectsVersion {
		t.Fatalf("queried version = %v", q.version)
	}
	err := VerifyArtifactObjects(context.Background(), &verifierQuerier{row: verifierRow{checksum: strings.Repeat("0", 64)}})
	if !errors.Is(err, ErrArtifactObjectsChecksumMismatch) {
		t.Fatalf("checksum mismatch = %v", err)
	}
	err = VerifyArtifactObjects(context.Background(), &verifierQuerier{row: verifierRow{err: pgx.ErrNoRows}})
	if !errors.Is(err, ErrArtifactObjectsNotApplied) {
		t.Fatalf("not applied = %v", err)
	}
}

func TestApplyArtifactObjectsRejectsNilTransaction(t *testing.T) {
	if err := ApplyArtifactObjects(context.Background(), nil); err == nil {
		t.Fatal("nil transaction was accepted")
	}
}
