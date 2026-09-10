package skill_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	skillmemory "github.com/liuzengh/trpc-agent-service/trpcservice/skill/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/inmemory"
)

func TestIngestorStagesScannedArtifactAndPublishesExactDigest(t *testing.T) {
	ctx := context.Background()
	artifacts := artifactmemory.New()
	content := []byte("---\nname: writer\ndescription: test\n---\nbody\n")
	source := sha256.Sum256([]byte("source"))
	id, ref, err := artifact.StableIdentity("tenant", "request", 0, hex.EncodeToString(source[:]))
	if err != nil {
		t.Fatal(err)
	}
	contentSum := sha256.Sum256(content)
	if _, err = artifacts.PutArtifact(ctx, artifact.Record{TenantID: "tenant", RequestID: "request", ArtifactID: id, ArtifactRef: ref, Ordinal: 0, SourceDigest: hex.EncodeToString(source[:]), ContentDigest: hex.EncodeToString(contentSum[:]), MediaType: "text/plain", Kind: "file", Content: content, MalwareScanVersion: "av", DLPVersion: "dlp"}); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	catalog := skillmemory.New()
	in := skill.Ingestor{Catalog: catalog, Stager: skill.ArtifactStager{Artifacts: artifacts, StagingRoot: root}}
	pkg, err := in.Ingest(ctx, skill.StageRequest{TenantID: "tenant", SkillID: "writer", Version: 1, ArtifactID: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Resolve(ctx, "tenant", "writer", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "tenant", filepath.FromSlash(pkg.RelativePath), "writer", "SKILL.md")); err != nil {
		t.Fatal(err)
	}
}
