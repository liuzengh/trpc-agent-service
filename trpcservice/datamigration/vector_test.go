package datamigration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testVectorRecord(epoch string, sequence int64, content string) VectorRecord {
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
	return VectorRecord{
		FormatVersion: 1, TenantID: "tenant-a", AppName: "assistant", UserID: "user-a",
		SessionID: "session-a", SessionEpoch: epoch, FilterKey: "", ThroughSequence: sequence,
		SummaryVersion: 1, EmbeddingModel: "embed-v1", Dimension: 2,
		ContentSHA256: hash, Content: content, Embedding: []float64{0.1, 0.2},
	}
}

func TestVectorTargetRejectsOlderSummaryAndEpoch(t *testing.T) {
	target := NewMemoryVectorTarget()
	ctx := context.Background()
	newer := testVectorRecord("epoch-2", 4, "new")
	if result, err := target.Import(ctx, "migration-1", newer); err != nil || result.Status != ObjectCopied {
		t.Fatalf("new vector import = %#v, %v", result, err)
	}
	older := testVectorRecord("epoch-2", 3, "old")
	if result, err := target.Import(ctx, "migration-1", older); err != nil || result.Status != ObjectMismatch || result.Conflict != "stale_source" {
		t.Fatalf("older summary import = %#v, %v", result, err)
	}
	wrongEpoch := testVectorRecord("epoch-1", 9, "different-session")
	if result, err := target.Import(ctx, "migration-1", wrongEpoch); err != nil || result.Status != ObjectMismatch {
		t.Fatalf("wrong epoch import = %#v, %v", result, err)
	}
}

func TestVectorRollbackPreservesReplacement(t *testing.T) {
	target := NewMemoryVectorTarget()
	ctx := context.Background()
	record := testVectorRecord("epoch-1", 1, "first")
	if _, err := target.Import(ctx, "migration-1", record); err != nil {
		t.Fatal(err)
	}
	replacement := testVectorRecord("epoch-1", 2, "replacement")
	if _, err := target.Import(ctx, "migration-2", replacement); err != nil {
		t.Fatal(err)
	}
	deleted, conflict, err := target.Rollback(ctx, "migration-1", record)
	if err != nil || deleted || !conflict {
		t.Fatalf("rollback replaced object = deleted:%v conflict:%v err:%v", deleted, conflict, err)
	}
	if result, err := target.Verify(ctx, replacement); err != nil || result.Status != ObjectVerified {
		t.Fatalf("replacement after rollback = %#v, %v", result, err)
	}
}

func TestJSONLVectorSourceResumesByLineCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vectors.jsonl")
	first := testVectorRecord("epoch", 1, "one")
	second := testVectorRecord("epoch", 2, "two")
	encoded1, _ := json.Marshal(first)
	encoded2, _ := json.Marshal(second)
	if err := os.WriteFile(path, []byte(string(encoded1)+"\n"+string(encoded2)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := NewJSONLVectorSource(path)
	if err != nil {
		t.Fatal(err)
	}
	batch, cursor, done, err := source.Scan(context.Background(), "", 1)
	if err != nil || len(batch) != 1 || cursor != "1" || done {
		t.Fatalf("first vector batch = %#v cursor=%q done=%v err=%v", batch, cursor, done, err)
	}
	batch, cursor, done, err = source.Scan(context.Background(), cursor, 1)
	if err != nil || len(batch) != 1 || batch[0].Content != "two" || cursor != "2" || done {
		t.Fatalf("resumed vector batch = %#v cursor=%q done=%v err=%v", batch, cursor, done, err)
	}
	batch, cursor, done, err = source.Scan(context.Background(), cursor, 1)
	if err != nil || len(batch) != 0 || !done || cursor != "2" {
		t.Fatalf("final vector batch = %#v cursor=%q done=%v err=%v", batch, cursor, done, err)
	}
}

func TestVectorObjectHashIsStableAndValidated(t *testing.T) {
	record := testVectorRecord("epoch", 1, "content")
	first, err := VectorObjectHash(record)
	if err != nil || len(first) != 64 {
		t.Fatalf("VectorObjectHash() = %q, %v", first, err)
	}
	record.Content = strings.TrimSpace(record.Content) + " changed"
	if _, err := VectorObjectHash(record); err == nil {
		t.Fatal("content hash mismatch was accepted")
	}
}
