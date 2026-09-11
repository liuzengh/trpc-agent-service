package datamigration

import (
	"context"
	"strings"
	"testing"
)

func TestPutObjectValidatesBeforeDatabaseAccess(t *testing.T) {
	store := &PostgresStore{}
	record := ObjectRecord{MigrationID: "m", ObjectKey: "k", SourceHash: strings.Repeat("a", 64), Status: ObjectCopied}
	if err := store.PutObject(context.Background(), record); err == nil {
		t.Fatal("store without pool accepted object")
	}
	record.SourceHash = "not-a-hash"
	if err := store.PutObject(context.Background(), record); err == nil {
		t.Fatal("invalid object hash was accepted")
	}
}
