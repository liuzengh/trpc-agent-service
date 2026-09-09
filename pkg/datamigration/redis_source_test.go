package datamigration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisSourceSnapshotUsesVersionKeysetAndChanges(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	source := &RedisSource{Client: client, Prefix: "migration-test"}
	index := source.indexKey("tenant-a", DomainMemory)
	for _, item := range []struct {
		key string
		ver float64
		val string
	}{{"k1", 1, "one"}, {"k2", 2, "two"}, {"k3", 3, "three"}} {
		if err := client.ZAdd(context.Background(), index, redis.Z{Score: item.ver, Member: item.key}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.Set(context.Background(), source.VersionedValueKey("tenant-a", DomainMemory, item.key, int64(item.ver)), item.val, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	first, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.Watermark != 3 || first.NextCursor == "" || len(first.Records) != 2 {
		t.Fatalf("first snapshot=%#v", first)
	}
	second, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, first.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Done || second.NextCursor != "" || len(second.Records) != 1 || second.Records[0].Key != "k3" {
		t.Fatalf("second snapshot=%#v", second)
	}
	changes, err := source.Changes(context.Background(), "tenant-a", DomainMemory, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if changes.Done || changes.NextCursor != "2" || changes.Watermark != 2 || len(changes.Records) != 2 {
		t.Fatalf("changes=%#v", changes)
	}
	final, err := source.Changes(context.Background(), "tenant-a", DomainMemory, changes.Watermark, 2)
	if err != nil || !final.Done || final.Watermark != 3 || len(final.Records) != 1 {
		t.Fatalf("final changes=%#v err=%v", final, err)
	}
}

func TestRedisSourceRejectsMissingValueAndInvalidCursor(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	source := &RedisSource{Client: client, Prefix: "migration-test"}
	index := source.indexKey("tenant-a", DomainMemory)
	if err := client.ZAdd(context.Background(), index, redis.Z{Score: 1, Member: "gone"}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, "", 1); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("missing value error=%v, want invalid record", err)
	}
	if _, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, "%%%", 1); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("invalid cursor error=%v, want invalid record", err)
	}
}

func TestRedisSourceRejectsDuplicateAndInexactVersionsAtPageBoundary(t *testing.T) {
	for _, test := range []struct {
		name   string
		scores []float64
	}{
		{name: "duplicate", scores: []float64{1, 1}},
		{name: "inexact", scores: []float64{1, float64(1 << 53)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			mr, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			defer mr.Close()
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer client.Close()
			source := &RedisSource{Client: client, Prefix: "migration-test"}
			for i, score := range test.scores {
				key := fmt.Sprintf("k%d", i)
				if err := client.ZAdd(context.Background(), source.indexKey("tenant-a", DomainMemory), redis.Z{Score: score, Member: key}).Err(); err != nil {
					t.Fatal(err)
				}
				if err := client.Set(context.Background(), source.VersionedValueKey("tenant-a", DomainMemory, key, int64(score)), key, 0).Err(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, "", 1); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("Snapshot error=%v, want ErrInvalidRecord", err)
			}
		})
	}
}

func TestRedisSourceRejectsZeroVersion(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	source := &RedisSource{Client: client, Prefix: "migration-test"}
	if err := client.ZAdd(context.Background(), source.indexKey("tenant-a", DomainMemory), redis.Z{Score: 0, Member: "zero"}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, "", 1); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("zero version error=%v, want ErrInvalidRecord", err)
	}
}

func TestRedisSourceRejectsOversizedPayloadBeforeReturningBatch(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	source := &RedisSource{Client: client, Prefix: "migration-test"}
	key := "oversized"
	if err := client.ZAdd(context.Background(), source.indexKey("tenant-a", DomainMemory), redis.Z{Score: 1, Member: key}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), source.VersionedValueKey("tenant-a", DomainMemory, key, 1), strings.Repeat("x", maxRecordPayloadBytes+1), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, "", 1); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("oversized payload error=%v, want ErrInvalidRecord", err)
	}
}

func TestRedisSourceEmitsVersionedTombstone(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	source := &RedisSource{Client: client, Prefix: "migration-test"}
	key := "deleted"
	if err := client.ZAdd(context.Background(), source.indexKey("tenant-a", DomainMemory), redis.Z{Score: 4, Member: key}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), source.VersionedValueKey("tenant-a", DomainMemory, key, 4), []byte{}, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), source.VersionedDeletedKey("tenant-a", DomainMemory, key, 4), "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	batch, err := source.Snapshot(context.Background(), "tenant-a", DomainMemory, "", 1)
	if err != nil || len(batch.Records) != 1 || !batch.Records[0].Deleted || batch.Records[0].Payload == nil {
		t.Fatalf("tombstone batch=%#v err=%v", batch, err)
	}
}
