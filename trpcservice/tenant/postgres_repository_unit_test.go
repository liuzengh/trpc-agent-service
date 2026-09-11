package tenant

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

type rowScannerFunc func(...any) error

func (f rowScannerFunc) Scan(dest ...any) error { return f(dest...) }

func TestPostgresRepositoryDefaultCIValidationBoundaries(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresRepository(nil); err == nil {
		t.Fatal("NewPostgresRepository(nil) error = nil")
	}
	database, err := sql.Open("pgx", "postgres://unused:unused@127.0.0.1:1/unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(database)
	if err != nil {
		t.Fatal(err)
	}
	if !repository.UsesConfigCacheInvalidationOutbox() {
		t.Fatal("UsesConfigCacheInvalidationOutbox() = false")
	}
	if _, err := repository.beginTenantTransaction(context.Background(), " "); err == nil {
		t.Fatal("beginTenantTransaction() accepted blank tenant")
	}
	if _, err := repository.GetVersion(context.Background(), "tenant-a", "support", 0); err == nil {
		t.Fatal("GetVersion() accepted zero version")
	}
	if _, err := repository.ResolveBinding(context.Background(), channels.Channel("unknown"), "binding"); err == nil {
		t.Fatal("ResolveBinding() accepted unsupported channel")
	}
	if _, err := repository.ResolveBinding(context.Background(), channels.Telegram, " "); err == nil {
		t.Fatal("ResolveBinding() accepted blank binding")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repository.ListApplications(cancelled, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListApplications(cancelled) error = %v", err)
	}
	if _, err := repository.Publish(cancelled, config.TenantConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish(cancelled) error = %v", err)
	}
}

func TestPostgresRepositoryPureSerializationHelpers(t *testing.T) {
	t.Parallel()
	encoded, err := marshalChannelAllowlist(nil)
	if err != nil || string(encoded) != "[]" {
		t.Fatalf("marshalChannelAllowlist(nil) = %s, %v", encoded, err)
	}
	encoded, err = marshalChannelAllowlist([]string{"user-2", "user-1"})
	if err != nil || string(encoded) != `["user-2","user-1"]` {
		t.Fatalf("marshalChannelAllowlist(values) = %s, %v", encoded, err)
	}

	configuration := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 7,
		Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "support-bot"}},
	}
	publishedAt := time.Date(2026, 9, 11, 5, 0, 0, 0, time.UTC)
	snapshot, err := newSnapshot(configuration, publishedAt)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := marshalConfig(configuration)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeSnapshot(7, payload, []byte(snapshot.Checksum), publishedAt)
	if err != nil || decoded.Checksum != snapshot.Checksum {
		t.Fatalf("decodeSnapshot() = %#v, %v", decoded, err)
	}
	if _, err := decodeSnapshot(8, payload, []byte(snapshot.Checksum), publishedAt); err == nil || !strings.Contains(err.Error(), "version mismatch") {
		t.Fatalf("version mismatch error = %v", err)
	}
	if _, err := decodeSnapshot(7, []byte("not-json"), []byte(snapshot.Checksum), publishedAt); err == nil || !strings.Contains(err.Error(), "decode tenant configuration JSON") {
		t.Fatalf("invalid JSON error = %v", err)
	}
}

func TestPostgresRepositoryScannersClassifyRowsAndDecodeRollout(t *testing.T) {
	t.Parallel()
	if _, err := scanSnapshot(rowScannerFunc(func(...any) error { return sql.ErrNoRows })); !errors.Is(err, ErrNotFound) {
		t.Fatalf("scanSnapshot(no rows) error = %v", err)
	}
	wantErr := errors.New("scan failed")
	if _, err := scanSnapshot(rowScannerFunc(func(...any) error { return wantErr })); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "scan tenant configuration") {
		t.Fatalf("scanSnapshot(scan error) = %v", err)
	}
	if _, err := scanRollout(rowScannerFunc(func(...any) error { return sql.ErrNoRows })); !errors.Is(err, ErrRolloutNotFound) {
		t.Fatalf("scanRollout(no rows) error = %v", err)
	}
	now := time.Now().UTC()
	rollout, err := scanRollout(rowScannerFunc(func(dest ...any) error {
		*(dest[0].(*string)) = "tenant-a"
		*(dest[1].(*string)) = "support"
		*(dest[2].(*uint64)) = 4
		*(dest[3].(*uint64)) = 7
		*(dest[4].(*uint64)) = 8
		*(dest[5].(*int)) = 2500
		*(dest[6].(*[]byte)) = []byte(`["user-1","user-2"]`)
		*(dest[7].(*[]byte)) = []byte(`["web/web-console","telegram/support-bot"]`)
		*(dest[8].(*time.Time)) = now
		return nil
	}))
	if err != nil || rollout.TenantID != "tenant-a" || rollout.Generation != 4 || len(rollout.TestUserIDs) != 2 || len(rollout.Ingresses) != 2 {
		t.Fatalf("scanRollout() = %#v, %v", rollout, err)
	}
	if _, err := scanRollout(rowScannerFunc(func(dest ...any) error {
		*(dest[0].(*string)) = "tenant-a"
		*(dest[1].(*string)) = "support"
		*(dest[2].(*uint64)) = 1
		*(dest[3].(*uint64)) = 1
		*(dest[4].(*uint64)) = 2
		*(dest[5].(*int)) = 100
		*(dest[6].(*[]byte)) = []byte(`not-json`)
		*(dest[7].(*[]byte)) = []byte(`[]`)
		*(dest[8].(*time.Time)) = now
		return nil
	})); err == nil || !strings.Contains(err.Error(), "decode rollout test users") {
		t.Fatalf("bad users rollout error = %v", err)
	}
}
