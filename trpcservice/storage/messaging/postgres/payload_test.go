package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestPayloadEncryptionRoundTripAndAADBinding(t *testing.T) {
	key := bytes.Repeat([]byte{0x3c}, 32)
	aad := []byte("tenant\x00request\x00ref\x00digest")
	plaintext := []byte(`{"text":"secret"}`)
	ciphertext, nonce, err := encryptPayload(key, aad, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, plaintext) {
		t.Fatal("ciphertext contains plaintext")
	}
	actual, err := decryptPayload(key, aad, ciphertext, nonce)
	if err != nil || !bytes.Equal(actual, plaintext) {
		t.Fatalf("actual=%q err=%v", actual, err)
	}
	if _, err := decryptPayload(key, []byte("other scope"), ciphertext, nonce); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("wrong AAD err=%v", err)
	}
}

func TestStructuredContentTypeIsBoundIntoPayloadAAD(t *testing.T) {
	key := bytes.Repeat([]byte{0x3c}, 32)
	card := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: strings.Repeat("a", 64), ContentType: messaging.ContentTypeCard}
	ciphertext, nonce, err := encryptPayload(key, resultAAD(card), []byte(`{"header":{"title":"private"}}`))
	if err != nil {
		t.Fatal(err)
	}
	card.ContentType = messaging.ContentTypeText
	if _, err := decryptPayload(key, resultAAD(card), ciphertext, nonce); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("mutated content type err=%v", err)
	}
}

func TestTenantScopedPayloadKeysPostgreSQL16(t *testing.T) {
	db := openPayloadContractDB(t)
	ctx := context.Background()
	const tenantA = "t_01ARZ3NDEKTSV4RRFFQ69G5FAH"
	const tenantB = "t_01ARZ3NDEKTSV4RRFFQ69G5FAJ"
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES
($1,'payload-key-a','Payload Key A'),($2,'payload-key-b','Payload Key B')`, tenantA, tenantB); err != nil {
		t.Fatal(err)
	}
	resolver := keyedPayloadResolver{keys: map[string]map[int64][]byte{
		tenantA: {3: bytes.Repeat([]byte{0x31}, 32)},
		tenantB: {5: bytes.Repeat([]byte{0x52}, 32)},
	}}
	store := NewWithPayloadKeyResolver(db, resolver)
	records := []messaging.PayloadRecord{
		{TenantID: tenantA, RequestID: "request-a", PayloadRef: "inbound://tenant-a/request-a", ContentDigest: strings.Repeat("a", 64), Content: []byte("tenant-a-content"), KeyVersion: 3},
		{TenantID: tenantB, RequestID: "request-b", PayloadRef: "inbound://tenant-b/request-b", ContentDigest: strings.Repeat("b", 64), Content: []byte("tenant-b-content"), KeyVersion: 5},
	}
	for _, record := range records {
		if err := store.PutPayload(ctx, record); err != nil {
			t.Fatal(err)
		}
		stored, err := store.GetPayload(ctx, record.TenantID, record.RequestID)
		if err != nil || stored.KeyVersion != record.KeyVersion || !bytes.Equal(stored.Content, record.Content) {
			t.Fatalf("tenant=%q version=%d content_len=%d err=%v", record.TenantID, stored.KeyVersion, len(stored.Content), err)
		}
		var ciphertext []byte
		if err := db.QueryRowContext(ctx, `SELECT payload_ciphertext FROM inbound_payload WHERE tenant_id=$1 AND request_id=$2`, record.TenantID, record.RequestID).Scan(&ciphertext); err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(ciphertext, record.Content) {
			t.Fatalf("tenant=%q ciphertext contains plaintext", record.TenantID)
		}
	}

	wrong := NewWithPayloadKeyResolver(db, keyedPayloadResolver{keys: map[string]map[int64][]byte{
		tenantA: {3: resolver.keys[tenantB][5]},
	}})
	if _, err := wrong.GetPayload(ctx, tenantA, "request-a"); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("cross-tenant key substitution err=%v", err)
	}
}

func TestPostgreSQLPayloadStoreWithoutKeyFailsClosed(t *testing.T) {
	store := New(nil)
	err := store.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: "tenant", RequestID: "request", PayloadRef: "payload://request", ContentDigest: string(bytes.Repeat([]byte{'a'}, 64)), Content: []byte("payload"), KeyVersion: 1})
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestPostgreSQLPayloadStoreResolvesExactTenantGeneration(t *testing.T) {
	resolver := &recordingPayloadKeyResolver{value: messaging.PayloadCipherKey{Bytes: bytes.Repeat([]byte{0x31}, 32), Version: 7}}
	store := NewWithPayloadKeyResolver(nil, resolver)
	key, err := store.resolvePayloadKey(context.Background(), "tenant-a", 7)
	if err != nil || resolver.tenantID != "tenant-a" || resolver.version != 7 || len(key) != 32 {
		t.Fatalf("tenant=%q version=%d key_len=%d err=%v", resolver.tenantID, resolver.version, len(key), err)
	}
	clear(key)

	resolver.value.Version = 8
	if _, err := store.resolvePayloadKey(context.Background(), "tenant-a", 7); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("version drift err=%v", err)
	}
	resolver.value = messaging.PayloadCipherKey{Bytes: bytes.Repeat([]byte{0x31}, 16), Version: 7}
	if _, err := store.resolvePayloadKey(context.Background(), "tenant-a", 7); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("invalid key material err=%v", err)
	}
	if _, err := store.resolvePayloadKey(context.Background(), "tenant-a", 0); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("unversioned key err=%v", err)
	}
}

type recordingPayloadKeyResolver struct {
	tenantID string
	version  int64
	value    messaging.PayloadCipherKey
}

func (r *recordingPayloadKeyResolver) ResolvePayloadKey(_ context.Context, tenantID string, version int64) (messaging.PayloadCipherKey, error) {
	r.tenantID, r.version = tenantID, version
	return messaging.PayloadCipherKey{Bytes: append([]byte(nil), r.value.Bytes...), Version: r.value.Version}, nil
}

type keyedPayloadResolver struct {
	keys map[string]map[int64][]byte
}

func (r keyedPayloadResolver) ResolvePayloadKey(_ context.Context, tenantID string, version int64) (messaging.PayloadCipherKey, error) {
	versions, ok := r.keys[tenantID]
	if !ok {
		return messaging.PayloadCipherKey{}, runtime.ErrTenantScope
	}
	key, ok := versions[version]
	if !ok {
		return messaging.PayloadCipherKey{}, runtime.ErrNotFound
	}
	return messaging.PayloadCipherKey{Bytes: append([]byte(nil), key...), Version: version}, nil
}

func openPayloadContractDB(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var serverMajor int
	if err := db.QueryRowContext(context.Background(), `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &serverMajor); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || serverMajor != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, serverMajor)
	}
	return db
}
