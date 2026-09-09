package s3object

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestNewRejectsIncompleteOrUnsafeConfig(t *testing.T) {
	cases := []Config{
		{},
		{Endpoint: "object:9000", Region: "us-east-1", Bucket: "bucket", AccessKey: "access", SecretKey: "secret"},
		{Endpoint: "http://object:9000/path", Region: "us-east-1", Bucket: "bucket", AccessKey: "access", SecretKey: "secret"},
		{Endpoint: "http://user:secret@object:9000", Region: "us-east-1", Bucket: "bucket", AccessKey: "access", SecretKey: "secret"},
		{Endpoint: "http://object:9000?credential=secret", Region: "us-east-1", Bucket: "bucket", AccessKey: "access", SecretKey: "secret"},
		{Endpoint: "http://object:9000", Region: "us-east-1", Bucket: "Bad_Bucket", AccessKey: "access", SecretKey: "secret"},
	}
	for index, cfg := range cases {
		if _, err := New(cfg); err == nil {
			t.Errorf("invalid config case %d was accepted", index)
		}
	}
}

func TestVerifiedObjectReaderChecksBytes(t *testing.T) {
	body := []byte("verified object bytes")
	digest := sha256.Sum256(body)
	info := storage.ObjectInfo{SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
	reader := newVerifiedObjectReader(io.NopCloser(bytes.NewReader(body)), info)
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatal("verified reader rejected matching bytes")
	}
	if err := reader.Close(); err != nil {
		t.Fatal("verified reader close failed")
	}
}

func TestVerifiedObjectReaderRejectsMismatch(t *testing.T) {
	body := []byte("verified object bytes")
	digest := sha256.Sum256(body)
	checksumReader := newVerifiedObjectReader(io.NopCloser(bytes.NewReader(body)), storage.ObjectInfo{SizeBytes: int64(len(body)), SHA256: strings.Repeat("0", 64)})
	if _, err := io.ReadAll(checksumReader); !errors.Is(err, storage.ErrObjectChecksum) {
		t.Fatal("verified reader accepted checksum mismatch")
	}
	sizeReader := newVerifiedObjectReader(io.NopCloser(bytes.NewReader(body)), storage.ObjectInfo{SizeBytes: int64(len(body) + 1), SHA256: hex.EncodeToString(digest[:])})
	if _, err := io.ReadAll(sizeReader); !errors.Is(err, storage.ErrObjectInvalid) {
		t.Fatal("verified reader accepted size mismatch")
	}
}

func TestConfigDefaultsAndBounds(t *testing.T) {
	store, err := New(Config{Endpoint: "http://object:9000", Region: "us-east-1", Bucket: "bucket", AccessKey: "access", SecretKey: "secret"})
	if err != nil {
		t.Fatalf("default configuration was rejected")
	}
	if store.maxObjectBytes <= 0 || store.presignMaxTTL != DefaultPresignTTL {
		t.Fatalf("default configuration values were not applied")
	}
	if _, err := New(Config{Endpoint: "http://object:9000", Region: "us-east-1", Bucket: "bucket", AccessKey: "access", SecretKey: "secret", PresignMaxTTL: 8 * 24 * time.Hour}); err == nil {
		t.Fatal("accepted presign TTL above provider bound")
	}
}
