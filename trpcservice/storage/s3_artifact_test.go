package storage

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestParseS3ConfigRequiresBucketAndKeys(t *testing.T) {
	if _, err := ParseS3Config(`{"region":"us-east-1"}`); err == nil {
		t.Fatal("ParseS3Config() error = nil, want missing bucket/keys rejected")
	}
	if _, err := ParseS3Config(`{"bucket":"artifacts","access_key":"a","secret_key":"s","endpoint":"not-a-url"}`); err == nil {
		t.Fatal("ParseS3Config() error = nil, want invalid endpoint rejected")
	}
	configuration, err := ParseS3Config(`{"bucket":"artifacts","access_key":"a","secret_key":"s","endpoint":"http://127.0.0.1:9000"}`)
	if err != nil {
		t.Fatalf("ParseS3Config() error = %v", err)
	}
	if configuration.Region != "us-east-1" || !configuration.usePathStyle() {
		t.Fatalf("ParseS3Config() = %#v, want default region and path-style for custom endpoint", configuration)
	}
}

func TestParseCOSConfigRequiresBucketURLAndKeys(t *testing.T) {
	if _, err := ParseCOSConfig(`{"secret_id":"id","secret_key":"key"}`); err == nil {
		t.Fatal("ParseCOSConfig() error = nil, want missing bucket_url rejected")
	}
	if _, err := ParseCOSConfig(`{"bucket_url":"not-a-url","secret_id":"id","secret_key":"key"}`); err == nil {
		t.Fatal("ParseCOSConfig() error = nil, want invalid bucket_url rejected")
	}
	configuration, err := ParseCOSConfig(`{"bucket_url":"https://bucket.cos.ap-guangzhou.myqcloud.com","secret_id":"id","secret_key":"key"}`)
	if err != nil {
		t.Fatalf("ParseCOSConfig() error = %v", err)
	}
	if configuration.BucketURL != "https://bucket.cos.ap-guangzhou.myqcloud.com" {
		t.Fatalf("ParseCOSConfig() = %#v", configuration)
	}
}

func TestArtifactBackendsOpenRegisteredDriversAndRejectUnknown(t *testing.T) {
	backends := newArtifactBackends(&PostgresArtifactService{})
	names := backends.DriverNames()
	if len(names) < 3 || names[0] != ArtifactDriverPostgres || names[1] != ArtifactDriverS3 || names[2] != ArtifactDriverCOS {
		t.Fatalf("DriverNames() = %v, want postgres, s3, cos", names)
	}
	if _, err := backends.Open(context.Background(), "gcs", ""); err == nil {
		t.Fatal("Open(gcs) error = nil, want unsupported driver")
	}
	if _, err := backends.Open(context.Background(), ArtifactDriverS3, ""); err == nil {
		t.Fatal("Open(s3) error = nil, want missing connection")
	}
	service, err := backends.Open(context.Background(), "", "")
	if err != nil {
		t.Fatalf("Open(default) error = %v", err)
	}
	if _, err := service.SaveArtifact(context.Background(), agentartifact.SessionInfo{}, "invalid", nil); err == nil {
		t.Fatal("SaveArtifact(nil) error = nil, want platform artifact validation")
	}
}

func TestS3ArtifactStoreAgainstConfiguredServer(t *testing.T) {
	raw := os.Getenv("TEST_S3_CONFIG_JSON")
	if raw == "" {
		t.Skip("TEST_S3_CONFIG_JSON is not set")
	}
	service, err := openOfficialS3ArtifactService(context.Background(), raw)
	if err != nil {
		t.Fatalf("openOfficialS3ArtifactService() error = %v", err)
	}
	info := agentartifact.SessionInfo{AppName: "integration/storage", UserID: "integration", SessionID: "artifact-test"}
	filename := "official-" + time.Now().UTC().Format("150405.000")
	want := &agentartifact.Artifact{Data: []byte("s3-official"), MimeType: "text/plain"}
	version, err := service.SaveArtifact(context.Background(), info, filename, want)
	if err != nil {
		t.Fatalf("SaveArtifact() error = %v", err)
	}
	t.Cleanup(func() { _ = service.DeleteArtifact(context.Background(), info, filename) })
	got, err := service.LoadArtifact(context.Background(), info, filename, &version)
	if err != nil {
		t.Fatalf("LoadArtifact() error = %v", err)
	}
	if got == nil || string(got.Data) != "s3-official" {
		t.Fatalf("LoadArtifact() = %#v, want %q", got, want.Data)
	}
}

func TestParseS3ConfigRoundTripJSON(t *testing.T) {
	raw := `{"endpoint":"http://127.0.0.1:9000","region":"us-east-1","bucket":"trpc-artifacts","access_key":"key","secret_key":"secret","path_style":false}`
	configuration, err := ParseS3Config(raw)
	if err != nil {
		t.Fatalf("ParseS3Config() error = %v", err)
	}
	if configuration.usePathStyle() {
		t.Fatal("path_style=false should disable path-style addressing")
	}
	encoded, err := json.Marshal(configuration)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if _, err := ParseS3Config(string(encoded)); err != nil {
		t.Fatalf("ParseS3Config(round-trip) error = %v", err)
	}
}
