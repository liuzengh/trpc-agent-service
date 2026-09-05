package testinfra

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestObjectLabReadinessAndOwnership(t *testing.T) {
	if os.Getenv("OBJECT_S3_INTEGRATION") != "1" {
		t.Skip("set OBJECT_S3_INTEGRATION=1 to run the real MinIO readiness gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	lab := NewObjectLab(t, "caelumc")
	lab.Start(ctx)
	if lab.NetworkAlias() != "minio" || !lab.HostPortPublished() {
		t.Fatalf("object readiness failed stage=endpoint_reachability category=unexpected_test_topology")
	}
	lab.AssertOwnership(ctx)
	lab.CreateBucket(ctx)
	lab.RemoveBucket(ctx)
	lab.Cleanup()
	if lab.RemainingResources(ctx) {
		t.Fatalf("object cleanup failed stage=cleanup category=owner_resources_remain")
	}
}
