package metrics

import (
	"context"
	"testing"
	"time"
)

func TestRecorderSmoke(t *testing.T) {
	recorder, err := New()
	if err != nil {
		t.Fatalf("new recorder: %v", err)
	}
	recorder.RecordInbound(context.Background(), "tenant-a", "wecom", false)
	recorder.RecordRun(context.Background(), "tenant-a", "completed", time.Millisecond)
	recorder.RecordUsage(context.Background(), "tenant-a", 12, 8, 0.0001)
	recorder.RecordDelivery(context.Background(), "tenant-a", "wecom", "sent", time.Millisecond)
}
