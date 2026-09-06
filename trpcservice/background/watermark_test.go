package background

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func watermarkContract(t *testing.T, a, b Watermarks) {
	t.Helper()
	ctx := context.Background()
	key := WatermarkKey{"tutorial-tenant", "tutorial-app", "watermark-user", "watermark-session", JobMemoryExtract}
	base := time.Now().UTC().Truncate(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value := base.Add(time.Duration(i) * time.Nanosecond)
			current, err := a.Advance(ctx, key, value)
			if err != nil || current.Before(value) {
				t.Errorf("advance moved backward or failed: %v", err)
			}
		}(i)
	}
	wg.Wait()
	want := base.Add(63 * time.Nanosecond)
	got, ok, err := b.Read(ctx, key)
	if err != nil || !ok || !got.Equal(want) {
		t.Fatalf("cross-instance watermark=%v want=%v err=%v", got, want, err)
	}
	got, err = b.Advance(ctx, key, base)
	if err != nil || !got.Equal(want) {
		t.Fatal("old job regressed watermark")
	}
	other := key
	other.TenantID = "other-tenant"
	if _, found, err := b.Read(ctx, other); err != nil || found {
		t.Fatal("watermark crossed tenant boundary")
	}
}

func TestWatermarkMonotonicConcurrentUpdates(t *testing.T) {
	store := NewWatermarks(controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData()))
	watermarkContract(t, store, store)
}

func TestSessionJobRejectsForgedScope(t *testing.T) {
	job := Job{TenantID: "tenant-a", AppID: "app-a", Type: JobMemoryExtract}
	for _, scope := range []string{"t/tenant-b/a/app-a", "t/tenant-a/a/app-b", "invalid"} {
		if _, err := watermarkKey(job, SessionJobPayload{StorageScope: scope, UserID: "user", SessionID: "session"}); err == nil {
			t.Fatal("forged background scope accepted")
		}
	}
}
