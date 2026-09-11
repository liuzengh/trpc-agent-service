package leaseheartbeat

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHeartbeatRenewsValueAndStops(t *testing.T) {
	renewed := make(chan struct{}, 1)
	heartbeat := Start(context.Background(), "test lease heartbeat", 1, time.Millisecond, time.Second,
		func(context.Context, int) (int, error) {
			select {
			case renewed <- struct{}{}:
			default:
			}
			return 2, nil
		})
	t.Cleanup(heartbeat.Stop)

	select {
	case <-renewed:
	case <-time.After(time.Second):
		t.Fatal("lease was not renewed")
	}
	deadline := time.Now().Add(time.Second)
	for heartbeat.Value() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("heartbeat value = %d, want 2", heartbeat.Value())
		}
		time.Sleep(time.Millisecond)
	}
	if err := heartbeat.Err(); err != nil {
		t.Fatalf("heartbeat error = %v", err)
	}
}

func TestHeartbeatCancelsOnRenewFailure(t *testing.T) {
	wantErr := errors.New("lease lost")
	heartbeat := Start(context.Background(), "test lease heartbeat", struct{}{}, time.Millisecond, time.Second,
		func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, wantErr
		})
	t.Cleanup(heartbeat.Stop)

	select {
	case <-heartbeat.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat context was not cancelled")
	}
	if !errors.Is(heartbeat.Err(), wantErr) {
		t.Fatalf("heartbeat error = %v, want %v", heartbeat.Err(), wantErr)
	}
}
