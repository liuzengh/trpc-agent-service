package admin

import (
	"context"
	"errors"
	"testing"
)

// failingStateStore is a coordination.StateStore whose reads always fail —
// the shape of a Redis outage at boot.
type failingStateStore struct{}

func (failingStateStore) Get(context.Context, string) (string, error) {
	return "", errors.New("redis down")
}

func (failingStateStore) Set(context.Context, string, string) error { return nil }

func (failingStateStore) Delete(context.Context, string) error { return nil }

// TestLoadMarksUnreachableStore: a snapshot store that cannot be reached must
// be distinguishable from a snapshot that cannot be parsed, because boot
// treats them differently — fall back to the file config (and let the session
// probe name the real dependency, as D5 requires) versus refuse to start.
func TestLoadMarksUnreachableStore(t *testing.T) {
	_, err := NewRedisRuntimeStore(failingStateStore{}).Load(context.Background())
	if !errors.Is(err, ErrSnapshotUnavailable) {
		t.Fatalf("Load on a down store = %v, want ErrSnapshotUnavailable", err)
	}
}
