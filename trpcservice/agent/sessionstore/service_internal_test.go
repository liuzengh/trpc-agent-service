package sessionstore

import (
	"context"
	"errors"
	"testing"

	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
)

func TestObserveStoreRejectsInvalidInputs(t *testing.T) {
	if _, err := observeStore[struct{}](nil, context.Background(), func(context.Context) (struct{}, error) {
		return struct{}{}, nil
	}); !errors.Is(err, sessionstorage.ErrInvalid) {
		t.Fatalf("nil service error = %v", err)
	}
	service := &Service{}
	if _, err := observeStore[struct{}](service, context.Background(), nil); !errors.Is(err, sessionstorage.ErrInvalid) {
		t.Fatalf("nil operation error = %v", err)
	}
}
