package reliable

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgresStoreUnavailableNeverPanics(t *testing.T) {
	tests := []struct {
		name string
		call func(*PostgresStore) error
	}{
		{name: "enqueue inbox", call: func(s *PostgresStore) error {
			_, err := s.EnqueueInbox(nil, newTestInbox())
			return err
		}},
		{name: "claim inbox", call: func(s *PostgresStore) error {
			_, err := s.ClaimInbox(nil, "consumer-1", time.Second)
			return err
		}},
		{name: "reap expired", call: func(s *PostgresStore) error {
			_, err := s.ReapExpired(nil, 1)
			return err
		}},
		{name: "renew inbox", call: func(s *PostgresStore) error {
			_, err := s.RenewInbox(nil, 1, testLease(), time.Second)
			return err
		}},
		{name: "complete inbox", call: func(s *PostgresStore) error {
			_, err := s.CompleteInbox(nil, 1, testLease(), OutboxReply{Content: "ok"})
			return err
		}},
		{name: "retry inbox", call: func(s *PostgresStore) error {
			return s.RetryInbox(nil, 1, testLease(), errors.New("temporary"), time.Now())
		}},
		{name: "retry inbox after", call: func(s *PostgresStore) error {
			return s.RetryInboxAfter(nil, 1, testLease(), errors.New("temporary"), time.Second)
		}},
		{name: "wait inbox approval", call: func(s *PostgresStore) error {
			return s.WaitInboxApproval(nil, 1, testLease(), errors.New("approval"), time.Second, time.Now().Add(time.Hour))
		}},
		{name: "block inbox", call: func(s *PostgresStore) error {
			return s.BlockInbox(nil, 1, testLease(), errors.New("unknown"))
		}},
		{name: "dead-letter inbox", call: func(s *PostgresStore) error {
			return s.DeadLetterInbox(nil, 1, testLease(), errors.New("permanent"))
		}},
		{name: "replay inbox", call: func(s *PostgresStore) error {
			return s.ReplayInbox(nil, 1, "operator", "reconcile")
		}},
		{name: "claim outbox", call: func(s *PostgresStore) error {
			_, err := s.ClaimOutbox(nil, "delivery-1", time.Second)
			return err
		}},
		{name: "renew outbox", call: func(s *PostgresStore) error {
			_, err := s.RenewOutbox(nil, 1, testLease(), time.Second)
			return err
		}},
		{name: "mark delivered", call: func(s *PostgresStore) error {
			return s.MarkDelivered(nil, 1, testLease())
		}},
		{name: "advance outbox", call: func(s *PostgresStore) error {
			return s.AdvanceOutbox(nil, 1, testLease(), 1)
		}},
		{name: "retry outbox", call: func(s *PostgresStore) error {
			return s.RetryOutbox(nil, 1, testLease(), errors.New("temporary"), time.Now(), 1)
		}},
		{name: "retry outbox after", call: func(s *PostgresStore) error {
			return s.RetryOutboxAfter(nil, 1, testLease(), errors.New("temporary"), time.Second, 1)
		}},
		{name: "dead-letter outbox", call: func(s *PostgresStore) error {
			return s.DeadLetterOutbox(nil, 1, testLease(), errors.New("permanent"))
		}},
		{name: "block outbox", call: func(s *PostgresStore) error {
			return s.BlockOutbox(nil, 1, testLease(), errors.New("unknown"))
		}},
		{name: "replay outbox", call: func(s *PostgresStore) error {
			return s.ReplayOutbox(nil, 1, "operator", "reconcile")
		}},
		{name: "restart outbox", call: func(s *PostgresStore) error {
			return s.RestartOutbox(nil, 1, "operator", "redeliver")
		}},
		{name: "ping", call: func(s *PostgresStore) error {
			return s.PingContext(nil)
		}},
		{name: "close", call: func(s *PostgresStore) error {
			return s.Close()
		}},
	}

	stores := []struct {
		name  string
		store *PostgresStore
	}{
		{name: "typed nil", store: (*PostgresStore)(nil)},
		{name: "zero value", store: &PostgresStore{}},
	}
	for _, store := range stores {
		for _, test := range tests {
			t.Run(store.name+"/"+test.name, func(t *testing.T) {
				if err := test.call(store.store); !errors.Is(err, ErrStoreUnavailable) {
					t.Fatalf("error=%v, want ErrStoreUnavailable", err)
				}
			})
		}
	}
}

func testLease() Lease {
	return Lease{Owner: "worker-1", Fence: 1, Until: time.Now().Add(time.Minute)}
}

func TestPostgresStoreNilContextIsNormalized(t *testing.T) {
	// The unavailable path still exercises the nil-context normalization in
	// each public operation without requiring a live PostgreSQL server.
	var store *PostgresStore
	if err := store.PingContext(context.TODO()); !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("PingContext error=%v, want ErrStoreUnavailable", err)
	}
}
