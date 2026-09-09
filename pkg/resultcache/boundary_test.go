package resultcache

import (
	"errors"
	"testing"
)

func TestStoreUnavailableNeverPanics(t *testing.T) {
	cases := []struct {
		name string
		call func(*Store) error
	}{
		{name: "get scoped", call: func(s *Store) error {
			_, _, err := s.GetScoped(nil, testIdentity())
			return err
		}},
		{name: "commit success", call: func(s *Store) error {
			return s.CommitSuccess(nil, testIdentity(), 1, "token", []byte("response"))
		}},
		{name: "cleanup", call: func(s *Store) error {
			return s.RunCleanup(nil, 0)
		}},
	}
	stores := []struct {
		name  string
		store *Store
	}{
		{name: "typed nil", store: (*Store)(nil)},
		{name: "zero value", store: &Store{}},
		{name: "nil database", store: New(nil)},
	}
	for _, store := range stores {
		for _, test := range cases {
			t.Run(store.name+"/"+test.name, func(t *testing.T) {
				if err := test.call(store.store); !errors.Is(err, ErrStoreUnavailable) {
					t.Fatalf("error=%v, want ErrStoreUnavailable", err)
				}
			})
		}
	}
}
