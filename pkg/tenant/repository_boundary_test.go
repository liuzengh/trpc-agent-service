package tenant

import (
	"database/sql"
	"errors"
	"testing"
)

func TestSQLRepositoryUnavailableNeverPanics(t *testing.T) {
	tests := []struct {
		name string
		call func(*SQLRepository) error
	}{
		{name: "create", call: func(r *SQLRepository) error {
			return r.Create(nil, &Tenant{})
		}},
		{name: "get by id", call: func(r *SQLRepository) error {
			_, err := r.GetByID(nil, "tenant-a")
			return err
		}},
		{name: "get status", call: func(r *SQLRepository) error {
			_, err := r.GetStatus(nil, "tenant-a")
			return err
		}},
		{name: "get by webhook", call: func(r *SQLRepository) error {
			_, err := r.GetByWebhookToken(nil, "key")
			return err
		}},
		{name: "list", call: func(r *SQLRepository) error {
			_, err := r.List(nil, TenantStatusActive)
			return err
		}},
		{name: "list by ids", call: func(r *SQLRepository) error {
			_, err := r.ListByIDs(nil, TenantStatusActive, []string{"tenant-a"})
			return err
		}},
		{name: "update", call: func(r *SQLRepository) error {
			return r.Update(nil, &Tenant{})
		}},
		{name: "delete", call: func(r *SQLRepository) error {
			return r.Delete(nil, "tenant-a")
		}},
		{name: "close", call: func(r *SQLRepository) error {
			return r.Close()
		}},
		{name: "ping", call: func(r *SQLRepository) error {
			return r.PingContext(nil)
		}},
	}

	stores := []struct {
		name string
		repo *SQLRepository
	}{
		{name: "typed nil", repo: (*SQLRepository)(nil)},
		{name: "zero value", repo: &SQLRepository{}},
	}
	for _, store := range stores {
		for _, test := range tests {
			t.Run(store.name+"/"+test.name, func(t *testing.T) {
				if err := test.call(store.repo); !errors.Is(err, ErrTenantRepositoryUnavailable) {
					t.Fatalf("error=%v, want ErrTenantRepositoryUnavailable", err)
				}
			})
		}
	}
}

func TestInsertChannelBindingRejectsMissingInputs(t *testing.T) {
	if err := insertChannelBinding(nil, nil, "tenant-a", 0, &ChannelBinding{}); !errors.Is(err, ErrTenantRepositoryUnavailable) {
		t.Fatalf("nil executor error=%v, want ErrTenantRepositoryUnavailable", err)
	}
	var tx *sql.Tx
	if err := insertChannelBinding(nil, tx, "tenant-a", 0, &ChannelBinding{}); !errors.Is(err, ErrTenantRepositoryUnavailable) {
		t.Fatalf("typed-nil executor error=%v, want ErrTenantRepositoryUnavailable", err)
	}
}
