package reliable

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresRenewInboxKeepsTenantAdmissionLockedUntilCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	until := time.Now().Add(time.Minute)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id FROM inbox_messages WHERE id=$1`)).
		WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("tenant-a"))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO tenant_queue_schedule(tenant_id) VALUES($1) ON CONFLICT (tenant_id) DO NOTHING`)).
		WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT tenant_id FROM tenant_queue_schedule WHERE tenant_id=$1 FOR UPDATE`)).
		WithArgs("tenant-a").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("tenant-a"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM inbox_messages WHERE id=$1 FOR UPDATE`)).
		WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	mock.ExpectQuery(activeLeaseClockPredicate).
		WithArgs(int64(7), "worker", int64(3), int64(60000)).
		WillReturnRows(sqlmock.NewRows([]string{"lease_version", "lease_until"}).AddRow(3, until))
	mock.ExpectCommit()

	lease, err := NewPostgresStore(db).RenewInbox(context.Background(), 7, Lease{Owner: "worker", Fence: 3}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Owner != "worker" || lease.Fence != 3 || !lease.Until.Equal(until) {
		t.Fatalf("renewed lease=%+v, want original owner/fence and new expiry", lease)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
