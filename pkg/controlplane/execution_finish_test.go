// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package controlplane

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRenewLeaseRejectsExpiredAttempt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	recorder := NewExecutionRecorder(db)
	mock.ExpectExec("UPDATE execution_records").
		WithArgs(int64(7), "attempt-7", int64((15*time.Minute)/time.Millisecond)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT execution_token, status, lease_until").
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"execution_token", "status", "lease_until", "lease_valid"}).
			AddRow("attempt-7", "RUNNING", time.Now().Add(-time.Minute), false))

	err = recorder.RenewLease(context.Background(), ExecutionHandle{ID: 7, Token: "attempt-7"})
	if !errors.Is(err, ErrExecutionLeaseExpired) {
		t.Fatalf("renew error = %v, want ErrExecutionLeaseExpired", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRunHeartbeatStopsAfterRenewFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	recorder := NewExecutionRecorder(db)
	mock.ExpectExec("UPDATE execution_records").
		WithArgs(int64(7), "attempt-7", int64(DefaultExecutionLeaseTTL/time.Millisecond)).
		WillReturnError(errors.New("database unavailable"))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = recorder.RunHeartbeat(ctx, ExecutionHandle{ID: 7, Token: "attempt-7"}, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("heartbeat error = %v, want database failure", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFailRejectsStaleWorker(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	recorder := NewExecutionRecorder(db)
	mock.ExpectExec(regexp.QuoteMeta(`
		UPDATE execution_records
		SET status='FAILED', retry_safe=$3, error_message=$4,
		    completed_at=clock_timestamp(), lease_until=clock_timestamp()
		WHERE id=$1 AND execution_token=$2 AND status='RUNNING'
		  AND lease_until > clock_timestamp()`)).
		WithArgs(int64(7), "attempt-old", false, "execution_failed").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT execution_token, status, retry_safe, lease_until,
		       (lease_until > clock_timestamp())
		FROM execution_records
		WHERE id=$1`)).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"execution_token", "status", "retry_safe", "lease_until", "lease_valid"}).
			AddRow("attempt-new", "RUNNING", false, time.Now().Add(time.Minute), true))

	err = recorder.Fail(
		context.Background(),
		ExecutionHandle{ID: 7, Token: "attempt-old"},
		Failure{Code: "execution_failed"},
	)
	if !errors.Is(err, ErrExecutionFenceMismatch) {
		t.Fatalf("finish error = %v, want ErrExecutionFenceMismatch", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFailRecordsRetrySafetyWithToken(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	recorder := NewExecutionRecorder(db)
	mock.ExpectExec(regexp.QuoteMeta(`
		UPDATE execution_records
		SET status='FAILED', retry_safe=$3, error_message=$4,
		    completed_at=clock_timestamp(), lease_until=clock_timestamp()
		WHERE id=$1 AND execution_token=$2 AND status='RUNNING'
		  AND lease_until > clock_timestamp()`)).
		WithArgs(int64(7), "attempt-7", true, "worker_initialization_failed").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = recorder.Fail(
		context.Background(),
		ExecutionHandle{ID: 7, Token: "attempt-7"},
		Failure{Code: "worker_initialization_failed", SafeToRetry: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

const testExecutionPayloadHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestSanitizeExecutionErrorBoundsAndRemovesControls(t *testing.T) {
	got := sanitizeExecutionError("provider\nerror\t" + string(make([]byte, 700)))
	if len(got) > maxExecutionErrorBytes {
		t.Fatalf("error length=%d, want <=%d", len(got), maxExecutionErrorBytes)
	}
	for _, r := range got {
		if r == '\n' || r == '\r' || r == '\t' || r == 0 {
			t.Fatalf("control character survived sanitization: %q", got)
		}
	}
}
