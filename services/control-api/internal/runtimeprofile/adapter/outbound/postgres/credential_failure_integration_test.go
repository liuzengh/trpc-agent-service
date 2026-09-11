package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestCredentialTransactionWriteFailuresRollbackAgainstPostgreSQL(t *testing.T) {
	for _, failure := range []string{"sql constraint", "draft cas", "duplicate receipt", "context cancelled"} {
		t.Run(failure, func(t *testing.T) {
			store, _ := credentialPostgres(t)
			ctx := context.Background()
			if failure == "duplicate receipt" {
				if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
					return tx.InsertReceipt(ctx, testReceipt("failure"))
				}); err != nil {
					t.Fatal(err)
				}
			}
			writeCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			err := store.WithinProfile(writeCtx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
				if err := tx.InsertCredential(writeCtx, testCredential("must-rollback")); err != nil {
					return err
				}
				draft, err := tx.GetDraft(writeCtx)
				if err != nil {
					return err
				}
				if failure == "draft cas" {
					draft.Revision = 3
					return tx.SaveDraft(writeCtx, draft, 2)
				}
				draft.Revision++
				draft.Spec = json.RawMessage(`{"failed":true}`)
				if err := tx.SaveDraft(writeCtx, draft, 1); err != nil {
					return err
				}
				if failure == "context cancelled" {
					cancel()
					return nil // Commit must fail and roll back, despite callback success.
				}
				receipt := testReceipt("failure")
				if failure == "sql constraint" {
					receipt.Result = json.RawMessage(`[]`)
				}
				return tx.InsertReceipt(writeCtx, receipt)
			})
			if err == nil {
				t.Fatal("write failure committed")
			}
			if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
				draft, err := tx.GetDraft(ctx)
				if err != nil {
					return err
				}
				if draft.Revision != 1 || string(draft.Spec) != `{}` {
					t.Fatalf("failed write left draft=%#v", draft)
				}
				if _, err := tx.GetCredential(ctx, "must-rollback"); !errors.Is(err, domain.ErrCredentialNotFound) {
					t.Fatalf("failed write left credential: %v", err)
				}
				receipt, found, err := tx.FindReceipt(ctx, "actor", "failure")
				if err != nil {
					return err
				}
				if found != (failure == "duplicate receipt") {
					t.Fatalf("unexpected receipt existence=%v", found)
				}
				if found && receipt.RequestMAC != "non-secret-request-mac" {
					t.Fatal("original receipt was overwritten")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
