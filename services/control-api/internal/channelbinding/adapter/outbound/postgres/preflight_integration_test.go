package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	identitypostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/outbound/postgres"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

type preflightTestAuthorizer struct {
	tenantpostgres.TransactionAuthorizer
}

func (preflightTestAuthorizer) LockRequesterUsers(ctx context.Context, tx pgx.Tx, users []string) error {
	for _, user := range users {
		if _, err := (identitypostgres.TransactionAuthorizer{}).AuthorizeActiveUser(ctx, tx, user); err != nil {
			return err
		}
	}
	return nil
}

func (a preflightTestAuthorizer) AuthorizeRequester(ctx context.Context, tx pgx.Tx, tenant, user string) (bool, error) {
	active, err := (identitypostgres.TransactionAuthorizer{}).AuthorizeActiveUser(ctx, tx, user)
	if err != nil || !active {
		return active, err
	}
	return a.AuthorizeOwner(ctx, tx, tenant, user)
}

func preflightPG(t *testing.T) (*PreflightStore, *application.Service, *pgxpool.Pool) {
	t.Helper()
	_, service, pool := channelPG(t)
	raw, err := migrations.Files.ReadFile("0002_channel_preflights.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), string(raw)); err != nil {
		t.Fatal("apply preflight upgrade", err)
	}
	store, err := NewPreflightStore(pool, preflightTestAuthorizer{}, Options{ScopeID: testScope, SourceEpoch: testEpoch})
	if err != nil {
		t.Fatal(err)
	}
	return store, service, pool
}

func queuedPreflight(a application.PreflightAccount, now time.Time, id string) application.PreflightRecord {
	r := application.PreflightRecord{ScopeID: a.Account.ScopeID, SourceEpoch: testEpoch, WebhookPath: a.Account.Config.WebhookPath, LastCheckedAt: now}
	r.View = channelv1.PreflightView{PreflightID: id, TenantID: a.Account.TenantID, AccountID: a.Account.ID, RequestedBy: testActor.UserID, Provider: string(a.Account.Provider), ProviderAccountID: a.Account.ProviderAccountID, AccountRevision: a.Account.Revision, ConnectionRevision: a.Account.ConnectionRevision, State: "QUEUED", Outcome: "UNKNOWN", ReasonCode: "CHANNEL_PREFLIGHT_QUEUED", Freshness: "NOT_CHECKED", RequestedAt: now, JobDeadlineAt: now.Add(120 * time.Second), Checks: []channelv1.PreflightCheck{}}
	for _, c := range a.Credentials {
		if c.Meta.Purpose == domain.TelegramBotToken {
			r.CredentialID, r.BotTokenConfigured, r.View.BotTokenVersion = c.Meta.ID, c.Meta.Configured, c.Meta.Version
		}
		if c.Meta.Purpose == domain.TelegramWebhookSecret {
			r.WebhookSecretConfigured = c.Meta.Configured
		}
	}
	return r
}

func insertPreflight(t *testing.T, s *PreflightStore, account, id string, age time.Duration) application.PreflightRecord {
	t.Helper()
	var result application.PreflightRecord
	err := s.WithTransaction(context.Background(), "tenant:tnt_a", func(tx application.PreflightTransaction) error {
		a, err := tx.LockAccount(context.Background(), "tnt_a", account, testActor.UserID, false)
		if err != nil {
			return err
		}
		now, err := tx.Now(context.Background())
		if err != nil {
			return err
		}
		result = queuedPreflight(a, now.Add(-age), id)
		return tx.Save(context.Background(), result)
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPreflightPersistenceAndIsolatedUpgradeAgainstPostgreSQL(t *testing.T) {
	s, app, pool := preflightPG(t)
	created := createAccount(t, app)
	ctx := context.Background()
	var before string
	if err := pool.QueryRow(ctx, `SELECT row_to_json(c)::text FROM channel_account_catalog c WHERE scope_id=$1`, testScope).Scan(&before); err != nil {
		t.Fatal(err)
	}
	r := insertPreflight(t, s, created.Account.ID, "cpf_persist", 0)
	key, found, err := s.Lookup(ctx, testScope, r.View.PreflightID)
	if err != nil || !found || key.TenantID != "tnt_a" || key.AccountID != created.Account.ID {
		t.Fatal("lookup", found, err)
	}
	activeKey, found, err := s.ActiveAccount(ctx, testScope, "tnt_a", created.Account.ID)
	if err != nil || !found || activeKey != key {
		t.Fatal("active account discovery", found, err)
	}
	if _, found, err := s.ActiveAccount(ctx, testScope, "tnt_b", created.Account.ID); err != nil || found {
		t.Fatal("cross-tenant active task discovery", found, err)
	}
	err = s.WithTransaction(ctx, "account:"+key.AccountID, func(tx application.PreflightTransaction) error {
		a, err := tx.LockAccount(ctx, key.TenantID, key.AccountID, key.RequestedBy, false)
		if err != nil || !a.TenantActive || !a.RequesterOwner || a.SessionOwner || len(a.Credentials) != 2 || len(a.Credentials[0].Ciphertext) == 0 {
			t.Fatalf("private facts were lost: tenant=%v requester=%v session=%v err=%v", a.TenantActive, a.RequesterOwner, a.SessionOwner, err)
		}
		got, ok, err := tx.Load(ctx, key.ID)
		if err != nil || !ok || got.CredentialID != r.CredentialID || !got.View.RequestedAt.Equal(r.View.RequestedAt) {
			t.Fatal("stored record", ok, err)
		}
		ac, tc, active, err := tx.Counts(ctx, key.TenantID, key.AccountID, r.View.RequestedAt.Add(-time.Minute))
		if err != nil || ac != 1 || tc != 1 || active != 1 {
			t.Fatal("quota counts", ac, tc, active, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var after string
	if err = pool.QueryRow(ctx, `SELECT row_to_json(c)::text FROM channel_account_catalog c WHERE scope_id=$1`, testScope).Scan(&after); err != nil || before != after {
		t.Fatal("preflight changed operational catalog", err)
	}
	var raw string
	if err = pool.QueryRow(ctx, `SELECT record_jsonb::text FROM channel_preflights WHERE id=$1`, key.ID).Scan(&raw); err != nil || strings.Contains(raw, "TEST_ONLY") || strings.Contains(raw, "ciphertext") {
		t.Fatal("secret-bearing task", err)
	}
}

func TestPreflightTransactionsRejectCrossAccountAndRollbackAgainstPostgreSQL(t *testing.T) {
	s, app, pool := preflightPG(t)
	a := createAccount(t, app)
	ctx := context.Background()
	r := insertPreflight(t, s, a.Account.ID, "cpf_scope", 0)
	if _, found, err := s.Lookup(ctx, "other_scope", r.View.PreflightID); err == nil || found {
		t.Fatal("scope expanded", err)
	}
	sentinel := errors.New("rollback requested")
	err := s.WithTransaction(ctx, "account:"+a.Account.ID, func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, "tnt_a", a.Account.ID, testActor.UserID, false); err != nil {
			return err
		}
		got, found, err := tx.Load(ctx, r.View.PreflightID)
		if err != nil || !found {
			t.Fatal(err)
		}
		if _, err = tx.LockAccount(ctx, "tnt_b", a.Account.ID, "usr_other", false); err == nil {
			t.Fatal("second-account lock admitted")
		}
		got.View.State, got.View.ReasonCode = "TIMED_OUT", "CHANNEL_PREFLIGHT_NO_EXECUTOR"
		if err = tx.Save(ctx, got); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	var state string
	if err = pool.QueryRow(ctx, `SELECT state FROM channel_preflights WHERE id=$1`, r.View.PreflightID).Scan(&state); err != nil || state != "QUEUED" {
		t.Fatal("callback failure committed", state, err)
	}
}

func TestPreflightReceiptRoundTripAndEmptyClaimAgainstPostgreSQL(t *testing.T) {
	s, app, _ := preflightPG(t)
	a := createAccount(t, app)
	r := insertPreflight(t, s, a.Account.ID, "cpf_receipt", 0)
	ctx := context.Background()
	key := "sha256:" + strings.Repeat("a", 64)
	err := s.WithTransaction(ctx, "tenant:tnt_a", func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, "tnt_a", a.Account.ID, testActor.UserID, true); err != nil {
			return err
		}
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(channelv1.PreflightCreated{PreflightID: r.View.PreflightID, TenantID: "tnt_a", AccountID: a.Account.ID, RequestedAt: r.View.RequestedAt, JobDeadlineAt: r.View.JobDeadlineAt, StatusURL: "/v1/tenants/tnt_a/channel-accounts/" + a.Account.ID + "/preflights/" + r.View.PreflightID})
		return tx.SaveRequest(ctx, application.PreflightRequest{Kind: "create", Key: key, Digest: key, TenantID: "tnt_a", AccountID: a.Account.ID, RequestedBy: testActor.UserID, PreflightID: r.View.PreflightID, CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour), Response: raw})
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.WithTransaction(ctx, "instance:principal", func(tx application.PreflightTransaction) error {
		now, err := tx.Now(ctx)
		if err != nil {
			return err
		}
		return tx.SaveRequest(ctx, application.PreflightRequest{Kind: "claim", Key: key, Digest: key, PrincipalID: "principal", InstanceID: "instance", InstanceEpoch: testEpoch, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute)})
	})
	if err != nil {
		t.Fatal(err)
	}
	err = s.WithTransaction(ctx, "instance:principal", func(tx application.PreflightTransaction) error {
		got, found, err := tx.FindRequest(ctx, "claim", key)
		if err != nil || !found || (len(got.Response) != 0 && string(got.Response) != "null") {
			t.Fatal("empty claim receipt", found, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
