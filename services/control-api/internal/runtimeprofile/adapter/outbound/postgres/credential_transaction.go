package postgresadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

var _ application.CredentialStore = (*Store)(nil)

// WithinProfile serializes configuration, credential and receipt mutations for
// exactly one tenant-owned Profile. The callback must not retain its transaction.
func (s *Store) WithinProfile(ctx context.Context, tenantID, profileID string, callback func(application.CredentialTransaction) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin profile credential transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var id string
	err = tx.QueryRow(ctx, `SELECT id FROM runtime_profiles WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, tenantID, profileID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrRuntimeProfileNotFound
	}
	if err != nil {
		return fmt.Errorf("lock credential owning profile: %w", err)
	}
	transaction := &credentialTransaction{tx: tx, store: &Store{db: tx}, tenantID: tenantID, profileID: profileID}
	if err := callback(transaction); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile credential transaction: %w", err)
	}
	return nil
}

type credentialTransaction struct {
	tx        pgx.Tx
	store     *Store
	tenantID  string
	profileID string
}

func (t *credentialTransaction) GetDraft(ctx context.Context) (domain.ProfileDraft, error) {
	return t.store.GetProfileDraft(ctx, t.tenantID, t.profileID)
}

func (t *credentialTransaction) SaveDraft(ctx context.Context, draft domain.ProfileDraft, expected int64) error {
	if draft.TenantID != t.tenantID || draft.ProfileID != t.profileID {
		return domain.ErrCredentialAssociation
	}
	if expected <= 0 || draft.Revision != expected+1 {
		return application.ErrDraftRevisionConflict
	}
	return t.store.SaveProfileDraft(ctx, draft, expected)
}

func (t *credentialTransaction) GetRevision(ctx context.Context, number int64) (domain.ProfileRevision, error) {
	return t.store.GetProfileRevision(ctx, t.tenantID, t.profileID, number)
}

const credentialColumns = `id, tenant_id, profile_id, category, resource_name, purpose, audience_digest,
	credential_revision, status, ciphertext, created_by, updated_by, created_at, updated_at`

func (t *credentialTransaction) GetCredential(ctx context.Context, id string) (domain.ProfileCredential, error) {
	var c domain.ProfileCredential
	err := t.tx.QueryRow(ctx, `SELECT `+credentialColumns+` FROM runtime_profile_credentials WHERE tenant_id=$1 AND profile_id=$2 AND id=$3`, t.tenantID, t.profileID, id).Scan(
		&c.ID, &c.TenantID, &c.ProfileID, &c.Category, &c.ResourceName, &c.Purpose, &c.AudienceDigest,
		&c.Revision, &c.Status, &c.Ciphertext, &c.CreatedBy, &c.UpdatedBy, &c.CreatedAt, &c.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ProfileCredential{}, domain.ErrCredentialNotFound
	}
	if err != nil {
		return domain.ProfileCredential{}, fmt.Errorf("query profile credential: %w", err)
	}
	return c, nil
}

func (t *credentialTransaction) InsertCredential(ctx context.Context, c domain.ProfileCredential) error {
	if c.TenantID != t.tenantID || c.ProfileID != t.profileID {
		return domain.ErrCredentialAssociation
	}
	if c.Revision != 1 || c.Status != domain.CredentialActive || len(c.Ciphertext) == 0 {
		return domain.ErrCredentialInput
	}
	tag, err := t.tx.Exec(ctx, `INSERT INTO runtime_profile_credentials (`+credentialColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (tenant_id, profile_id, id) DO NOTHING`,
		c.ID, t.tenantID, t.profileID, c.Category, c.ResourceName, c.Purpose, c.AudienceDigest,
		c.Revision, c.Status, c.Ciphertext, c.CreatedBy, c.UpdatedBy, c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert profile credential: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrCredentialConflict
	}
	return nil
}

func (t *credentialTransaction) UpdateCredential(ctx context.Context, c domain.ProfileCredential, expected int64) error {
	if c.TenantID != t.tenantID || c.ProfileID != t.profileID {
		return domain.ErrCredentialAssociation
	}
	if expected <= 0 || c.Revision != expected+1 {
		return domain.ErrCredentialConflict
	}
	if (c.Status != domain.CredentialActive && c.Status != domain.CredentialCleared) ||
		(c.Status == domain.CredentialActive && len(c.Ciphertext) == 0) ||
		(c.Status == domain.CredentialCleared && len(c.Ciphertext) != 0) {
		return domain.ErrCredentialInput
	}
	current, err := t.GetCredential(ctx, c.ID)
	if err != nil {
		return err
	}
	if current.Category != c.Category || current.ResourceName != c.ResourceName ||
		current.Purpose != c.Purpose || current.AudienceDigest != c.AudienceDigest {
		return domain.ErrCredentialAssociation
	}
	if current.Status != domain.CredentialActive {
		return domain.ErrCredentialUnavailable
	}
	if current.Revision != expected {
		return domain.ErrCredentialConflict
	}
	// Cleared records have no current value, including no empty bytea placeholder.
	var ciphertext []byte
	if c.Status == domain.CredentialActive {
		ciphertext = c.Ciphertext
	}
	tag, err := t.tx.Exec(ctx, `UPDATE runtime_profile_credentials
		SET credential_revision=$4, status=$5, ciphertext=$6, updated_by=$7, updated_at=$8
		WHERE tenant_id=$1 AND profile_id=$2 AND id=$3 AND credential_revision=$9 AND status='active'`,
		t.tenantID, t.profileID, c.ID, c.Revision, c.Status, ciphertext, c.UpdatedBy, c.UpdatedAt, expected)
	if err != nil {
		return fmt.Errorf("compare and swap profile credential: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrCredentialConflict
	}
	return nil
}

func (t *credentialTransaction) FindReceipt(ctx context.Context, actor, key string) (application.CredentialReceipt, bool, error) {
	var receipt application.CredentialReceipt
	err := t.tx.QueryRow(ctx, `SELECT actor_user_id, idempotency_key, request_mac, result_jsonb, created_at
		FROM runtime_profile_credential_receipts WHERE tenant_id=$1 AND profile_id=$2 AND actor_user_id=$3 AND idempotency_key=$4`,
		t.tenantID, t.profileID, actor, key).Scan(&receipt.ActorUserID, &receipt.Key, &receipt.RequestMAC, &receipt.Result, &receipt.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return application.CredentialReceipt{}, false, nil
	}
	if err != nil {
		return application.CredentialReceipt{}, false, fmt.Errorf("query profile credential receipt: %w", err)
	}
	return receipt, true, nil
}

func (t *credentialTransaction) InsertReceipt(ctx context.Context, receipt application.CredentialReceipt) error {
	tag, err := t.tx.Exec(ctx, `INSERT INTO runtime_profile_credential_receipts
		(tenant_id,profile_id,actor_user_id,idempotency_key,request_mac,result_jsonb,created_at)
		VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)
		ON CONFLICT (tenant_id,profile_id,actor_user_id,idempotency_key) DO NOTHING`,
		t.tenantID, t.profileID, receipt.ActorUserID, receipt.Key, receipt.RequestMAC, []byte(receipt.Result), receipt.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert profile credential receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return application.ErrCredentialIdempotencyConflict
	}
	return nil
}
