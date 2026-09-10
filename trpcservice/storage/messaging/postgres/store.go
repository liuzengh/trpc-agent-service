// Package postgres implements durable Inbox, Outbox and delivery stores.
package postgres

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type Store struct {
	db          *sql.DB
	payloadKeys messaging.PayloadKeyResolver
}

func New(db *sql.DB) *Store { return &Store{db: db} }

// NewWithPayloadKey is retained for tests and single-key compatibility
// fixtures. Production composition must use NewWithPayloadKeyResolver.
func NewWithPayloadKey(db *sql.DB, key []byte, keyVersion int64) *Store {
	return NewWithPayloadKeyResolver(db, staticPayloadKeyResolver{key: append([]byte(nil), key...), version: keyVersion})
}

func NewWithPayloadKeyResolver(db *sql.DB, resolver messaging.PayloadKeyResolver) *Store {
	return &Store{db: db, payloadKeys: resolver}
}

func (s *Store) PutPayload(ctx context.Context, in messaging.PayloadRecord) error {
	if in.TenantID == "" || in.RequestID == "" || in.PayloadRef == "" || in.ContentDigest == "" || len(in.Content) == 0 {
		return runtime.ErrCommitConflict
	}
	key, err := s.resolvePayloadKey(ctx, in.TenantID, in.KeyVersion)
	if err != nil {
		return err
	}
	defer clear(key)
	ciphertext, nonce, err := encryptPayload(key, payloadAAD(in), in.Content)
	if err != nil {
		return err
	}
	var storedRef, storedDigest string
	var storedCiphertext, storedNonce []byte
	var storedKeyVersion int64
	err = s.db.QueryRowContext(ctx, `INSERT INTO inbound_payload(tenant_id,request_id,payload_ref,payload_ciphertext,payload_nonce,content_digest,key_version)
VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (tenant_id,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
RETURNING payload_ref,content_digest,payload_ciphertext,payload_nonce,key_version`, in.TenantID, in.RequestID, in.PayloadRef, ciphertext, nonce, in.ContentDigest, in.KeyVersion).
		Scan(&storedRef, &storedDigest, &storedCiphertext, &storedNonce, &storedKeyVersion)
	if err != nil {
		return translate(err)
	}
	if storedRef != in.PayloadRef || storedDigest != in.ContentDigest || storedKeyVersion != in.KeyVersion {
		return runtime.ErrIdempotencyCollision
	}
	storedContent, err := decryptPayload(key, payloadAAD(in), storedCiphertext, storedNonce)
	defer clear(storedContent)
	if err != nil || !bytes.Equal(storedContent, in.Content) {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func (s *Store) GetPayload(ctx context.Context, tenantID, requestID string) (messaging.PayloadRecord, error) {
	var record messaging.PayloadRecord
	record.TenantID, record.RequestID = tenantID, requestID
	var ciphertext, nonce []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload_ref,content_digest,payload_ciphertext,payload_nonce,key_version,created_at FROM inbound_payload
WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).
		Scan(&record.PayloadRef, &record.ContentDigest, &ciphertext, &nonce, &record.KeyVersion, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.PayloadRecord{}, runtime.ErrNotFound
	}
	if err != nil {
		return messaging.PayloadRecord{}, err
	}
	key, err := s.resolvePayloadKey(ctx, record.TenantID, record.KeyVersion)
	if err != nil {
		return messaging.PayloadRecord{}, err
	}
	defer clear(key)
	record.Content, err = decryptPayload(key, payloadAAD(record), ciphertext, nonce)
	return record, err
}

func (s *Store) PutPreparedPayload(ctx context.Context, in messaging.PreparedPayloadRecord) error {
	if in.TenantID == "" || in.RequestID == "" || !strings.HasPrefix(in.PayloadRef, "prepared://") || !strings.HasPrefix(in.SourcePayloadRef, "inbound://") || in.ContentDigest == "" || len(in.Content) == 0 ||
		in.ArtifactRetention < time.Second || in.ArtifactRetention%time.Second != 0 || !validPreparedArtifactReferences(in.ArtifactReferences) {
		return runtime.ErrCommitConflict
	}
	key, err := s.resolvePayloadKey(ctx, in.TenantID, in.KeyVersion)
	if err != nil {
		return err
	}
	defer clear(key)
	payload := messaging.PayloadRecord{TenantID: in.TenantID, RequestID: in.RequestID, PayloadRef: in.PayloadRef,
		ContentDigest: in.ContentDigest, Content: in.Content, KeyVersion: in.KeyVersion}
	ciphertext, nonce, err := encryptPayload(key, payloadAAD(payload), in.Content)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var storedSource, storedRef, storedDigest string
	var storedCiphertext, storedNonce []byte
	var storedKeyVersion, storedRetentionSeconds int64
	var storedCreatedAt time.Time
	retentionSeconds := int64(in.ArtifactRetention / time.Second)
	err = tx.QueryRowContext(ctx, `INSERT INTO prepared_payload(
tenant_id,request_id,payload_ref,source_payload_ref,payload_ciphertext,payload_nonce,content_digest,key_version,artifact_retention_seconds)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (tenant_id,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
RETURNING source_payload_ref,payload_ref,content_digest,payload_ciphertext,payload_nonce,key_version,
artifact_retention_seconds,created_at`, in.TenantID, in.RequestID, in.PayloadRef, in.SourcePayloadRef, ciphertext, nonce,
		in.ContentDigest, in.KeyVersion, retentionSeconds).
		Scan(&storedSource, &storedRef, &storedDigest, &storedCiphertext, &storedNonce, &storedKeyVersion,
			&storedRetentionSeconds, &storedCreatedAt)
	if err != nil {
		return translate(err)
	}
	if storedSource != in.SourcePayloadRef || storedRef != in.PayloadRef || storedDigest != in.ContentDigest ||
		storedKeyVersion != in.KeyVersion || storedRetentionSeconds != retentionSeconds {
		return runtime.ErrIdempotencyCollision
	}
	storedContent, err := decryptPayload(key, payloadAAD(payload), storedCiphertext, storedNonce)
	defer clear(storedContent)
	if err != nil || !bytes.Equal(storedContent, in.Content) {
		return runtime.ErrIdempotencyCollision
	}
	retainUntil := storedCreatedAt.Add(in.ArtifactRetention)
	for _, reference := range in.ArtifactReferences {
		var storedArtifactID string
		var storedRetainUntil time.Time
		err := tx.QueryRowContext(ctx, `INSERT INTO artifact_reference(
tenant_id,artifact_id,reference_kind,reference_id,retain_until)
SELECT $1,a.artifact_id,'prepared_payload',$4,$5
FROM media_artifact a WHERE a.tenant_id=$1 AND a.request_id=$2 AND a.artifact_id=$3
ON CONFLICT (tenant_id,artifact_id,reference_kind,reference_id)
DO UPDATE SET artifact_id=EXCLUDED.artifact_id
RETURNING artifact_id,retain_until`, in.TenantID, in.RequestID, reference.ArtifactID, in.PayloadRef, retainUntil).
			Scan(&storedArtifactID, &storedRetainUntil)
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ErrCommitConflict
		}
		if err != nil {
			return translate(err)
		}
		if storedArtifactID != reference.ArtifactID || !storedRetainUntil.Equal(retainUntil) {
			return runtime.ErrIdempotencyCollision
		}
		result, err := tx.ExecContext(ctx, `UPDATE media_artifact SET retention_managed=true
WHERE tenant_id=$1 AND request_id=$2 AND artifact_id=$3`, in.TenantID, in.RequestID, reference.ArtifactID)
		if err != nil {
			return translate(err)
		}
		if count, err := result.RowsAffected(); err != nil || count != 1 {
			return runtime.ErrCommitConflict
		}
	}
	var storedReferenceCount int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM artifact_reference
WHERE tenant_id=$1 AND reference_kind='prepared_payload' AND reference_id=$2`, in.TenantID, in.PayloadRef).
		Scan(&storedReferenceCount); err != nil {
		return translate(err)
	}
	if storedReferenceCount != len(in.ArtifactReferences) {
		return runtime.ErrIdempotencyCollision
	}
	if err := tx.Commit(); err != nil {
		return translate(err)
	}
	return nil
}

func validPreparedArtifactReferences(values []messaging.PreparedArtifactReference) bool {
	if len(values) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.ArtifactID == "" {
			return false
		}
		if _, exists := seen[value.ArtifactID]; exists {
			return false
		}
		seen[value.ArtifactID] = struct{}{}
	}
	return true
}

func (s *Store) GetPreparedPayload(ctx context.Context, tenantID, requestID, payloadRef string) (messaging.PayloadRecord, error) {
	if tenantID == "" || requestID == "" || !strings.HasPrefix(payloadRef, "prepared://") {
		return messaging.PayloadRecord{}, runtime.ErrTenantScope
	}
	record := messaging.PayloadRecord{TenantID: tenantID, RequestID: requestID, PayloadRef: payloadRef}
	var ciphertext, nonce []byte
	err := s.db.QueryRowContext(ctx, `SELECT content_digest,payload_ciphertext,payload_nonce,key_version,created_at
FROM prepared_payload WHERE tenant_id=$1 AND request_id=$2 AND payload_ref=$3`, tenantID, requestID, payloadRef).
		Scan(&record.ContentDigest, &ciphertext, &nonce, &record.KeyVersion, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.PayloadRecord{}, runtime.ErrNotFound
	}
	if err != nil {
		return messaging.PayloadRecord{}, err
	}
	key, err := s.resolvePayloadKey(ctx, record.TenantID, record.KeyVersion)
	if err != nil {
		return messaging.PayloadRecord{}, err
	}
	defer clear(key)
	record.Content, err = decryptPayload(key, payloadAAD(record), ciphertext, nonce)
	return record, err
}

func (s *Store) PutResult(ctx context.Context, in messaging.ResultRecord) error {
	contentType, typeErr := messaging.NormalizeContentType(in.ContentType)
	if in.TenantID == "" || in.RequestID == "" || in.ResultRef == "" || in.ContentDigest == "" || len(in.Content) == 0 || typeErr != nil {
		return runtime.ErrCommitConflict
	}
	in.ContentType = contentType
	key, err := s.resolvePayloadKey(ctx, in.TenantID, in.KeyVersion)
	if err != nil {
		return err
	}
	defer clear(key)
	aad := resultAAD(in)
	ciphertext, nonce, err := encryptPayload(key, aad, in.Content)
	if err != nil {
		return err
	}
	var ref, digest, storedType string
	var encrypted, storedNonce []byte
	var version int64
	err = s.db.QueryRowContext(ctx, `INSERT INTO result_payload(tenant_id,request_id,result_ref,result_ciphertext,result_nonce,content_digest,content_type,key_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (tenant_id,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
RETURNING result_ref,content_digest,content_type,result_ciphertext,result_nonce,key_version`, in.TenantID, in.RequestID, in.ResultRef, ciphertext, nonce, in.ContentDigest, in.ContentType, in.KeyVersion).
		Scan(&ref, &digest, &storedType, &encrypted, &storedNonce, &version)
	if err != nil {
		return translate(err)
	}
	if ref != in.ResultRef || digest != in.ContentDigest || storedType != in.ContentType || version != in.KeyVersion {
		return runtime.ErrIdempotencyCollision
	}
	content, err := decryptPayload(key, aad, encrypted, storedNonce)
	defer clear(content)
	if err != nil || !bytes.Equal(content, in.Content) {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func (s *Store) GetResult(ctx context.Context, tenantID, requestID string) (messaging.ResultRecord, error) {
	record := messaging.ResultRecord{TenantID: tenantID, RequestID: requestID}
	var ciphertext, nonce []byte
	err := s.db.QueryRowContext(ctx, `SELECT result_ref,content_digest,content_type,result_ciphertext,result_nonce,key_version,created_at FROM result_payload WHERE tenant_id=$1 AND request_id=$2`, tenantID, requestID).
		Scan(&record.ResultRef, &record.ContentDigest, &record.ContentType, &ciphertext, &nonce, &record.KeyVersion, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.ResultRecord{}, runtime.ErrNotFound
	}
	if err != nil {
		return messaging.ResultRecord{}, err
	}
	key, err := s.resolvePayloadKey(ctx, record.TenantID, record.KeyVersion)
	if err != nil {
		return messaging.ResultRecord{}, err
	}
	defer clear(key)
	record.Content, err = decryptPayload(key, resultAAD(record), ciphertext, nonce)
	return record, err
}

func (s *Store) PutToolResult(ctx context.Context, in messaging.ToolResultRecord) error {
	if in.TenantID == "" || in.GrantID == "" || in.RequestID == "" || in.ResultRef == "" || in.ContentDigest == "" || len(in.Content) == 0 || in.KeyVersion < 1 {
		return runtime.ErrCommitConflict
	}
	key, err := s.resolvePayloadKey(ctx, in.TenantID, in.KeyVersion)
	if err != nil {
		return err
	}
	defer clear(key)
	aad := toolResultAAD(in)
	ciphertext, nonce, err := encryptPayload(key, aad, in.Content)
	if err != nil {
		return err
	}
	var requestID, ref, digest string
	var encrypted, storedNonce []byte
	var version int64
	err = s.db.QueryRowContext(ctx, `INSERT INTO tool_result_payload(tenant_id,grant_id,request_id,result_ref,result_ciphertext,result_nonce,content_digest,key_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (tenant_id,grant_id) DO UPDATE SET grant_id=EXCLUDED.grant_id
RETURNING request_id,result_ref,content_digest,result_ciphertext,result_nonce,key_version`, in.TenantID, in.GrantID, in.RequestID, in.ResultRef, ciphertext, nonce, in.ContentDigest, in.KeyVersion).
		Scan(&requestID, &ref, &digest, &encrypted, &storedNonce, &version)
	if err != nil {
		return translate(err)
	}
	if requestID != in.RequestID || ref != in.ResultRef || digest != in.ContentDigest || version != in.KeyVersion {
		return runtime.ErrIdempotencyCollision
	}
	content, err := decryptPayload(key, aad, encrypted, storedNonce)
	defer clear(content)
	if err != nil || !bytes.Equal(content, in.Content) {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func (s *Store) GetToolResult(ctx context.Context, tenantID, grantID string) (messaging.ToolResultRecord, error) {
	record := messaging.ToolResultRecord{TenantID: tenantID, GrantID: grantID}
	var ciphertext, nonce []byte
	err := s.db.QueryRowContext(ctx, `SELECT request_id,result_ref,content_digest,result_ciphertext,result_nonce,key_version,created_at
FROM tool_result_payload WHERE tenant_id=$1 AND grant_id=$2`, tenantID, grantID).
		Scan(&record.RequestID, &record.ResultRef, &record.ContentDigest, &ciphertext, &nonce, &record.KeyVersion, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.ToolResultRecord{}, runtime.ErrNotFound
	}
	if err != nil {
		return messaging.ToolResultRecord{}, err
	}
	key, err := s.resolvePayloadKey(ctx, tenantID, record.KeyVersion)
	if err != nil {
		return messaging.ToolResultRecord{}, err
	}
	defer clear(key)
	record.Content, err = decryptPayload(key, toolResultAAD(record), ciphertext, nonce)
	return record, err
}

func (s *Store) PutInteraction(ctx context.Context, in messaging.InteractionRecord) error {
	contentType, typeErr := messaging.NormalizeContentType(in.ContentType)
	if in.TenantID == "" || in.RequestID == "" || in.ContentRef == "" || in.ContentDigest == "" || len(in.Content) == 0 || in.KeyVersion < 1 || typeErr != nil {
		return runtime.ErrCommitConflict
	}
	in.ContentType = contentType
	key, err := s.resolvePayloadKey(ctx, in.TenantID, in.KeyVersion)
	if err != nil {
		return err
	}
	defer clear(key)
	aad := interactionAAD(in)
	ciphertext, nonce, err := encryptPayload(key, aad, in.Content)
	if err != nil {
		return err
	}
	var digest, storedType string
	var encrypted, storedNonce []byte
	var version int64
	err = s.db.QueryRowContext(ctx, `INSERT INTO interaction_payload(tenant_id,request_id,content_ref,content_ciphertext,content_nonce,content_digest,content_type,key_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (tenant_id,request_id,content_ref) DO UPDATE SET content_ref=EXCLUDED.content_ref
RETURNING content_digest,content_type,content_ciphertext,content_nonce,key_version`, in.TenantID, in.RequestID, in.ContentRef, ciphertext, nonce, in.ContentDigest, in.ContentType, in.KeyVersion).
		Scan(&digest, &storedType, &encrypted, &storedNonce, &version)
	if err != nil {
		return translate(err)
	}
	if digest != in.ContentDigest || storedType != in.ContentType || version != in.KeyVersion {
		return runtime.ErrIdempotencyCollision
	}
	content, err := decryptPayload(key, aad, encrypted, storedNonce)
	defer clear(content)
	if err != nil || !bytes.Equal(content, in.Content) {
		return runtime.ErrIdempotencyCollision
	}
	return nil
}

func (s *Store) GetReplyContent(ctx context.Context, tenantID, requestID, contentRef string) (messaging.ResultRecord, error) {
	terminal, err := s.GetResult(ctx, tenantID, requestID)
	terminalExists := err == nil
	if err == nil && terminal.ResultRef == contentRef {
		return terminal, nil
	}
	if err != nil && !errors.Is(err, runtime.ErrNotFound) {
		return messaging.ResultRecord{}, err
	}
	value := messaging.ResultRecord{TenantID: tenantID, RequestID: requestID, ResultRef: contentRef}
	var ciphertext, nonce []byte
	err = s.db.QueryRowContext(ctx, `SELECT content_digest,content_type,content_ciphertext,content_nonce,key_version,created_at FROM interaction_payload
WHERE tenant_id=$1 AND request_id=$2 AND content_ref=$3`, tenantID, requestID, contentRef).
		Scan(&value.ContentDigest, &value.ContentType, &ciphertext, &nonce, &value.KeyVersion, &value.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		if terminalExists {
			return messaging.ResultRecord{}, runtime.ErrVersionMismatch
		}
		return messaging.ResultRecord{}, runtime.ErrNotFound
	}
	if err != nil {
		return messaging.ResultRecord{}, err
	}
	key, err := s.resolvePayloadKey(ctx, tenantID, value.KeyVersion)
	if err != nil {
		return messaging.ResultRecord{}, err
	}
	defer clear(key)
	record := messaging.InteractionRecord{TenantID: tenantID, RequestID: requestID, ContentRef: contentRef, ContentDigest: value.ContentDigest, ContentType: value.ContentType}
	value.Content, err = decryptPayload(key, interactionAAD(record), ciphertext, nonce)
	return value, err
}

func (s *Store) resolvePayloadKey(ctx context.Context, tenantID string, version int64) ([]byte, error) {
	if s == nil || s.payloadKeys == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	if tenantID == "" {
		return nil, runtime.ErrTenantScope
	}
	if version < 1 {
		return nil, runtime.ErrVersionMismatch
	}
	value, err := s.payloadKeys.ResolvePayloadKey(ctx, tenantID, version)
	if err != nil {
		clear(value.Bytes)
		return nil, err
	}
	if value.Version != version {
		clear(value.Bytes)
		return nil, runtime.ErrVersionMismatch
	}
	if len(value.Bytes) != 32 {
		clear(value.Bytes)
		return nil, runtime.ErrCapabilityUnsupported
	}
	return value.Bytes, nil
}

type staticPayloadKeyResolver struct {
	key     []byte
	version int64
}

func (r staticPayloadKeyResolver) ResolvePayloadKey(_ context.Context, _ string, version int64) (messaging.PayloadCipherKey, error) {
	if version != r.version {
		return messaging.PayloadCipherKey{}, runtime.ErrVersionMismatch
	}
	if len(r.key) != 32 {
		return messaging.PayloadCipherKey{}, runtime.ErrCapabilityUnsupported
	}
	return messaging.PayloadCipherKey{Bytes: append([]byte(nil), r.key...), Version: r.version}, nil
}

func (s *Store) ResolveReplyRoute(ctx context.Context, tenantID, requestID string) (messaging.ReplyRoute, error) {
	if tenantID == "" || requestID == "" {
		return messaging.ReplyRoute{}, runtime.ErrTenantScope
	}
	var route messaging.ReplyRoute
	route.TenantID, route.RequestID = tenantID, requestID
	err := s.db.QueryRowContext(ctx, `SELECT e.channel,e.config_version,cb.binding_id,i.external_account_id,i.external_message_id,i.external_chat_id,i.external_user_id
FROM execution_record e
JOIN inbox i ON i.tenant_id=e.tenant_id AND i.request_id=e.request_id
JOIN channel_binding cb ON cb.tenant_id=e.tenant_id AND cb.config_version=e.config_version
  AND cb.channel=i.channel AND cb.external_account_id=i.external_account_id
  AND cb.agent_app_id=e.agent_app_id
WHERE e.tenant_id=$1 AND e.request_id=$2`, tenantID, requestID).
		Scan(&route.Channel, &route.ConfigVersion, &route.ChannelBindingID, &route.ExternalAccountID, &route.ExternalMessageID,
			&route.ExternalChatID, &route.ExternalUserID)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.ReplyRoute{}, runtime.ErrNotFound
	}
	return route, err
}

func resultAAD(record messaging.ResultRecord) []byte {
	value := record.TenantID + "\x00" + record.RequestID + "\x00" + record.ResultRef + "\x00" + record.ContentDigest
	if record.ContentType != "" && record.ContentType != messaging.ContentTypeText {
		value += "\x00" + record.ContentType
	}
	return []byte(value)
}

func toolResultAAD(record messaging.ToolResultRecord) []byte {
	return []byte(record.TenantID + "\x00" + record.GrantID + "\x00" + record.RequestID + "\x00" + record.ResultRef + "\x00" + record.ContentDigest)
}

func interactionAAD(record messaging.InteractionRecord) []byte {
	value := record.TenantID + "\x00" + record.RequestID + "\x00" + record.ContentRef + "\x00" + record.ContentDigest
	if record.ContentType != "" && record.ContentType != messaging.ContentTypeText {
		value += "\x00" + record.ContentType
	}
	return []byte(value)
}

func payloadAAD(record messaging.PayloadRecord) []byte {
	return []byte(record.TenantID + "\x00" + record.RequestID + "\x00" + record.PayloadRef + "\x00" + record.ContentDigest)
}

func encryptPayload(key, aad, plaintext []byte) ([]byte, []byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, runtime.ErrCapabilityUnsupported
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := cryptorand.Read(nonce); err != nil {
		return nil, nil, err
	}
	return aead.Seal(nil, nonce, plaintext, aad), nonce, nil
}

func decryptPayload(key, aad, ciphertext, nonce []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, runtime.ErrVersionMismatch
	}
	return plaintext, nil
}

func (s *Store) ClaimInbox(ctx context.Context, in messaging.ClaimInboxRequest) (messaging.InboxRecord, error) {
	if in.TenantID == "" || in.Channel == "" || in.ExternalAccountID == "" || in.ExternalMessageID == "" {
		return messaging.InboxRecord{}, runtime.ErrTenantScope
	}
	if in.AgentAppID == "" || in.PayloadDigest == "" || in.KeyVersion < 1 {
		return messaging.InboxRecord{}, runtime.ErrCommitConflict
	}
	requestID, payloadRef := messaging.StableInboxIdentity(in.InboxKey)
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id,channel,external_account_id,external_message_id,
request_id,agent_app_id,COALESCE(session_id,''),external_chat_id,external_user_id,COALESCE(input_seq,0),state,payload_ref,payload_digest,
key_version,version,COALESCE(terminal_reason,''),COALESCE(result_ref,''),created_at,updated_at
FROM claim_channel_inbox($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		in.TenantID, in.Channel, in.ExternalAccountID, in.ExternalMessageID, requestID,
		in.AgentAppID, nullable(in.SessionID), in.ExternalChatID, in.ExternalUserID, payloadRef, in.PayloadDigest, in.KeyVersion, string(in.InitialState))
	record, err := scanInbox(row)
	return record, translate(err)
}

func (s *Store) GetInbox(ctx context.Context, key messaging.InboxKey) (messaging.InboxRecord, error) {
	row := s.db.QueryRowContext(ctx, `SELECT tenant_id,channel,external_account_id,external_message_id,
request_id,agent_app_id,COALESCE(session_id,''),external_chat_id,external_user_id,COALESCE(input_seq,0),state,payload_ref,payload_digest,
key_version,version,COALESCE(terminal_reason,''),COALESCE(result_ref,''),created_at,updated_at
FROM inbox WHERE tenant_id=$1 AND channel=$2 AND external_account_id=$3 AND external_message_id=$4`,
		key.TenantID, key.Channel, key.ExternalAccountID, key.ExternalMessageID)
	return scanInbox(row)
}

type rowScanner interface{ Scan(...any) error }

func scanInbox(row rowScanner) (messaging.InboxRecord, error) {
	var record messaging.InboxRecord
	err := row.Scan(&record.TenantID, &record.Channel, &record.ExternalAccountID, &record.ExternalMessageID,
		&record.RequestID, &record.AgentAppID, &record.SessionID, &record.ExternalChatID, &record.ExternalUserID, &record.InputSeq, &record.State,
		&record.PayloadRef, &record.PayloadDigest, &record.KeyVersion, &record.Version,
		&record.TerminalReason, &record.ResultRef, &record.CreatedAt, &record.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.InboxRecord{}, runtime.ErrNotFound
	}
	return record, err
}

func (s *Store) ClaimOutbox(ctx context.Context, kind string, limit int, owner string, until time.Time) ([]messaging.OutboxRecord, error) {
	if kind == "" || limit < 1 || owner == "" || until.IsZero() {
		return nil, runtime.ErrCommitConflict
	}
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
  SELECT tenant_id,outbox_id FROM outbox
  WHERE kind=$1 AND ((state IN ('pending','retry_wait') AND next_attempt_at<=now()) OR (state='claimed' AND claim_until<now()))
  ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT $2
)
UPDATE outbox o SET state='claimed',claim_owner=$3,claim_until=$4,version=o.version+1,attempt=o.attempt+1
FROM candidates c WHERE o.tenant_id=c.tenant_id AND o.outbox_id=c.outbox_id
RETURNING o.tenant_id,o.outbox_id,o.kind,o.aggregate_id,o.event_seq,o.idempotency_key,o.payload_ref,
o.traceparent,o.state,o.version,o.attempt,o.next_attempt_at,o.claim_owner,o.claim_until,o.created_at`, kind, limit, owner, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []messaging.OutboxRecord
	for rows.Next() {
		var record messaging.OutboxRecord
		var trace sql.NullString
		if err := rows.Scan(&record.TenantID, &record.OutboxID, &record.Kind, &record.AggregateID,
			&record.EventSeq, &record.IdempotencyKey, &record.PayloadRef, &trace, &record.State,
			&record.Version, &record.Attempt, &record.NextAttemptAt, &record.ClaimOwner, &record.ClaimUntil, &record.CreatedAt); err != nil {
			return nil, err
		}
		record.TraceParent = trace.String
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, tenantID, outboxID string, expectedVersion uint64) error {
	result, err := s.db.ExecContext(ctx, `UPDATE outbox SET state='published',published_at=now(),claim_owner=NULL,claim_until=NULL,version=version+1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed'`, tenantID, outboxID, expectedVersion)
	return casResult(result, err)
}

func (s *Store) RenewOutboxClaim(ctx context.Context, tenantID, outboxID string, expectedVersion uint64, owner string, until time.Time) (uint64, error) {
	if owner == "" || until.IsZero() {
		return 0, runtime.ErrCommitConflict
	}
	var version uint64
	err := s.db.QueryRowContext(ctx, `UPDATE outbox SET claim_until=$5,version=version+1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed' AND claim_owner=$4
RETURNING version`, tenantID, outboxID, expectedVersion, owner, until).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, runtime.ErrVersionConflict
	}
	return version, err
}

func (s *Store) MarkRetry(ctx context.Context, tenantID, outboxID string, expectedVersion uint64, next time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE outbox SET state='retry_wait',next_attempt_at=$4,claim_owner=NULL,claim_until=NULL,version=version+1
WHERE tenant_id=$1 AND outbox_id=$2 AND version=$3 AND state='claimed'`, tenantID, outboxID, expectedVersion, next)
	return casResult(result, err)
}

func (s *Store) FindReconciliationIssues(ctx context.Context, before time.Time, limit int) ([]messaging.ReconciliationIssue, error) {
	if before.IsZero() || limit < 1 {
		return nil, runtime.ErrCommitConflict
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT kind,tenant_id,aggregate_id,ref_id,version FROM (
  SELECT 'stuck_inbox'::text AS kind,i.tenant_id,i.request_id AS aggregate_id,i.request_id AS ref_id,i.version
  FROM inbox i WHERE i.state IN ('preprocess_pending','dispatch_pending') AND i.updated_at<$1
  UNION ALL
  SELECT 'expired_outbox_claim',o.tenant_id,o.aggregate_id,o.outbox_id,o.version
  FROM outbox o WHERE o.state='claimed' AND o.claim_until<$1
  UNION ALL
  SELECT 'parked_input',e.tenant_id,e.request_id,e.request_id,e.version
  FROM execution_record e WHERE e.outcome='pending' AND e.not_before<$1
  UNION ALL
  SELECT 'missing_reply_outbox',c.tenant_id,c.request_id,c.commit_id,c.session_version
  FROM session_commit c
  WHERE c.outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')
    AND c.result_ref IS NOT NULL AND c.created_at<$1
    AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.tenant_id=c.tenant_id AND o.kind='reply' AND o.aggregate_id=c.request_id)
  UNION ALL
  SELECT 'stuck_delivery',d.tenant_id,d.delivery_key,d.segment_no::text,d.version
  FROM delivery_ledger d WHERE d.state='sending' AND d.updated_at<$1
  UNION ALL
  SELECT 'ambiguous_delivery',d.tenant_id,d.delivery_key,d.segment_no::text,d.version
  FROM delivery_ledger d WHERE d.state='ambiguous' AND d.updated_at<$1
) issues ORDER BY kind,tenant_id,aggregate_id LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var issues []messaging.ReconciliationIssue
	for rows.Next() {
		var issue messaging.ReconciliationIssue
		if err := rows.Scan(&issue.Kind, &issue.TenantID, &issue.AggregateID, &issue.RefID, &issue.Version); err != nil {
			return nil, err
		}
		issues = append(issues, issue)
	}
	return issues, rows.Err()
}

func (s *Store) GetDelivery(ctx context.Context, key messaging.DeliveryKey) (messaging.DeliveryRecord, error) {
	var record messaging.DeliveryRecord
	record.DeliveryKey = key
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(provider_message_id,''),state,renderer_version,format_version,content_digest,segment_count,
attempt,reconcile_attempt,not_before,COALESCE(last_error_class,''),client_request_id,COALESCE(claim_owner,''),COALESCE(claim_until,'epoch'::timestamptz),version,updated_at FROM delivery_ledger
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3`, key.TenantID, key.DeliveryKey, key.SegmentNo).
		Scan(&record.ProviderMessageID, &record.State, &record.Plan.RendererVersion, &record.Plan.FormatVersion,
			&record.Plan.ContentDigest, &record.Plan.SegmentCount, &record.Attempt, &record.ReconcileAttempt, &record.NotBefore,
			&record.LastErrorClass, &record.ClientRequestID, &record.ClaimOwner, &record.ClaimUntil, &record.Version, &record.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrNotFound
	}
	return record, err
}

func (s *Store) ClaimDelivery(ctx context.Context, key messaging.DeliveryKey, plan messaging.DeliveryPlan, claim messaging.DeliveryClaim) (messaging.DeliveryRecord, bool, error) {
	if err := validateDeliveryPlan(key, plan); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if claim.Owner == "" || claim.TTL <= 0 {
		return messaging.DeliveryRecord{}, false, runtime.ErrCommitConflict
	}
	clientRequestID, err := messaging.StableDeliveryRequestID(key)
	if err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE delivery_ledger SET state='ambiguous',last_error_class='owner_lost',
claim_owner=NULL,claim_until=NULL,version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND state='sending' AND claim_until<=now()`, key.TenantID, key.DeliveryKey, key.SegmentNo); err != nil {
		return messaging.DeliveryRecord{}, false, err
	}
	record := messaging.DeliveryRecord{DeliveryKey: key, Plan: plan, ClientRequestID: clientRequestID}
	err = s.db.QueryRowContext(ctx, `INSERT INTO delivery_ledger(
tenant_id,delivery_key,segment_no,state,renderer_version,format_version,content_digest,segment_count,attempt,client_request_id,claim_owner,claim_until)
VALUES($1,$2,$3,'sending',$4,$5,$6,$7,1,$8,$9,now()+($10 * interval '1 microsecond'))
ON CONFLICT (tenant_id,delivery_key,segment_no) DO UPDATE SET
  state='sending',attempt=delivery_ledger.attempt+1,claim_owner=EXCLUDED.claim_owner,claim_until=EXCLUDED.claim_until,
  version=delivery_ledger.version+1,updated_at=now()
WHERE delivery_ledger.state IN ('pending','retry_wait') AND delivery_ledger.not_before<=now()
  AND delivery_ledger.renderer_version=EXCLUDED.renderer_version
  AND delivery_ledger.format_version=EXCLUDED.format_version
  AND delivery_ledger.content_digest=EXCLUDED.content_digest
  AND delivery_ledger.segment_count=EXCLUDED.segment_count
  AND delivery_ledger.client_request_id=EXCLUDED.client_request_id
RETURNING COALESCE(provider_message_id,''),state,attempt,reconcile_attempt,not_before,COALESCE(last_error_class,''),client_request_id,
COALESCE(claim_owner,''),COALESCE(claim_until,'epoch'::timestamptz),version,updated_at`,
		key.TenantID, key.DeliveryKey, key.SegmentNo, plan.RendererVersion, plan.FormatVersion, plan.ContentDigest, plan.SegmentCount,
		clientRequestID, claim.Owner, claim.TTL.Microseconds()).
		Scan(&record.ProviderMessageID, &record.State, &record.Attempt, &record.ReconcileAttempt, &record.NotBefore, &record.LastErrorClass,
			&record.ClientRequestID, &record.ClaimOwner, &record.ClaimUntil, &record.Version, &record.UpdatedAt)
	if err == nil {
		return record, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, false, err
	}
	existing, getErr := s.GetDelivery(ctx, key)
	if getErr != nil {
		return messaging.DeliveryRecord{}, false, getErr
	}
	if existing.Plan != plan {
		return messaging.DeliveryRecord{}, false, runtime.ErrIdempotencyCollision
	}
	return existing, false, nil
}

func (s *Store) RenewDeliveryClaim(ctx context.Context, in messaging.DeliveryRecord, ttl time.Duration) (messaging.DeliveryRecord, error) {
	if ttl <= 0 || in.ClaimOwner == "" || in.ClientRequestID == "" {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET claim_until=now()+($7 * interval '1 microsecond'),version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$4 AND state='sending' AND claim_owner=$5
  AND client_request_id=$6 AND claim_until>now()
RETURNING claim_until,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, in.Version,
		in.ClaimOwner, in.ClientRequestID, ttl.Microseconds()).Scan(&in.ClaimUntil, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	return in, err
}

func (s *Store) FinishDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryAmbiguous && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	if in.NotBefore.IsZero() {
		in.NotBefore = time.Now().UTC()
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET provider_message_id=$4,state=$5,not_before=$6,last_error_class=$7,
claim_owner=NULL,claim_until=NULL,version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$8 AND state='sending' AND claim_owner=$9 AND client_request_id=$10
RETURNING attempt,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, nullable(in.ProviderMessageID),
		in.State, in.NotBefore, nullable(in.LastErrorClass), expectedVersion, in.ClaimOwner, in.ClientRequestID).
		Scan(&in.Attempt, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	return in, err
}

func (s *Store) ReconcileDelivery(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if in.State != messaging.DeliverySent && in.State != messaging.DeliveryRetryWait && in.State != messaging.DeliveryFailed {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	if in.NotBefore.IsZero() {
		in.NotBefore = time.Now().UTC()
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET provider_message_id=$4,state=$5,not_before=$6,last_error_class=$7,
version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$8 AND state='ambiguous' AND client_request_id=$9
RETURNING attempt,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, nullable(in.ProviderMessageID),
		in.State, in.NotBefore, nullable(in.LastErrorClass), expectedVersion, in.ClientRequestID).
		Scan(&in.Attempt, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	in.ClaimOwner, in.ClaimUntil = "", time.Time{}
	return in, err
}

func (s *Store) DeferDeliveryReconciliation(ctx context.Context, in messaging.DeliveryRecord, expectedVersion int64) (messaging.DeliveryRecord, error) {
	if in.State != messaging.DeliveryAmbiguous || in.ReconcileAttempt < 1 || in.NotBefore.IsZero() {
		return messaging.DeliveryRecord{}, runtime.ErrCommitConflict
	}
	err := s.db.QueryRowContext(ctx, `UPDATE delivery_ledger SET reconcile_attempt=$4,not_before=$5,last_error_class=$6,
version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$7 AND state='ambiguous'
  AND client_request_id=$8 AND reconcile_attempt=$4-1
RETURNING attempt,version,updated_at`, in.TenantID, in.DeliveryKey.DeliveryKey, in.SegmentNo, in.ReconcileAttempt,
		in.NotBefore, nullable(in.LastErrorClass), expectedVersion, in.ClientRequestID).
		Scan(&in.Attempt, &in.Version, &in.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.DeliveryRecord{}, runtime.ErrVersionConflict
	}
	return in, err
}

func validateDeliveryPlan(key messaging.DeliveryKey, plan messaging.DeliveryPlan) error {
	if key.TenantID == "" || key.DeliveryKey == "" || key.SegmentNo < 0 {
		return runtime.ErrTenantScope
	}
	if plan.RendererVersion == "" || plan.FormatVersion == "" || plan.ContentDigest == "" || plan.SegmentCount < 1 || key.SegmentNo >= plan.SegmentCount {
		return runtime.ErrCommitConflict
	}
	return nil
}

func casResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if err == nil {
		return nil
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "42501":
			return runtime.ErrTenantScope
		case "P0002":
			return runtime.ErrNotFound
		case "40001":
			return runtime.ErrVersionConflict
		}
	}
	return err
}

var _ messaging.InboxClaimer = (*Store)(nil)
var _ messaging.PayloadStore = (*Store)(nil)
var _ messaging.PreparedPayloadStore = (*Store)(nil)
var _ messaging.ResultStore = (*Store)(nil)
var _ messaging.ToolResultStore = (*Store)(nil)
var _ messaging.InteractionStore = (*Store)(nil)
var _ messaging.ReplyRouteStore = (*Store)(nil)
var _ messaging.OutboxStore = (*Store)(nil)
var _ messaging.ReconciliationStore = (*Store)(nil)
var _ messaging.DeliveryLedger = (*Store)(nil)
