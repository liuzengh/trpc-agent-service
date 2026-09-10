package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// IdentityMappingRequest contains provider identifiers at the controlled
// mapping boundary. The mapper normalizes lookup IDs and protects provider
// targets before returning a platform principal; callers must not copy either
// value into Gateway, Runner, or Session data.
type IdentityMappingRequest struct {
	Scope                      tenant.Scope
	BindingID                  string
	Channel                    channels.Channel
	Kind                       channels.ConversationKind
	ExternalSenderID           string
	ExternalChatID             string
	ExternalThreadID           string
	ProviderSenderTarget       string
	ProviderConversationTarget string
	ProviderThreadTarget       string
}

// Validate checks the fields required by the selected direct, group, or topic
// mapping shape.
func (r IdentityMappingRequest) Validate() error {
	if err := r.Scope.Validate(); err != nil {
		return fmt.Errorf("mapping scope: %w", err)
	}
	if r.BindingID == "" {
		return errors.New("mapping binding_id is required")
	}
	if err := r.Channel.Validate(); err != nil {
		return fmt.Errorf("mapping channel: %w", err)
	}
	if err := r.Kind.Validate(); err != nil {
		return err
	}
	if err := requireExternalID(r.ExternalSenderID, "external sender id"); err != nil {
		return err
	}
	if err := requireProviderTarget(r.ProviderSenderTarget, "provider sender target"); err != nil {
		return err
	}
	switch r.Kind {
	case channels.ConversationDirect:
		if r.ExternalChatID != "" || r.ExternalThreadID != "" || r.ProviderConversationTarget != "" || r.ProviderThreadTarget != "" {
			return errors.New("direct mapping contains conversation fields")
		}
	case channels.ConversationGroup:
		if err := requireExternalID(r.ExternalChatID, "external chat id"); err != nil {
			return err
		}
		if r.ExternalThreadID != "" || r.ProviderThreadTarget != "" {
			return errors.New("group mapping contains topic fields")
		}
		if err := requireProviderTarget(r.ProviderConversationTarget, "provider conversation target"); err != nil {
			return err
		}
	case channels.ConversationTopic:
		if err := requireExternalID(r.ExternalChatID, "external chat id"); err != nil {
			return err
		}
		if err := requireExternalID(r.ExternalThreadID, "external thread id"); err != nil {
			return err
		}
		if err := requireProviderTarget(r.ProviderThreadTarget, "provider thread target"); err != nil {
			return err
		}
		if r.ProviderConversationTarget != "" {
			if err := requireProviderTarget(r.ProviderConversationTarget, "provider conversation target"); err != nil {
				return err
			}
		}
	}
	return nil
}

// IdentityMapper persists binding-scoped external identities and conversation
// principals. Each Map call uses one PostgreSQL transaction for the mapping
// rows.
type IdentityMapper struct {
	store               *Store
	hasher              channels.ExternalIDHasher
	protector           channels.TargetProtector
	acceptedKeyVersions []string
}

// NewIdentityMapper creates a PostgreSQL-backed identity mapper.
func NewIdentityMapper(
	store *Store,
	hasher channels.ExternalIDHasher,
	protector channels.TargetProtector,
	acceptedKeyVersions []string,
) (*IdentityMapper, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	if hasher == nil {
		return nil, errors.New("external id hasher is required")
	}
	if protector == nil {
		return nil, errors.New("target protector is required")
	}
	if len(acceptedKeyVersions) == 0 {
		return nil, errors.New("at least one accepted external id key version is required")
	}
	versions := make([]string, 0, len(acceptedKeyVersions))
	seen := make(map[string]struct{}, len(acceptedKeyVersions))
	for _, version := range acceptedKeyVersions {
		if version == "" {
			return nil, errors.New("accepted external id key version is required")
		}
		if _, ok := seen[version]; ok {
			continue
		}
		seen[version] = struct{}{}
		versions = append(versions, version)
	}
	return &IdentityMapper{
		store:               store,
		hasher:              hasher,
		protector:           protector,
		acceptedKeyVersions: versions,
	}, nil
}

// Map resolves or creates the Identity and, for group/topic messages, the
// Conversation for one provider message.
func (m *IdentityMapper) Map(ctx context.Context, request IdentityMappingRequest) (channels.MappedPrincipal, error) {
	if m == nil || m.store == nil || m.hasher == nil || m.protector == nil {
		return channels.MappedPrincipal{}, errors.New("identity mapper is not initialized")
	}
	if err := m.store.validate(); err != nil {
		return channels.MappedPrincipal{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return channels.MappedPrincipal{}, err
	}
	tx, err := m.store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return channels.MappedPrincipal{}, fmt.Errorf("begin identity mapping: %w", err)
	}
	defer func() { rollback(tx) }()

	mapped, err := m.mapInTransaction(ctx, tx, request)
	if err != nil {
		return channels.MappedPrincipal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return channels.MappedPrincipal{}, fmt.Errorf("commit identity mapping: %w", err)
	}
	return mapped, nil
}

type externalKeyHash struct {
	hash       string
	keyVersion string
}

type conversationKey struct {
	chatHash   string
	threadHash string
	keyVersion string
}

func (m *IdentityMapper) externalIDHashes(
	ctx context.Context,
	request IdentityMappingRequest,
	kind channels.ExternalIDKind,
	externalID string,
) ([]externalKeyHash, error) {
	activeHash, activeVersion, err := m.hasher.Hash(
		ctx,
		request.Scope,
		request.BindingID,
		kind,
		externalID,
	)
	if err != nil {
		return nil, err
	}
	if activeVersion == "" {
		return nil, errors.New("active external id key version is required")
	}
	keyHashes := []externalKeyHash{{hash: activeHash, keyVersion: activeVersion}}
	for _, version := range m.acceptedKeyVersions {
		if version == activeVersion {
			continue
		}
		candidateHash, err := m.hasher.HashWithVersion(
			ctx,
			request.Scope,
			request.BindingID,
			kind,
			externalID,
			version,
		)
		if err != nil {
			return nil, err
		}
		keyHashes = append(keyHashes, externalKeyHash{hash: candidateHash, keyVersion: version})
	}
	return keyHashes, nil
}

func (m *IdentityMapper) mapInTransaction(
	ctx context.Context,
	tx pgx.Tx,
	request IdentityMappingRequest,
) (channels.MappedPrincipal, error) {
	if tx == nil {
		return channels.MappedPrincipal{}, errors.New("identity mapping transaction is required")
	}
	request, err := normalizeIdentityMappingRequest(request)
	if err != nil {
		return channels.MappedPrincipal{}, err
	}
	if err := validateMappingBindingTx(ctx, tx, request); err != nil {
		return channels.MappedPrincipal{}, err
	}
	return m.mapNormalizedTx(ctx, tx, request)
}

// channelPayloadHash returns the stable semantic hash used by channel Inbox
// idempotency. It contains scoped digests of normalized provider identifiers,
// not raw identifiers, and excludes internal principal IDs, targets,
// timestamps, and route authorization snapshots. The digests are deliberately
// independent of the rotating lookup HMAC key so provider retries remain
// idempotent across key rotation.
func (m *IdentityMapper) channelPayloadHash(
	ctx context.Context,
	request IdentityMappingRequest,
	input channels.ChannelInput,
) ([sha256.Size]byte, error) {
	if m == nil || m.hasher == nil {
		return [sha256.Size]byte{}, errors.New("identity mapper is not initialized")
	}
	if err := input.Validate(); err != nil {
		return [sha256.Size]byte{}, err
	}
	normalized, err := normalizeIdentityMappingRequest(request)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	payload := channelPayload{
		TenantID:  normalized.Scope.TenantID,
		AppID:     normalized.Scope.AppID,
		BindingID: normalized.BindingID,
		Channel:   normalized.Channel,
		SenderKeyHash: stableChannelIDDigest(
			normalized.Scope,
			normalized.BindingID,
			channels.ExternalIDUser,
			normalized.ExternalSenderID,
		),
		MessageType:  input.MessageType,
		Text:         input.Text,
		ArtifactRefs: append([]string{}, input.ArtifactRefs...),
	}
	if normalized.Kind != channels.ConversationDirect {
		threadKind := channels.ExternalIDNoThread
		threadID := channels.NoThreadExternalID
		if normalized.Kind == channels.ConversationTopic {
			threadKind = channels.ExternalIDThread
			threadID = normalized.ExternalThreadID
		}
		payload.ConversationKind = normalized.Kind
		payload.ChatKeyHash = stableChannelIDDigest(
			normalized.Scope,
			normalized.BindingID,
			channels.ExternalIDChat,
			normalized.ExternalChatID,
		)
		payload.ThreadKeyHash = stableChannelIDDigest(
			normalized.Scope,
			normalized.BindingID,
			threadKind,
			threadID,
		)
	} else {
		payload.ConversationKind = normalized.Kind
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("marshal channel payload: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func stableChannelIDDigest(
	scope tenant.Scope,
	bindingID string,
	kind channels.ExternalIDKind,
	externalID string,
) string {
	canonical := struct {
		TenantID  string                  `json:"tenant_id"`
		AppID     string                  `json:"app_id"`
		BindingID string                  `json:"binding_id"`
		Kind      channels.ExternalIDKind `json:"kind"`
		External  string                  `json:"external_id"`
	}{
		TenantID:  scope.TenantID,
		AppID:     scope.AppID,
		BindingID: bindingID,
		Kind:      kind,
		External:  externalID,
	}
	encoded, _ := json.Marshal(canonical)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

type channelPayload struct {
	TenantID         string                    `json:"tenant_id"`
	AppID            string                    `json:"app_id"`
	BindingID        string                    `json:"binding_id"`
	Channel          channels.Channel          `json:"channel"`
	ConversationKind channels.ConversationKind `json:"conversation_kind"`
	SenderKeyHash    string                    `json:"sender_key_hash"`
	ChatKeyHash      string                    `json:"chat_key_hash,omitempty"`
	ThreadKeyHash    string                    `json:"thread_key_hash,omitempty"`
	MessageType      channels.MessageType      `json:"message_type"`
	Text             string                    `json:"text"`
	ArtifactRefs     []string                  `json:"artifact_refs"`
}

func (m *IdentityMapper) mapNormalizedTx(
	ctx context.Context,
	tx pgx.Tx,
	request IdentityMappingRequest,
) (channels.MappedPrincipal, error) {
	userHashes, err := m.externalIDHashes(ctx, request, channels.ExternalIDUser, request.ExternalSenderID)
	if err != nil {
		return channels.MappedPrincipal{}, fmt.Errorf("hash external sender id: %w", err)
	}
	identity, err := m.resolveIdentityTx(ctx, tx, request, userHashes)
	if err != nil {
		return channels.MappedPrincipal{}, err
	}
	mapped := channels.MappedPrincipal{
		Identity:           identity,
		SessionPrincipalID: identity.UserID,
		SessionID:          channels.DefaultSessionID,
	}
	if request.Kind == channels.ConversationDirect {
		if err := mapped.Validate(); err != nil {
			return channels.MappedPrincipal{}, err
		}
		return mapped, nil
	}

	chatHashes, err := m.externalIDHashes(ctx, request, channels.ExternalIDChat, request.ExternalChatID)
	if err != nil {
		return channels.MappedPrincipal{}, fmt.Errorf("hash external chat id: %w", err)
	}
	threadKind := channels.ExternalIDNoThread
	threadID := channels.NoThreadExternalID
	if request.Kind == channels.ConversationTopic {
		threadKind = channels.ExternalIDThread
		threadID = request.ExternalThreadID
	}
	threadHashes, err := m.externalIDHashes(ctx, request, threadKind, threadID)
	if err != nil {
		return channels.MappedPrincipal{}, fmt.Errorf("hash external thread id: %w", err)
	}
	conversationHashes := make([]conversationKey, 0, len(chatHashes))
	for _, chatHash := range chatHashes {
		for _, threadHash := range threadHashes {
			if chatHash.keyVersion != threadHash.keyVersion {
				continue
			}
			conversationHashes = append(conversationHashes, conversationKey{
				chatHash:   chatHash.hash,
				threadHash: threadHash.hash,
				keyVersion: chatHash.keyVersion,
			})
		}
	}
	if len(conversationHashes) == 0 {
		return channels.MappedPrincipal{}, errors.New("conversation external id key versions do not match")
	}
	conversation, err := m.resolveConversationTx(
		ctx,
		tx,
		request,
		conversationHashes,
	)
	if err != nil {
		return channels.MappedPrincipal{}, err
	}
	mapped.Conversation = &conversation
	mapped.SessionPrincipalID = conversation.SessionPrincipalID
	if err := mapped.Validate(); err != nil {
		return channels.MappedPrincipal{}, err
	}
	return mapped, nil
}

func (m *IdentityMapper) resolveIdentityTx(
	ctx context.Context,
	tx pgx.Tx,
	request IdentityMappingRequest,
	keyHashes []externalKeyHash,
) (channels.Identity, error) {
	if len(keyHashes) == 0 {
		return channels.Identity{}, errors.New("external sender hashes are required")
	}
	for _, keyHash := range keyHashes {
		digest, err := decodeHash(keyHash.hash)
		if err != nil {
			return channels.Identity{}, fmt.Errorf("external sender hash: %w", err)
		}
		identity, found, err := findIdentityTx(ctx, tx, request, digest)
		if err != nil {
			return channels.Identity{}, err
		}
		if found {
			if identity.Status != channels.IdentityActive {
				return channels.Identity{}, fmt.Errorf("channel identity status %q: %w", identity.Status, channels.ErrIdentityInactive)
			}
			return identity, nil
		}
	}
	activeHash := keyHashes[0]
	digest, err := decodeHash(activeHash.hash)
	if err != nil {
		return channels.Identity{}, fmt.Errorf("external sender hash: %w", err)
	}
	userID := uuid.NewString()
	target, err := m.protector.Seal(ctx, channels.TargetContext{
		Scope:            request.Scope,
		BindingID:        request.BindingID,
		Channel:          request.Channel,
		EntityType:       channels.TargetEntityIdentity,
		InternalEntityID: userID,
	}, channels.TargetPurposeIdentityUser, channels.TargetPlaintext{
		Version:        channels.TargetVersion,
		Channel:        request.Channel,
		TargetKind:     channels.TargetKindUser,
		ExternalUserID: request.ExternalSenderID,
		ProviderTarget: request.ProviderSenderTarget,
	})
	if err != nil {
		return channels.Identity{}, fmt.Errorf("seal identity target: %w", err)
	}
	if err := target.Validate(); err != nil {
		return channels.Identity{}, err
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return channels.Identity{}, fmt.Errorf("marshal identity target: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.channel_identity (
    tenant_id, app_id, binding_id, channel, external_user_key_hash,
    user_id, status, key_version, provider_target_envelope
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, app_id, binding_id, external_user_key_hash) DO NOTHING`,
		request.Scope.TenantID,
		request.Scope.AppID,
		request.BindingID,
		request.Channel,
		digest,
		userID,
		channels.IdentityActive,
		activeHash.keyVersion,
		targetJSON,
	); err != nil {
		return channels.Identity{}, fmt.Errorf("create channel identity: %w", err)
	}
	identity, found, err := findIdentityTx(ctx, tx, request, digest)
	if err != nil {
		return channels.Identity{}, err
	}
	if !found {
		return channels.Identity{}, errors.New("channel identity was not available after insert")
	}
	if identity.Status != channels.IdentityActive {
		return channels.Identity{}, fmt.Errorf("channel identity status %q: %w", identity.Status, channels.ErrIdentityInactive)
	}
	return identity, nil
}

func (m *IdentityMapper) resolveConversationTx(
	ctx context.Context,
	tx pgx.Tx,
	request IdentityMappingRequest,
	keyHashes []conversationKey,
) (channels.Conversation, error) {
	if len(keyHashes) == 0 {
		return channels.Conversation{}, errors.New("conversation hashes are required")
	}
	for _, keyHash := range keyHashes {
		chatDigest, err := decodeHash(keyHash.chatHash)
		if err != nil {
			return channels.Conversation{}, fmt.Errorf("external chat hash: %w", err)
		}
		threadDigest, err := decodeHash(keyHash.threadHash)
		if err != nil {
			return channels.Conversation{}, fmt.Errorf("external thread hash: %w", err)
		}
		conversation, found, err := findConversationTx(ctx, tx, request, chatDigest, threadDigest)
		if err != nil {
			return channels.Conversation{}, err
		}
		if found {
			return conversation, nil
		}
	}
	activeHash := keyHashes[0]
	chatDigest, err := decodeHash(activeHash.chatHash)
	if err != nil {
		return channels.Conversation{}, fmt.Errorf("external chat hash: %w", err)
	}
	threadDigest, err := decodeHash(activeHash.threadHash)
	if err != nil {
		return channels.Conversation{}, fmt.Errorf("external thread hash: %w", err)
	}
	conversationID := uuid.NewString()
	purpose := channels.TargetPurposeConversationChat
	targetKind := channels.TargetKindConversation
	providerTarget := request.ProviderConversationTarget
	if request.Kind == channels.ConversationTopic {
		purpose = channels.TargetPurposeConversationTopic
		targetKind = channels.TargetKindTopic
		providerTarget = request.ProviderThreadTarget
	}
	target, err := m.protector.Seal(ctx, channels.TargetContext{
		Scope:            request.Scope,
		BindingID:        request.BindingID,
		Channel:          request.Channel,
		EntityType:       channels.TargetEntityConversation,
		InternalEntityID: conversationID,
	}, purpose, channels.TargetPlaintext{
		Version:          channels.TargetVersion,
		Channel:          request.Channel,
		TargetKind:       targetKind,
		ExternalChatID:   request.ExternalChatID,
		ExternalThreadID: request.ExternalThreadID,
		ProviderTarget:   providerTarget,
	})
	if err != nil {
		return channels.Conversation{}, fmt.Errorf("seal conversation target: %w", err)
	}
	if err := target.Validate(); err != nil {
		return channels.Conversation{}, err
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return channels.Conversation{}, fmt.Errorf("marshal conversation target: %w", err)
	}
	scope := channels.ConversationScopeGroup
	if request.Kind == channels.ConversationTopic {
		scope = channels.ConversationScopeTopic
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.channel_conversation (
    tenant_id, app_id, binding_id, channel, external_chat_key_hash,
    thread_key_hash, conversation_id, session_principal_id, scope,
    key_version, provider_target_envelope
) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10)
ON CONFLICT (tenant_id, app_id, binding_id, external_chat_key_hash, thread_key_hash) DO NOTHING`,
		request.Scope.TenantID,
		request.Scope.AppID,
		request.BindingID,
		request.Channel,
		chatDigest,
		threadDigest,
		conversationID,
		scope,
		activeHash.keyVersion,
		targetJSON,
	); err != nil {
		return channels.Conversation{}, fmt.Errorf("create channel conversation: %w", err)
	}
	conversation, found, err := findConversationTx(ctx, tx, request, chatDigest, threadDigest)
	if err != nil {
		return channels.Conversation{}, err
	}
	if !found {
		return channels.Conversation{}, errors.New("channel conversation was not available after insert")
	}
	return conversation, nil
}

func validateMappingBindingTx(
	ctx context.Context,
	tx pgx.Tx,
	request IdentityMappingRequest,
) error {
	binding, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
FOR UPDATE`, request.Scope.TenantID, request.Scope.AppID, request.BindingID))
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("channel binding: %w", ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("lock channel binding for identity mapping: %w", err)
	}
	if binding.Channel != request.Channel {
		return channels.ErrBindingChannelMismatch
	}
	if binding.Status != channels.BindingActive {
		return channels.ErrBindingInactive
	}
	return nil
}

func findIdentityTx(
	ctx context.Context,
	tx pgx.Tx,
	request IdentityMappingRequest,
	digest []byte,
) (channels.Identity, bool, error) {
	var identity channels.Identity
	var storedDigest, targetJSON []byte
	err := tx.QueryRow(ctx, `
SELECT tenant_id, app_id, binding_id, channel, external_user_key_hash,
       user_id, status, key_version, provider_target_envelope
FROM platform.channel_identity
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_user_key_hash = $4`,
		request.Scope.TenantID,
		request.Scope.AppID,
		request.BindingID,
		digest,
	).Scan(
		&identity.TenantID,
		&identity.AppID,
		&identity.BindingID,
		&identity.Channel,
		&storedDigest,
		&identity.UserID,
		&identity.Status,
		&identity.KeyVersion,
		&targetJSON,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return channels.Identity{}, false, nil
	}
	if err != nil {
		return channels.Identity{}, false, fmt.Errorf("resolve channel identity: %w", err)
	}
	identity.ExternalUserKeyHash = hex.EncodeToString(storedDigest)
	if err := json.Unmarshal(targetJSON, &identity.ProviderTargetEnvelope); err != nil {
		return channels.Identity{}, false, fmt.Errorf("decode identity target envelope: %w", err)
	}
	if err := identity.ProviderTargetEnvelope.Validate(); err != nil {
		return channels.Identity{}, false, fmt.Errorf("identity target envelope: %w", err)
	}
	return identity, true, nil
}

func findConversationTx(
	ctx context.Context,
	tx pgx.Tx,
	request IdentityMappingRequest,
	chatDigest []byte,
	threadDigest []byte,
) (channels.Conversation, bool, error) {
	var conversation channels.Conversation
	var storedChatDigest, storedThreadDigest, targetJSON []byte
	err := tx.QueryRow(ctx, `
SELECT tenant_id, app_id, binding_id, channel, external_chat_key_hash,
       thread_key_hash, conversation_id, session_principal_id, scope,
       key_version, provider_target_envelope
FROM platform.channel_conversation
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_chat_key_hash = $4 AND thread_key_hash = $5`,
		request.Scope.TenantID,
		request.Scope.AppID,
		request.BindingID,
		chatDigest,
		threadDigest,
	).Scan(
		&conversation.TenantID,
		&conversation.AppID,
		&conversation.BindingID,
		&conversation.Channel,
		&storedChatDigest,
		&storedThreadDigest,
		&conversation.ConversationID,
		&conversation.SessionPrincipalID,
		&conversation.Scope,
		&conversation.KeyVersion,
		&targetJSON,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return channels.Conversation{}, false, nil
	}
	if err != nil {
		return channels.Conversation{}, false, fmt.Errorf("resolve channel conversation: %w", err)
	}
	conversation.ExternalChatKeyHash = hex.EncodeToString(storedChatDigest)
	conversation.ThreadKeyHash = hex.EncodeToString(storedThreadDigest)
	if err := json.Unmarshal(targetJSON, &conversation.ProviderTargetEnvelope); err != nil {
		return channels.Conversation{}, false, fmt.Errorf("decode conversation target envelope: %w", err)
	}
	if err := conversation.ProviderTargetEnvelope.Validate(); err != nil {
		return channels.Conversation{}, false, fmt.Errorf("conversation target envelope: %w", err)
	}
	return conversation, true, nil
}

func normalizeIdentityMappingRequest(request IdentityMappingRequest) (IdentityMappingRequest, error) {
	if err := request.Validate(); err != nil {
		return IdentityMappingRequest{}, err
	}
	var err error
	request.ExternalSenderID, err = channels.NormalizeExternalID(request.ExternalSenderID)
	if err != nil {
		return IdentityMappingRequest{}, fmt.Errorf("external sender id: %w", err)
	}
	if request.Kind == channels.ConversationDirect {
		return request, nil
	}
	request.ExternalChatID, err = channels.NormalizeExternalID(request.ExternalChatID)
	if err != nil {
		return IdentityMappingRequest{}, fmt.Errorf("external chat id: %w", err)
	}
	if request.Kind == channels.ConversationTopic {
		request.ExternalThreadID, err = channels.NormalizeExternalID(request.ExternalThreadID)
		if err != nil {
			return IdentityMappingRequest{}, fmt.Errorf("external thread id: %w", err)
		}
	}
	return request, nil
}

func requireExternalID(value, field string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", field)
	}
	return nil
}

func requireProviderTarget(value, field string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid utf-8", field)
	}
	return nil
}

func decodeHash(value string) ([]byte, error) {
	if len(value) != sha256.Size*2 {
		return nil, errors.New("hash must be a sha256 digest")
	}
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != sha256.Size {
		return nil, errors.New("hash must be a sha256 digest")
	}
	return digest, nil
}
