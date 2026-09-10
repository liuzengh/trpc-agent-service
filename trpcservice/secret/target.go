package secret

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	providerTargetKeyName = "im-provider-target-key"
	externalIDKeyName     = "im-external-id-hmac-key"
	secretKeyBytes        = 32
)

var (
	_ channels.ExternalIDHasher = (*ExternalIDHasher)(nil)
	_ channels.TargetProtector  = (*AEADTargetProtector)(nil)
)

// ExternalIDHasher derives scoped lookup hashes for provider identifiers. The
// raw identifier is used only during the call and is never returned or logged.
type ExternalIDHasher struct {
	provider         SecretProvider
	activeKeyVersion string
}

// NewExternalIDHasher creates an HMAC-SHA-256 external ID hasher backed by a
// versioned tenant-scoped secret.
func NewExternalIDHasher(provider SecretProvider, activeKeyVersion string) (*ExternalIDHasher, error) {
	if provider == nil {
		return nil, errors.New("secret provider is required")
	}
	if activeKeyVersion == "" {
		return nil, errors.New("external id hmac key version is required")
	}
	return &ExternalIDHasher{provider: provider, activeKeyVersion: activeKeyVersion}, nil
}

// Hash returns a lower-case hexadecimal HMAC digest and the key version used
// to derive it.
func (h *ExternalIDHasher) Hash(
	ctx context.Context,
	scope tenant.Scope,
	bindingID string,
	kind channels.ExternalIDKind,
	externalID string,
) (string, string, error) {
	hash, err := h.hashWithVersion(ctx, scope, bindingID, kind, externalID, h.activeKeyVersion)
	if err != nil {
		return "", "", err
	}
	return hash, h.activeKeyVersion, nil
}

// HashWithVersion derives a lookup hash with an explicitly selected accepted
// HMAC key version. It is used to find mappings written before key rotation.
func (h *ExternalIDHasher) HashWithVersion(
	ctx context.Context,
	scope tenant.Scope,
	bindingID string,
	kind channels.ExternalIDKind,
	externalID string,
	keyVersion string,
) (string, error) {
	return h.hashWithVersion(ctx, scope, bindingID, kind, externalID, keyVersion)
}

func (h *ExternalIDHasher) hashWithVersion(
	ctx context.Context,
	scope tenant.Scope,
	bindingID string,
	kind channels.ExternalIDKind,
	externalID string,
	keyVersion string,
) (string, error) {
	if h == nil || h.provider == nil {
		return "", errors.New("external id hasher is not initialized")
	}
	if err := scope.Validate(); err != nil {
		return "", fmt.Errorf("external id scope: %w", err)
	}
	if bindingID == "" {
		return "", errors.New("external id binding_id is required")
	}
	if err := kind.Validate(); err != nil {
		return "", err
	}
	normalized, err := channels.NormalizeExternalID(externalID)
	if err != nil {
		return "", err
	}
	if kind == channels.ExternalIDNoThread && normalized != channels.NoThreadExternalID {
		return "", errors.New("no-thread external id must use the fixed sentinel")
	}
	key, err := resolveSecretKey(ctx, h.provider, scope, externalIDKeyName, keyVersion)
	if err != nil {
		return "", fmt.Errorf("resolve external id hmac key: %w", err)
	}
	input, err := lengthPrefixedStrings(
		scope.TenantID,
		scope.AppID,
		bindingID,
		string(kind),
		normalized,
	)
	if err != nil {
		return "", fmt.Errorf("encode external id hash input: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	if _, err := mac.Write(input); err != nil {
		return "", fmt.Errorf("hash external id: %w", err)
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// AEADTargetProtector encrypts provider targets with AES-256-GCM. Secret
// values are resolved only through the supplied scoped SecretProvider.
type AEADTargetProtector struct {
	provider         SecretProvider
	activeKeyVersion string
	random           io.Reader
}

// NewAEADTargetProtector creates a target protector using crypto/rand for every
// envelope nonce.
func NewAEADTargetProtector(provider SecretProvider, activeKeyVersion string) (*AEADTargetProtector, error) {
	return newAEADTargetProtector(provider, activeKeyVersion, rand.Reader)
}

func newAEADTargetProtector(
	provider SecretProvider,
	activeKeyVersion string,
	random io.Reader,
) (*AEADTargetProtector, error) {
	if provider == nil {
		return nil, errors.New("secret provider is required")
	}
	if activeKeyVersion == "" {
		return nil, errors.New("provider target key version is required")
	}
	if random == nil {
		return nil, errors.New("target random source is required")
	}
	return &AEADTargetProtector{
		provider:         provider,
		activeKeyVersion: activeKeyVersion,
		random:           random,
	}, nil
}

// Seal validates and encrypts one provider target under the target context.
func (p *AEADTargetProtector) Seal(
	ctx context.Context,
	targetContext channels.TargetContext,
	purpose channels.TargetPurpose,
	target channels.TargetPlaintext,
) (channels.TargetEnvelope, error) {
	ctx = normalizeContext(ctx)
	if p == nil || p.provider == nil {
		return channels.TargetEnvelope{}, errors.New("target protector is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return channels.TargetEnvelope{}, err
	}
	if err := targetContext.ValidateFor(purpose); err != nil {
		return channels.TargetEnvelope{}, err
	}
	if err := purpose.Validate(); err != nil {
		return channels.TargetEnvelope{}, err
	}
	if err := target.Validate(purpose); err != nil {
		return channels.TargetEnvelope{}, err
	}
	if target.Channel != targetContext.Channel {
		return channels.TargetEnvelope{}, errors.New("target channel does not match target context")
	}
	key, err := resolveSecretKey(ctx, p.provider, targetContext.Scope, providerTargetKeyName, p.activeKeyVersion)
	if err != nil {
		return channels.TargetEnvelope{}, fmt.Errorf("resolve provider target key: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return channels.TargetEnvelope{}, fmt.Errorf("create target cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return channels.TargetEnvelope{}, fmt.Errorf("create target aead: %w", err)
	}
	if aead.NonceSize() != channels.TargetNonceSize {
		return channels.TargetEnvelope{}, errors.New("target aead nonce size is unsupported")
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(p.random, nonce); err != nil {
		return channels.TargetEnvelope{}, fmt.Errorf("generate target nonce: %w", err)
	}
	plaintext, err := json.Marshal(target)
	if err != nil {
		return channels.TargetEnvelope{}, fmt.Errorf("marshal provider target: %w", err)
	}
	aad, err := targetAAD(targetContext, purpose, p.activeKeyVersion)
	if err != nil {
		return channels.TargetEnvelope{}, err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, aad)
	return channels.TargetEnvelope{
		Algorithm:     channels.TargetAlgorithmAES256GCM,
		KeyVersion:    p.activeKeyVersion,
		NonceB64:      base64.RawURLEncoding.EncodeToString(nonce),
		CiphertextB64: base64.RawURLEncoding.EncodeToString(ciphertext),
	}, nil
}

// Open validates and decrypts one provider target under the target context.
func (p *AEADTargetProtector) Open(
	ctx context.Context,
	targetContext channels.TargetContext,
	purpose channels.TargetPurpose,
	envelope channels.TargetEnvelope,
) (channels.TargetPlaintext, error) {
	ctx = normalizeContext(ctx)
	if p == nil || p.provider == nil {
		return channels.TargetPlaintext{}, errors.New("target protector is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := targetContext.ValidateFor(purpose); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := purpose.Validate(); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := envelope.Validate(); err != nil {
		return channels.TargetPlaintext{}, err
	}
	key, err := resolveSecretKey(ctx, p.provider, targetContext.Scope, providerTargetKeyName, envelope.KeyVersion)
	if err != nil {
		return channels.TargetPlaintext{}, fmt.Errorf("resolve provider target key: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return channels.TargetPlaintext{}, fmt.Errorf("create target cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return channels.TargetPlaintext{}, fmt.Errorf("create target aead: %w", err)
	}
	nonce, err := base64.RawURLEncoding.DecodeString(envelope.NonceB64)
	if err != nil || len(nonce) != channels.TargetNonceSize {
		return channels.TargetPlaintext{}, errors.New("target envelope nonce is invalid")
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(envelope.CiphertextB64)
	if err != nil || len(ciphertext) < aead.Overhead() {
		return channels.TargetPlaintext{}, errors.New("target envelope ciphertext is invalid")
	}
	aad, err := targetAAD(targetContext, purpose, envelope.KeyVersion)
	if err != nil {
		return channels.TargetPlaintext{}, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return channels.TargetPlaintext{}, errors.New("open provider target: authentication failed")
	}
	target, err := decodeTargetPlaintext(plaintext, purpose)
	if err != nil {
		return channels.TargetPlaintext{}, err
	}
	if target.Channel != targetContext.Channel {
		return channels.TargetPlaintext{}, errors.New("target channel does not match target context")
	}
	return target, nil
}

func resolveSecretKey(
	ctx context.Context,
	provider SecretProvider,
	scope tenant.Scope,
	name string,
	version string,
) ([]byte, error) {
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if provider == nil {
		return nil, errors.New("secret provider is required")
	}
	if name == "" || version == "" {
		return nil, errors.New("secret name and version are required")
	}
	value, err := provider.ResolveSecret(ctx, scope, tenant.SecretRef{Name: name, Version: version})
	if err != nil {
		return nil, err
	}
	key, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, errors.New("secret is not valid unpadded base64url")
	}
	if len(key) != secretKeyBytes {
		return nil, errors.New("secret must decode to 32 bytes")
	}
	return key, nil
}

func targetAAD(
	targetContext channels.TargetContext,
	purpose channels.TargetPurpose,
	keyVersion string,
) ([]byte, error) {
	if err := targetContext.ValidateFor(purpose); err != nil {
		return nil, err
	}
	if err := purpose.Validate(); err != nil {
		return nil, err
	}
	if keyVersion == "" {
		return nil, errors.New("target key version is required")
	}
	return lengthPrefixedStrings(
		targetContext.Scope.TenantID,
		targetContext.Scope.AppID,
		targetContext.BindingID,
		string(targetContext.Channel),
		string(targetContext.EntityType),
		targetContext.InternalEntityID,
		string(purpose),
		keyVersion,
	)
}

func lengthPrefixedStrings(values ...string) ([]byte, error) {
	encoded := make([]byte, 0)
	for _, value := range values {
		if uint64(len(value)) > math.MaxUint32 {
			return nil, errors.New("length-prefixed value is too long")
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, value...)
	}
	return encoded, nil
}

func decodeTargetPlaintext(data []byte, purpose channels.TargetPurpose) (channels.TargetPlaintext, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return channels.TargetPlaintext{}, errors.New("provider target is not valid json")
	}
	const fieldCount = 7
	if len(fields) != fieldCount {
		return channels.TargetPlaintext{}, errors.New("provider target fields are invalid")
	}
	for _, field := range []string{
		"version",
		"channel",
		"target_kind",
		"external_user_id",
		"external_chat_id",
		"external_thread_id",
		"provider_target",
	} {
		if _, ok := fields[field]; !ok {
			return channels.TargetPlaintext{}, errors.New("provider target field is missing")
		}
	}
	if err := rejectJSONNull(fields["version"]); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := rejectJSONNull(fields["channel"]); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := rejectJSONNull(fields["target_kind"]); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := rejectJSONNull(fields["external_user_id"]); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := rejectJSONNull(fields["external_chat_id"]); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := rejectJSONNull(fields["external_thread_id"]); err != nil {
		return channels.TargetPlaintext{}, err
	}
	if err := rejectJSONNull(fields["provider_target"]); err != nil {
		return channels.TargetPlaintext{}, err
	}
	var target channels.TargetPlaintext
	if err := json.Unmarshal(data, &target); err != nil {
		return channels.TargetPlaintext{}, errors.New("provider target fields have invalid types")
	}
	if err := target.Validate(purpose); err != nil {
		return channels.TargetPlaintext{}, err
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(canonical, data) {
		return channels.TargetPlaintext{}, errors.New("provider target is not canonical")
	}
	return target, nil
}

func rejectJSONNull(value json.RawMessage) error {
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return errors.New("provider target field cannot be null")
	}
	return nil
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
