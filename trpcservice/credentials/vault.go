// Package credentials holds encrypted, tenant-scoped connection credentials.
package credentials

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

var ErrUnavailable = errors.New("连接存储不可用，请联系管理员检查数据库和加密配置")
var Purposes = []string{secret.TelegramBot, secret.TelegramWebhook, secret.TelegramMedia, secret.WeComMCPRead, secret.WeComMCPSend, secret.Session, secret.Memory, secret.Knowledge, secret.Artifact, secret.Embedding}

type Vault struct {
	db    *sql.DB
	aead  cipher.AEAD
	keyID string
}
type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (v *Vault) sql(ctx context.Context) executor {
	if tx := database.Transaction(ctx, v.db); tx != nil {
		return tx
	}
	return v.db
}
func New(repository any, encoded string) (*Vault, error) {
	if encoded == "" {
		return nil, nil
	}
	provider, ok := repository.(interface{ SQLDB() *sql.DB })
	if !ok || provider.SQLDB() == nil {
		return nil, ErrUnavailable
	}
	key, e := base64.StdEncoding.DecodeString(encoded)
	if e != nil || len(key) != 32 {
		return nil, ErrUnavailable
	}
	block, e := aes.NewCipher(key)
	if e != nil {
		return nil, ErrUnavailable
	}
	aead, e := cipher.NewGCM(block)
	if e != nil {
		return nil, ErrUnavailable
	}
	digest := sha256.Sum256(key)
	v := &Vault{provider.SQLDB(), aead, hex.EncodeToString(digest[:])}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var mismatch bool
	if v.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_credential WHERE key_id<>$1)`, v.keyID).Scan(&mismatch) != nil || mismatch {
		return nil, ErrUnavailable
	}
	return v, nil
}
func (v *Vault) Put(ctx context.Context, tenant string, purposes []string, value string) (string, error) {
	if v == nil {
		return "", ErrUnavailable
	}
	if value == "" || len(value) > 16384 || strings.ContainsAny(value, "\r\n\x00") {
		return "", secret.ErrForbidden
	}
	purposes = slices.Clone(purposes)
	slices.Sort(purposes)
	if len(purposes) == 0 || len(purposes) > 3 || len(slices.Compact(slices.Clone(purposes))) != len(purposes) {
		return "", secret.ErrForbidden
	}
	for _, p := range purposes {
		if !slices.Contains(Purposes, p) {
			return "", secret.ErrForbidden
		}
	}
	ref := "managed://" + uuid.NewString()
	nonce := make([]byte, v.aead.NonceSize())
	if _, e := io.ReadFull(rand.Reader, nonce); e != nil {
		return "", ErrUnavailable
	}
	sealed := v.aead.Seal(nonce, nonce, []byte(value), []byte(tenant+"\x00"+ref+"\x00"+strings.Join(purposes, ",")))
	if _, e := v.sql(ctx).ExecContext(ctx, `INSERT INTO channel_credential(tenant_id,reference,purposes,ciphertext,key_id) VALUES($1,$2,$3,$4,$5)`, tenant, ref, purposes, sealed, v.keyID); e != nil {
		return "", ErrUnavailable
	}
	return ref, nil
}
func (v *Vault) Authorize(ctx context.Context, tenant, purpose, ref string) error {
	if v == nil || !slices.Contains(Purposes, purpose) || !strings.HasPrefix(ref, "managed://") {
		return secret.ErrForbidden
	}
	var ok bool
	if v.sql(ctx).QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_credential WHERE tenant_id=$1 AND reference=$2 AND $3=ANY(purposes))`, tenant, ref, purpose).Scan(&ok) != nil {
		return ErrUnavailable
	}
	if !ok {
		return secret.ErrForbidden
	}
	return nil
}
func (v *Vault) Resolve(ctx context.Context, tenant, purpose, ref string) (string, error) {
	if e := v.Authorize(ctx, tenant, purpose, ref); e != nil {
		return "", e
	}
	var sealed, purposeJSON []byte
	var keyID string
	var purposes []string
	if v.sql(ctx).QueryRowContext(ctx, `SELECT ciphertext,key_id,to_json(purposes) FROM channel_credential WHERE tenant_id=$1 AND reference=$2`, tenant, ref).Scan(&sealed, &keyID, &purposeJSON) != nil || json.Unmarshal(purposeJSON, &purposes) != nil || keyID != v.keyID || len(sealed) < v.aead.NonceSize()+v.aead.Overhead() {
		return "", ErrUnavailable
	}
	value, e := v.aead.Open(nil, sealed[:v.aead.NonceSize()], sealed[v.aead.NonceSize():], []byte(tenant+"\x00"+ref+"\x00"+strings.Join(purposes, ",")))
	if e != nil {
		return "", ErrUnavailable
	}
	return string(value), nil
}

type Fallback interface {
	secret.Store
	secret.Authorizer
	secret.ReferenceCatalog
}
type Routed struct {
	Vault    *Vault
	Fallback Fallback
	Allowed  []string
}

func (r Routed) Authorize(ctx context.Context, t, p, ref string) error {
	if !strings.HasPrefix(ref, "managed://") {
		return r.Fallback.Authorize(ctx, t, p, ref)
	}
	if !slices.Contains(r.Allowed, p) {
		return secret.ErrForbidden
	}
	return r.Vault.Authorize(ctx, t, p, ref)
}
func (r Routed) Resolve(ctx context.Context, t, p, ref string) (string, error) {
	if !strings.HasPrefix(ref, "managed://") {
		return r.Fallback.Resolve(ctx, t, p, ref)
	}
	if e := r.Authorize(ctx, t, p, ref); e != nil {
		return "", e
	}
	return r.Vault.Resolve(ctx, t, p, ref)
}
func (r Routed) References(ctx context.Context, t, p string) ([]string, error) {
	// Web-managed references are implementation details, not a second UI picker.
	return r.Fallback.References(ctx, t, p)
}
