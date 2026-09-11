// Package modelregistry stores tenant-owned versioned model connections.
// API keys are never part of revisions or the public metadata type.
package modelregistry

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

var ErrUnavailable = errors.New("model connection store unavailable; check schema, role permissions and encryption key")
var ErrEndpoint = errors.New("模型 API 地址不可用")
var ErrInvalid = errors.New("模型连接需要有效的名称、模型 ID、API 地址和 API Key")

type Connection struct {
	TenantID          string    `json:"tenant_id"`
	ID                string    `json:"connection_id"`
	Name              string    `json:"name"`
	Model             string    `json:"model_name"`
	BaseURL           string    `json:"base_url"`
	CreatedBy         string    `json:"created_by"`
	CreatedAt         time.Time `json:"created_at"`
	RootID            string    `json:"root_connection_id"`
	ConfigVersion     int64     `json:"config_version"`
	CredentialVersion int64     `json:"credential_version"`
	Version           int64     `json:"version"`
	SupersededBy      string    `json:"superseded_by,omitempty"`
	UpdatedBy         string    `json:"updated_by"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type Store struct {
	db              *sql.DB
	aead            cipher.AEAD
	keyID           string
	origins         map[string]bool
	transport       http.RoundTripper
	publicTransport http.RoundTripper
	endpointPolicy  string
}

// New is opt-in. Existing installations without a master key are unchanged.
// The key must be shared by Admin, Worker and Jobs, never stored in the DB.
func New(ctx context.Context, repository any, encodedKey, allowedOrigins string, options ...Option) (*Store, error) {
	if encodedKey == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil || len(key) != 32 {
		return nil, ErrUnavailable
	}
	provider, ok := repository.(interface{ SQLDB() *sql.DB })
	if !ok || provider.SQLDB() == nil {
		return nil, ErrUnavailable
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrUnavailable
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrUnavailable
	}
	digest := sha256.Sum256(key)
	s := &Store{db: provider.SQLDB(), aead: aead, keyID: hex.EncodeToString(digest[:]), origins: map[string]bool{}, endpointPolicy: PolicyPublicHTTPS}
	s.transport = http.DefaultTransport
	s.publicTransport = newPublicTransport()
	for _, option := range options {
		if option != nil {
			if err := option(s); err != nil {
				return nil, err
			}
		}
	}
	for _, raw := range strings.Split(allowedOrigins, ",") {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		u, err := endpoint(strings.TrimSpace(raw))
		if err != nil || (u.Path != "" && u.Path != "/") {
			return nil, ErrEndpoint
		}
		s.origins[origin(u)] = true
	}
	var mismatched bool
	var version int64
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM model_connection WHERE key_id<>$1),COALESCE(max(version),1) FROM model_connection`, s.keyID).Scan(&mismatched, &version); err != nil || mismatched {
		return nil, ErrUnavailable
	}
	return s, nil
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) executor(ctx context.Context) executor {
	if tx := database.Transaction(ctx, s.db); tx != nil {
		return tx
	}
	return s.db
}

const metadata = `tenant_id,connection_id,display_name,model_name,base_url,created_by,created_at,root_connection_id,config_version,credential_version,version,COALESCE(superseded_by,''),updated_by,updated_at`

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return controlplane.ErrNotFound
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" {
		return controlplane.ErrConflict
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrUnavailable // Never log raw SQL/provider errors alongside credentials.
}

type scanner interface{ Scan(...any) error }

func scan(row scanner) (Connection, error) {
	var c Connection
	err := row.Scan(&c.TenantID, &c.ID, &c.Name, &c.Model, &c.BaseURL, &c.CreatedBy, &c.CreatedAt, &c.RootID, &c.ConfigVersion, &c.CredentialVersion, &c.Version, &c.SupersededBy, &c.UpdatedBy, &c.UpdatedAt)
	return c, mapError(err)
}
func (s *Store) Get(ctx context.Context, tenant, id string) (Connection, error) {
	if s == nil {
		return Connection{}, ErrUnavailable
	}
	return scan(s.executor(ctx).QueryRowContext(ctx, `SELECT `+metadata+` FROM model_connection WHERE tenant_id=$1 AND connection_id=$2`, tenant, id))
}

// ValidateReference checks tenant scope and the current endpoint policy without
// decrypting a credential or contacting a model during publication preflight.
func (s *Store) ValidateReference(ctx context.Context, tenant, id string) error {
	c, err := s.Get(ctx, tenant, id)
	if err != nil {
		return err
	}
	return s.validateEndpoint(c.BaseURL)
}
func (s *Store) List(ctx context.Context, tenant, after string) ([]Connection, string, error) {
	result := []Connection{}
	if s == nil {
		return result, "", nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+metadata+` FROM model_connection WHERE tenant_id=$1 AND connection_id>$2 ORDER BY connection_id LIMIT 101`, tenant, after)
	if err != nil {
		return nil, "", mapError(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		c, err := scan(rows)
		if err != nil {
			return nil, "", err
		}
		if len(result) == 100 {
			return result, result[99].ID, nil
		}
		result = append(result, c)
	}
	return result, "", mapError(rows.Err())
}
func aad(c Connection) []byte {
	// Bind the ciphertext to its tenant, immutable ID, model and destination.
	return []byte("model-connection-v1\x00" + c.TenantID + "\x00" + c.ID + "\x00" + c.Model + "\x00" + c.BaseURL)
}
func (s *Store) Create(ctx context.Context, c Connection, apiKey string) (Connection, error) {
	c.RootID, c.ConfigVersion = c.ID, 1
	return s.insert(ctx, c, apiKey)
}

func (s *Store) insert(ctx context.Context, c Connection, apiKey string) (Connection, error) {
	if s == nil {
		return Connection{}, ErrUnavailable
	}
	if err := s.validateEndpoint(c.BaseURL); err != nil {
		return Connection{}, err
	}
	for _, v := range []string{c.TenantID, c.ID, c.Name, c.Model, c.CreatedBy} {
		if strings.TrimSpace(v) == "" || len(v) > 256 || strings.ContainsAny(v, "\x00\r\n") {
			return Connection{}, ErrInvalid
		}
	}
	encrypted, err := s.encrypt(c, apiKey)
	if err != nil {
		return Connection{}, err
	}
	_, err = s.executor(ctx).ExecContext(ctx, `INSERT INTO model_connection(tenant_id,connection_id,display_name,model_name,base_url,encrypted_key,key_id,created_by,root_connection_id,config_version,updated_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$8)`, c.TenantID, c.ID, c.Name, c.Model, c.BaseURL, encrypted, s.keyID, c.CreatedBy, c.RootID, c.ConfigVersion)
	if err != nil {
		return Connection{}, mapError(err)
	}
	return s.Get(ctx, c.TenantID, c.ID)
}

func (s *Store) encrypt(c Connection, apiKey string) ([]byte, error) {
	if strings.TrimSpace(apiKey) == "" || len(apiKey) > 16384 || strings.ContainsAny(apiKey, "\r\n\x00") {
		return nil, ErrInvalid
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, ErrUnavailable
	}
	return s.aead.Seal(nonce, nonce, []byte(apiKey), aad(c)), nil
}

func (s *Store) Resolve(ctx context.Context, tenant, id string) (config.ModelConfig, error) {
	c, err := s.Get(ctx, tenant, id)
	if err != nil {
		return config.ModelConfig{}, err
	}
	if err := s.validateEndpoint(c.BaseURL); err != nil {
		return config.ModelConfig{}, err
	}
	u, _ := endpoint(c.BaseURL)
	transport := s.publicTransport
	if s.origins[origin(u)] {
		transport = s.transport
	}
	client := &http.Client{Transport: boundTransport{base: transport, expectedOrigin: origin(u), store: s, connection: c}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	// The SDK only retains a placeholder. The real key is resolved and injected
	// immediately before each HTTP request, including cached compiled Agents.
	return config.ModelConfig{Provider: "openai", Name: c.Model, BaseURL: c.BaseURL, APIKey: "managed-credential", HTTPClient: client}, nil
}

func (s *Store) credential(ctx context.Context, c Connection) (string, error) {
	var encrypted []byte
	var keyID, modelName, baseURL string
	err := s.executor(ctx).QueryRowContext(ctx, `SELECT encrypted_key,key_id,model_name,base_url FROM model_connection WHERE tenant_id=$1 AND connection_id=$2`, c.TenantID, c.ID).Scan(&encrypted, &keyID, &modelName, &baseURL)
	if err != nil {
		return "", mapError(err)
	}
	if keyID != s.keyID || modelName != c.Model || baseURL != c.BaseURL || len(encrypted) < s.aead.NonceSize()+s.aead.Overhead() {
		return "", ErrUnavailable
	}
	key, err := s.aead.Open(nil, encrypted[:s.aead.NonceSize()], encrypted[s.aead.NonceSize():], aad(c))
	if err != nil {
		return "", ErrUnavailable
	}
	return string(key), nil
}

type boundTransport struct {
	base           http.RoundTripper
	expectedOrigin string
	store          *Store
	connection     Connection
}

func (t boundTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL == nil || origin(r.URL) != t.expectedOrigin {
		return nil, ErrEndpoint
	}
	if err := t.store.validateEndpoint(r.URL.String()); err != nil {
		return nil, err
	}
	key, err := t.store.credential(r.Context(), t.connection)
	if err != nil {
		return nil, err
	} // Never fall back to an old cached credential.
	request := r.Clone(r.Context())
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := t.base.RoundTrip(request)
	if response != nil {
		// Do not expose the injected header through SDK response diagnostics.
		response.Request = r
	}
	return response, err
}
