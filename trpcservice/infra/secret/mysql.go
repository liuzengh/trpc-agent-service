package secret

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// MySQLStore persists secrets encrypted with AES-256-GCM. The master key is
// used to derive the AES key via sha256 and is never stored; a missing master
// key is a hard error so plaintext never lands in the database.
type MySQLStore struct {
	db     *sql.DB
	aead   cipher.AEAD
}

// NewMySQLStore returns a MySQL-backed secret store, deriving the AES key from
// masterKey. It fails when masterKey is empty (refusing to store plaintext).
func NewMySQLStore(db *sql.DB, masterKey string) (*MySQLStore, error) {
	if db == nil {
		return nil, errors.New("secret: db is required")
	}
	if masterKey == "" {
		return nil, errors.New("secret: master key is required for the MySQL store (refusing plaintext at rest)")
	}
	block, err := aes.NewCipher(deriveKey(masterKey))
	if err != nil {
		return nil, fmt.Errorf("secret: new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secret: new gcm: %w", err)
	}
	return &MySQLStore{db: db, aead: aead}, nil
}

// deriveKey normalizes an arbitrary master key to a 32-byte AES key.
func deriveKey(masterKey string) []byte {
	sum := sha256.Sum256([]byte(masterKey))
	return sum[:]
}

// Put encrypts and upserts a secret.
func (s *MySQLStore) Put(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("secret: key is required")
	}
	ct, err := s.seal(value)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO secrets (secret_key, ciphertext) VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE ciphertext = VALUES(ciphertext), updated_at = CURRENT_TIMESTAMP`,
		key, ct)
	if err != nil {
		return fmt.Errorf("secret: put: %w", err)
	}
	return nil
}

// Get decrypts and returns a secret.
func (s *MySQLStore) Get(ctx context.Context, key string) (string, error) {
	var ct []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT ciphertext FROM secrets WHERE secret_key = ?`, key).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("secret: get: %w", err)
	}
	plain, err := s.open(ct)
	if err != nil {
		return "", fmt.Errorf("secret: decrypt %q: %w", key, err)
	}
	return plain, nil
}

// List returns metadata only (key + updated_at), never values.
func (s *MySQLStore) List(ctx context.Context) ([]Secret, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT secret_key, updated_at FROM secrets ORDER BY secret_key`)
	if err != nil {
		return nil, fmt.Errorf("secret: list: %w", err)
	}
	defer rows.Close()
	out := make([]Secret, 0)
	for rows.Next() {
		var e Secret
		if err := rows.Scan(&e.Key, &e.UpdatedAt); err != nil {
			return nil, fmt.Errorf("secret: scan: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Delete removes a secret; deleting a missing key is a no-op.
func (s *MySQLStore) Delete(ctx context.Context, key string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM secrets WHERE secret_key = ?`, key); err != nil {
		return fmt.Errorf("secret: delete: %w", err)
	}
	return nil
}

// seal encrypts value into base64(nonce || ciphertext).
func (s *MySQLStore) seal(value string) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("secret: nonce: %w", err)
	}
	ct := s.aead.Seal(nonce, nonce, []byte(value), nil)
	out := make([]byte, base64.StdEncoding.EncodedLen(len(ct)))
	base64.StdEncoding.Encode(out, ct)
	return out, nil
}

// open decrypts a base64(nonce || ciphertext) value.
func (s *MySQLStore) open(encoded []byte) (string, error) {
	raw := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	n, err := base64.StdEncoding.Decode(raw, encoded)
	if err != nil {
		return "", err
	}
	raw = raw[:n]
	if len(raw) < s.aead.NonceSize() {
		return "", errors.New("ciphertext too short")
	}
	nonce, ct := raw[:s.aead.NonceSize()], raw[s.aead.NonceSize():]
	plain, err := s.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
