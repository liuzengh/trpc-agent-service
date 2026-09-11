package identity

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"unicode"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrLocalCredentialNotFound = errors.New("local credential not found")
	ErrLocalUsernameTaken      = errors.New("local username is already in use")
)

type LocalCredential struct {
	PlatformUserID     string
	Username           string
	PasswordHash       string
	MustChangePassword bool
}

func NormalizeLocalUsername(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) < 3 || len(value) > 64 {
		return "", errors.New("username must be between 3 and 64 characters")
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' {
			continue
		}
		return "", errors.New("username contains unsupported characters")
	}
	return value, nil
}

func HashLocalPassword(password string) (string, error) {
	if len(password) < 12 || len(password) > 1024 {
		return "", errors.New("password must be between 12 and 1024 characters")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", errors.New("hash local password")
	}
	return string(hash), nil
}

func VerifyLocalPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

func GenerateTemporaryPassword() (string, error) {
	buffer := make([]byte, 24)
	if _, err := rand.Read(buffer); err != nil {
		return "", errors.New("generate temporary password")
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

type LocalBootstrap struct {
	Username    string
	Password    string
	DisplayName string
	Email       string
}

// BootstrapLocalSystemAdmin creates the first administrator only while the
// instance has no usable System Admin. Once one exists, deployment bootstrap
// credentials are ignored and cannot grant or reset permissions later.
func BootstrapLocalSystemAdmin(ctx context.Context, store LocalBootstrapStore, config LocalBootstrap) error {
	if store == nil {
		return errors.New("identity store is required")
	}
	hasAdmin, err := store.HasUsableSystemAdmin(ctx)
	if err != nil {
		return err
	}
	if hasAdmin {
		return nil
	}
	if strings.TrimSpace(config.Username) == "" && strings.TrimSpace(config.Password) == "" {
		return nil
	}
	username, err := NormalizeLocalUsername(config.Username)
	if err != nil {
		return err
	}
	hash, err := HashLocalPassword(config.Password)
	if err != nil {
		return err
	}
	user, err := store.CreateLocalUser(ctx, username, config.DisplayName, config.Email, hash, true)
	if err != nil {
		return err
	}
	return store.SetSystemAdmin(ctx, user.PlatformUserID, true)
}
