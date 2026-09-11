package identity

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const loginTransactionCookieName = "dsh_login_tx"

const LoginFlowQRLegacy = "qr_legacy"

type LoginTransaction struct {
	ProviderID         string `json:"provider_id"`
	State              string `json:"state"`
	Nonce              string `json:"nonce"`
	PKCEVerifier       string `json:"pkce_verifier"`
	LinkPlatformUserID string `json:"link_platform_user_id,omitempty"`
	ReturnPath         string `json:"return_path,omitempty"`
	Flow               string `json:"flow,omitempty"`
}

func NewLoginLinkTransaction(providerID, platformUserID string) (LoginTransaction, string, error) {
	platformUserID = strings.TrimSpace(platformUserID)
	if platformUserID == "" {
		return LoginTransaction{}, "", errors.New("platform user ID is required")
	}
	transaction, challenge, err := NewLoginTransaction(providerID)
	if err != nil {
		return LoginTransaction{}, "", err
	}
	transaction.LinkPlatformUserID = platformUserID
	return transaction, challenge, nil
}

// SignState returns "<raw>.<hmac(secret, raw)>" so the callback can verify the
// same process (or any node sharing StateSecret) without server-side storage.
func SignState(secret, raw string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(raw))
	return raw + "." + hex.EncodeToString(mac.Sum(nil))
}

// VerifyState validates a signed state, returning the raw value when valid.
func VerifyState(secret, state string) (string, bool) {
	index := -1
	for i := 0; i < len(state); i++ {
		if state[i] == '.' {
			index = i
			break
		}
	}
	if index <= 0 || index == len(state)-1 {
		return "", false
	}
	raw := state[:index]
	signature := state[index+1:]
	expected := hmac.New(sha256.New, []byte(secret))
	_, _ = expected.Write([]byte(raw))
	if !hmac.Equal([]byte(signature), []byte(hex.EncodeToString(expected.Sum(nil)))) {
		return "", false
	}
	return raw, true
}

// RandomState returns a fresh random state value (login entry generation).
func RandomState() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func NewLoginTransaction(providerID string) (LoginTransaction, string, error) {
	providerID = strings.TrimSpace(providerID)
	if providerID == "" {
		return LoginTransaction{}, "", errors.New("login provider ID is required")
	}
	state, err := RandomState()
	if err != nil {
		return LoginTransaction{}, "", err
	}
	nonce, err := randomBase64URL(24)
	if err != nil {
		return LoginTransaction{}, "", err
	}
	verifier, err := randomBase64URL(48)
	if err != nil {
		return LoginTransaction{}, "", err
	}
	hash := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(hash[:])
	return LoginTransaction{ProviderID: providerID, State: state, Nonce: nonce, PKCEVerifier: verifier}, challenge, nil
}

func randomBase64URL(size int) (string, error) {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func SetLoginTransactionCookie(writer http.ResponseWriter, transaction LoginTransaction, secret string, secure bool) error {
	payload, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	value := SignState(secret, encoded)
	http.SetCookie(writer, &http.Cookie{
		Name: loginTransactionCookieName, Value: value, Path: "/api/v1/auth/callback", HttpOnly: true,
		Secure: secure, SameSite: http.SameSiteLaxMode, MaxAge: int((10 * time.Minute).Seconds()),
	})
	return nil
}

func ReadLoginTransactionCookie(request *http.Request, secret string) (LoginTransaction, error) {
	cookie, err := request.Cookie(loginTransactionCookieName)
	if err != nil {
		return LoginTransaction{}, errors.New("login transaction cookie is missing")
	}
	encoded, ok := VerifyState(secret, cookie.Value)
	if !ok {
		return LoginTransaction{}, errors.New("login transaction cookie is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return LoginTransaction{}, errors.New("login transaction cookie is malformed")
	}
	var transaction LoginTransaction
	if err := json.Unmarshal(payload, &transaction); err != nil {
		return LoginTransaction{}, errors.New("login transaction cookie is malformed")
	}
	if transaction.ProviderID == "" || transaction.State == "" || transaction.Nonce == "" || transaction.PKCEVerifier == "" {
		return LoginTransaction{}, errors.New("login transaction cookie is incomplete")
	}
	return transaction, nil
}

func ClearLoginTransactionCookie(writer http.ResponseWriter, secure bool) {
	http.SetCookie(writer, &http.Cookie{Name: loginTransactionCookieName, Value: "", Path: "/api/v1/auth/callback", HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode, MaxAge: -1})
}
