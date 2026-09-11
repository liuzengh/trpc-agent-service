package identity

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoginTransactionRoundTripThroughSignedCookie(t *testing.T) {
	t.Parallel()
	transaction, challenge, err := NewLoginTransaction("feishu-main")
	if err != nil {
		t.Fatal(err)
	}
	if transaction.ProviderID != "feishu-main" || transaction.State == "" || transaction.Nonce == "" || transaction.PKCEVerifier == "" {
		t.Fatalf("transaction = %+v", transaction)
	}
	wantChallengeHash := sha256.Sum256([]byte(transaction.PKCEVerifier))
	if want := base64.RawURLEncoding.EncodeToString(wantChallengeHash[:]); challenge != want {
		t.Fatalf("PKCE challenge = %q, want %q", challenge, want)
	}

	const secret = "test-login-state-secret"
	recorder := httptest.NewRecorder()
	if err := SetLoginTransactionCookie(recorder, transaction, secret, true); err != nil {
		t.Fatal(err)
	}
	response := recorder.Result()
	defer response.Body.Close()
	cookies := response.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("login cookies = %d, want 1", len(cookies))
	}
	cookie := cookies[0]
	if cookie.Name != loginTransactionCookieName || !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteLaxMode || cookie.MaxAge <= 0 {
		t.Fatalf("login transaction cookie = %+v", cookie)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback?code=code-1", nil)
	request.AddCookie(cookie)
	got, err := ReadLoginTransactionCookie(request, secret)
	if err != nil {
		t.Fatal(err)
	}
	if got != transaction {
		t.Fatalf("cookie transaction = %+v, want %+v", got, transaction)
	}

	clearRecorder := httptest.NewRecorder()
	ClearLoginTransactionCookie(clearRecorder, true)
	clearCookies := clearRecorder.Result().Cookies()
	if len(clearCookies) != 1 || clearCookies[0].Name != loginTransactionCookieName || clearCookies[0].MaxAge != -1 || !clearCookies[0].Secure {
		t.Fatalf("clear cookie = %+v", clearCookies)
	}
}

func TestLoginLinkTransactionPreservesPlatformUserAndSignedState(t *testing.T) {
	t.Parallel()
	transaction, _, err := NewLoginLinkTransaction("oidc-main", " platform-user-1 ")
	if err != nil {
		t.Fatal(err)
	}
	if transaction.LinkPlatformUserID != "platform-user-1" {
		t.Fatalf("link platform user = %q", transaction.LinkPlatformUserID)
	}
	signed := SignState("state-secret", transaction.State)
	if raw, ok := VerifyState("state-secret", signed); !ok || raw != transaction.State {
		t.Fatalf("VerifyState() = %q, %v", raw, ok)
	}
}
