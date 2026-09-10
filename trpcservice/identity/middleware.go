package identity

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

const (
	sessionCookieName   = "dsh_session"
	csrfCookieName      = "csrf_token"
	csrfHeaderName      = "X-CSRF-Token"
	actionSessionExpire = "user_session_expire"
)

type contextUserKey struct{}

// UserFromContext returns the authenticated user injected by SessionMiddleware.
func UserFromContext(ctx context.Context) (SessionUser, bool) {
	user, ok := ctx.Value(contextUserKey{}).(SessionUser)
	return user, ok && user.PlatformUserID != ""
}

// SessionMiddleware authenticates browser requests using only the HttpOnly
// session cookie. API/service tokens are a separate credential domain and must
// never reuse browser session IDs.
func SessionMiddleware(store SessionStore, resolver SessionPrincipalResolver, audit AuditRecorder, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		token := ""
		if cookie, err := request.Cookie(sessionCookieName); err == nil && cookie.Value != "" {
			token = cookie.Value
		}

		if token == "" {
			http.Error(writer, "unauthorized: missing login session", http.StatusUnauthorized)
			return
		}
		user, err := store.Get(request.Context(), token)
		if err != nil {
			// Redis cannot distinguish an expired key from a never-existing key,
			// so both states intentionally share one audit outcome.
			if audit != nil {
				_ = audit.RecordAudit(request.Context(), actionSessionExpire, "session_invalid", token)
			}
			http.Error(writer, "unauthorized: invalid or expired login session", http.StatusUnauthorized)
			return
		}
		if resolver != nil {
			user, err = resolver.ResolveSessionUser(request.Context(), user.PlatformUserID)
			if err != nil {
				if errors.Is(err, ErrPlatformUserNotFound) || errors.Is(err, ErrPlatformUserSuspended) {
					_ = store.Delete(request.Context(), token)
					if audit != nil {
						_ = audit.RecordAudit(request.Context(), actionSessionExpire, "principal_invalid", user.PlatformUserID)
					}
					http.Error(writer, "unauthorized: login principal is no longer available", http.StatusUnauthorized)
					return
				}
				http.Error(writer, "internal server error: resolve login principal", http.StatusInternalServerError)
				return
			}
		}
		ctx := context.WithValue(request.Context(), contextUserKey{}, user)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

// PasswordChangeMiddleware prevents a temporary Local credential from being
// used for normal product APIs before the user replaces it.
func PasswordChangeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if user, ok := UserFromContext(request.Context()); ok && user.MustChangePassword {
			http.Error(writer, "forbidden: password change required", http.StatusForbidden)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

// CSRFMiddleware applies double-submit validation to every browser write.
func CSRFMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			cookie, cookieErr := request.Cookie(csrfCookieName)
			header := request.Header.Get(csrfHeaderName)
			if cookieErr != nil || header == "" || cookie.Value == "" || header != cookie.Value {
				http.Error(writer, "forbidden: CSRF token mismatch", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(writer, request)
	})
}

// SessionCookieAttributes returns the common login-cookie attributes.
func SessionCookieAttributes(secure bool) string {
	attributes := []string{"Path=/", "HttpOnly", "SameSite=Lax"}
	if secure {
		attributes = append(attributes, "Secure")
	}
	return strings.Join(attributes, "; ")
}

// ClearSessionCookie returns a Set-Cookie value that expires the login session.
func ClearSessionCookie(secure bool) string {
	return sessionCookieName + "=; Max-Age=0; " + SessionCookieAttributes(secure)
}

// SetSessionCookie writes the login and double-submit CSRF cookies.
func SetSessionCookie(writer http.ResponseWriter, sessionID, csrfToken string, secure bool) {
	http.SetCookie(writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
	})
	http.SetCookie(writer, &http.Cookie{
		Name:     csrfCookieName,
		Value:    csrfToken,
		Path:     "/",
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
	})
}
