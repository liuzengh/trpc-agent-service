package identity

import (
	"errors"
	stdhttp "net/http"
	"time"

	"github.com/gin-gonic/gin"

	identityhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/inbound/http"
	passwordargon2id "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/outbound/argon2id"
	identitypostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

// Dependencies are the process-owned capabilities required by Identity.
type Dependencies struct {
	DB              identitypostgres.DB
	Routes          gin.IRoutes
	SessionLifetime time.Duration
	CookieName      string
	CookieDomain    string
	CookieSecure    bool
	CookieSameSite  stdhttp.SameSite
}

// Module exposes Identity application capabilities to process composition.
type Module struct {
	LoginWithPassword *application.LoginWithPassword
	Accounts          *application.AccountManagement
	Authenticate      *application.AuthenticateSession
	sessionHTTP       *identityhttp.SessionHandler
}

// AuthenticationMiddleware establishes IdentityContext for protected Admin
// and Tenant route groups.
func (m *Module) AuthenticationMiddleware() gin.HandlerFunc {
	return m.sessionHTTP.Middleware()
}

// NewModule assembles the Identity module and registers its inbound routes.
func NewModule(deps Dependencies) (*Module, error) {
	if deps.DB == nil || deps.Routes == nil {
		return nil, errors.New("identity: database and routes are required")
	}
	if deps.SessionLifetime <= 0 {
		return nil, errors.New("identity: session lifetime must be positive")
	}
	if deps.CookieName == "" {
		return nil, errors.New("identity: session cookie name is required")
	}

	hasher := passwordargon2id.New(passwordargon2id.DefaultParameters())
	dummyHash, err := hasher.Hash("identity-dummy-password")
	if err != nil {
		return nil, err
	}
	store := identitypostgres.NewStore(deps.DB)
	login := application.NewLoginWithPassword(application.LoginDependencies{
		Accounts:         store,
		Passwords:        hasher,
		Sessions:         store,
		NewSessionToken:  GenerateSessionToken,
		Now:              time.Now,
		Lifetime:         deps.SessionLifetime,
		DummyEncodedHash: dummyHash,
	})
	accounts := newAccountManagement(store, hasher)
	authenticate := application.NewAuthenticateSession(store, time.Now)
	logout := application.NewLogoutSession(store, time.Now)
	changePassword := application.NewChangePassword(store, hasher, time.Now)
	cookie := identityhttp.SessionCookie{
		Name:     deps.CookieName,
		Path:     "/",
		Domain:   deps.CookieDomain,
		Secure:   deps.CookieSecure,
		SameSite: deps.CookieSameSite,
	}
	loginHTTP := identityhttp.NewLoginHandler(login, cookie)
	loginHTTP.Register(deps.Routes)
	sessionHTTP := identityhttp.NewSessionHandler(authenticate, logout, changePassword, cookie)
	sessionHTTP.Register(deps.Routes)

	return &Module{
		LoginWithPassword: login,
		Accounts:          accounts,
		Authenticate:      authenticate,
		sessionHTTP:       sessionHTTP,
	}, nil
}

// NewAccountManagement assembles Identity account creation against the given
// persistence boundary. Process bootstrap uses it with a transaction-bound
// PostgreSQL adapter so initial account and operator creation commit together.
func NewAccountManagement(db identitypostgres.DB) *application.AccountManagement {
	hasher := passwordargon2id.New(passwordargon2id.DefaultParameters())
	return newAccountManagement(identitypostgres.NewStore(db), hasher)
}

func newAccountManagement(
	store *identitypostgres.Store,
	hasher application.PasswordHasher,
) *application.AccountManagement {
	return application.NewAccountManagement(application.AccountManagementDependencies{
		Store: store, Passwords: hasher,
		NewID: func() (string, error) { return GenerateID("usr") },
		Now:   time.Now,
	})
}
