package application

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
)

var (
	ErrInvalidUsername = errors.New("invalid username")
	ErrWeakPassword    = errors.New("password does not satisfy policy")
	ErrUsernameTaken   = errors.New("username is already in use")
	ErrAccountNotFound = errors.New("account not found")
)

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{2,31}$`)

// PasswordHasher is the complete credential capability needed by Identity
// account and password use cases.
type PasswordHasher interface {
	PasswordVerifier
	Hash(password string) (string, error)
}

// AccountStore owns atomic account/credential persistence and account queries.
type AccountStore interface {
	CreateAccount(context.Context, domain.UserAccount, domain.PasswordCredential) error
	GetAccount(context.Context, string) (domain.UserAccount, error)
	ListAccounts(context.Context, Page) (AccountPage, error)
}

// Page is the shared V1 offset pagination input inside Identity.
type Page struct {
	Offset int
	Limit  int
}

// AccountPage is a stable page of global platform accounts.
type AccountPage struct {
	Accounts []domain.UserAccount
	Total    int
}

// AccountManagementDependencies contains the Identity-owned dependencies for
// account creation and queries.
type AccountManagementDependencies struct {
	Store     AccountStore
	Passwords PasswordHasher
	NewID     func() (string, error)
	Now       func() time.Time
}

// AccountManagement is the deep module used by Admin through a narrow port.
type AccountManagement struct {
	deps AccountManagementDependencies
}

func NewAccountManagement(deps AccountManagementDependencies) *AccountManagement {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &AccountManagement{deps: deps}
}

type CreateManagedAccountCommand struct {
	Username          string
	DisplayName       string
	TemporaryPassword string
}

// CreateManagedAccount creates an ACTIVE global account whose first session is
// restricted until the temporary password is changed.
func (m *AccountManagement) CreateManagedAccount(
	ctx context.Context,
	command CreateManagedAccountCommand,
) (domain.UserAccount, error) {
	if m == nil || m.deps.Store == nil || m.deps.Passwords == nil || m.deps.NewID == nil {
		return domain.UserAccount{}, errors.New("account management: incomplete dependencies")
	}
	username := strings.TrimSpace(command.Username)
	if !usernamePattern.MatchString(username) {
		return domain.UserAccount{}, ErrInvalidUsername
	}
	if err := ValidatePassword(command.TemporaryPassword); err != nil {
		return domain.UserAccount{}, err
	}
	encodedHash, err := m.deps.Passwords.Hash(command.TemporaryPassword)
	if err != nil {
		return domain.UserAccount{}, fmt.Errorf("hash temporary password: %w", err)
	}
	id, err := m.deps.NewID()
	if err != nil {
		return domain.UserAccount{}, fmt.Errorf("generate account id: %w", err)
	}
	now := m.deps.Now().UTC()
	account := domain.UserAccount{
		ID:                 id,
		Username:           username,
		NormalizedUsername: domain.NormalizeUsername(username),
		DisplayName:        strings.TrimSpace(command.DisplayName),
		Status:             domain.AccountStatusActive,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if account.DisplayName == "" {
		account.DisplayName = username
	}
	credential := domain.PasswordCredential{
		EncodedHash:           encodedHash,
		MustChangeAtNextLogin: true,
	}
	if err := m.deps.Store.CreateAccount(ctx, account, credential); err != nil {
		if errors.Is(err, ErrUsernameTaken) {
			return domain.UserAccount{}, ErrUsernameTaken
		}
		return domain.UserAccount{}, fmt.Errorf("create account: %w", err)
	}
	return account, nil
}

func (m *AccountManagement) GetAccount(ctx context.Context, userID string) (domain.UserAccount, error) {
	return m.deps.Store.GetAccount(ctx, userID)
}

func (m *AccountManagement) ListAccounts(ctx context.Context, page Page) (AccountPage, error) {
	if page.Offset < 0 {
		page.Offset = 0
	}
	if page.Limit <= 0 {
		page.Limit = 20
	}
	if page.Limit > 100 {
		page.Limit = 100
	}
	return m.deps.Store.ListAccounts(ctx, page)
}

// ValidatePassword is the V1 local-password policy shared by managed account
// creation and password rotation.
func ValidatePassword(password string) error {
	length := utf8.RuneCountInString(password)
	if length < 12 || length > 128 {
		return ErrWeakPassword
	}
	return nil
}
