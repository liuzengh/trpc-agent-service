package domain

import "time"

// Session is the revocable server-side authentication state. The browser gets
// the plaintext token once; Identity persists only TokenHash.
type Session struct {
	ID         string
	UserID     string
	TokenHash  []byte
	Restricted bool
	CreatedAt  time.Time
	ExpiresAt  time.Time
	RevokedAt  *time.Time
}

// SessionIdentity is the single persistence view required to establish a
// trustworthy request identity.
type SessionIdentity struct {
	Session Session
	Account UserAccount
}
