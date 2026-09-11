package domain

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrCredentialNotFound    = errors.New("profile credential not found")
	ErrCredentialConflict    = errors.New("profile credential revision conflict")
	ErrCredentialUnavailable = errors.New("profile credential unavailable")
	ErrCredentialInput       = errors.New("invalid profile credential input")
	ErrCredentialAssociation = errors.New("profile credential association mismatch")
)

const (
	CredentialActive        = "active"
	CredentialCleared       = "cleared"
	MaxCredentialValueBytes = 64 * 1024
)

// ProfileCredential is private to Profile storage. It is never a public DTO.
type ProfileCredential struct {
	ID             string
	TenantID       string
	ProfileID      string
	Category       string
	ResourceName   string
	Purpose        string
	AudienceDigest string
	Revision       int64
	Status         string
	Ciphertext     []byte
	CreatedBy      string
	UpdatedBy      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (c ProfileCredential) Clone() ProfileCredential {
	c.Ciphertext = append([]byte(nil), c.Ciphertext...)
	return c
}

// AssociatedData authenticates immutable ownership and intended destination.
func (c ProfileCredential) AssociatedData() []byte {
	data, _ := json.Marshal([]string{"profile-credential-v1", c.TenantID, c.ProfileID, c.ID, c.Category, c.ResourceName, c.Purpose, c.AudienceDigest})
	return data
}
