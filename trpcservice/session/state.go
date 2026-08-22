package session

import (
	"fmt"
	"time"
)

type Summary struct {
	TenantID      string    `json:"tenant_id"`
	SessionID     string    `json:"session_id"`
	Version       int64     `json:"version"`
	CoveredSeq    int64     `json:"covered_seq"`
	Content       string    `json:"content"`
	TokenEstimate int64     `json:"token_estimate"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (s Summary) Validate() error {
	if err := validateID(s.TenantID); err != nil {
		return fmt.Errorf("%w: tenant_id: %v", ErrInvalidArgument, err)
	}
	if err := validateID(s.SessionID); err != nil {
		return fmt.Errorf("%w: session_id: %v", ErrInvalidArgument, err)
	}
	if s.Version < 1 || s.CoveredSeq < 0 || s.TokenEstimate < 0 {
		return fmt.Errorf("%w: invalid summary version, covered sequence, or token estimate", ErrInvalidArgument)
	}
	if s.UpdatedAt.Before(s.CreatedAt) && !s.CreatedAt.IsZero() && !s.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: updated_at precedes created_at", ErrInvalidArgument)
	}
	return nil
}
