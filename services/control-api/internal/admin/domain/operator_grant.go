// Package domain contains platform administration rules and state.
package domain

import "time"

type GrantedByActorType string

const (
	GrantedByUser            GrantedByActorType = "USER"
	GrantedBySystemBootstrap GrantedByActorType = "SYSTEM_BOOTSTRAP"
)

// OperatorGrant is the platform-level authorization attached to a global
// UserAccount. It is deliberately separate from Tenant membership.
type OperatorGrant struct {
	UserID             string
	GrantedByActorType GrantedByActorType
	GrantedByUserID    string
	GrantedAt          time.Time
	RevokedByUserID    string
	RevokedAt          *time.Time
}

func (grant OperatorGrant) Active() bool {
	return grant.RevokedAt == nil
}
