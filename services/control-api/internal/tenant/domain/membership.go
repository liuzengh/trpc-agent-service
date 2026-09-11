package domain

import "time"

type MembershipRole string

const (
	MembershipRoleOwner  MembershipRole = "OWNER"
	MembershipRoleMember MembershipRole = "MEMBER"
)

type Membership struct {
	ID        string
	TenantID  string
	UserID    string
	Role      MembershipRole
	CreatedBy string
	CreatedAt time.Time
}

func (m Membership) CanManageMembers() bool {
	return m.Role == MembershipRoleOwner
}

type TenantMembership struct {
	Tenant     Tenant
	Membership Membership
}
