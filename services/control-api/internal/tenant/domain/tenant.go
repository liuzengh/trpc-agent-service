package domain

import "time"

type TenantStatus string

const TenantStatusActive TenantStatus = "ACTIVE"

type Tenant struct {
	ID        string
	Slug      string
	Name      string
	Status    TenantStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}
