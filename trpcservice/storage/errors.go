package storage

import (
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrInvalidArgument = errors.New("invalid storage argument")
	ErrNotFound        = errors.New("resource not found")
	ErrTenantMismatch  = tenant.ErrTenantMismatch
	ErrConflict        = errors.New("write conflict")
	ErrLeaseLost       = errors.New("session lease lost")
	ErrFenceRejected   = errors.New("stale fence token")
	ErrAlreadyClaimed  = errors.New("message already claimed")
)
