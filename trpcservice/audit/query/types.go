// Package query exposes a read-only, tenant-scoped audit query over the
// independent compliance database, with a durable secondary-access record for
// every query (allowed or denied).
package query

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

var (
	ErrUnauthenticated = errors.New("audit query principal is not authenticated")
	ErrForbidden       = errors.New("audit query principal is not authorized")
	ErrInvalidFilter   = errors.New("audit query filter is invalid")
)

// Principal is the authenticated identity for an audit query.
type Principal struct {
	Authenticated           bool
	SubjectID               string
	TenantID                string
	CanReadAudit            bool
	CanReadAuditCrossTenant bool
}

// Filter is a bounded, tenant-scoped audit query.
type Filter struct {
	TenantID    string
	CrossTenant bool
	From        time.Time
	To          time.Time
	PageSize    int
	Cursor      string
}

// Page is a keyset-paginated result.
type Page struct {
	Events     []audit.Event
	NextCursor string
}

// QueryRecord is the immutable secondary-audit fact written for every query.
type QueryRecord struct {
	QueryID      string
	TenantID     string
	Subject      string
	CrossTenant  bool
	From         time.Time
	To           time.Time
	FilterDigest string
	ResultCount  int64
	ResultDigest string
	Decision     string // allowed | denied
	ReasonCode   string
	TraceID      string
	OccurredAt   time.Time
}

// Store is the read-only audit query surface.
type Store interface {
	Query(context.Context, Filter) (Page, error)
	RecordAccess(context.Context, QueryRecord) error
}

// ValidateFilter checks the mandatory tenant scope, time range and page cap.
// maxWindow and maxPage are enforced by the caller and must be positive.
func ValidateFilter(f Filter, maxWindow time.Duration, maxPage int) error {
	if maxWindow <= 0 || maxPage <= 0 {
		return ErrInvalidFilter
	}
	if !f.CrossTenant && f.TenantID == "" {
		return ErrInvalidFilter
	}
	if f.From.IsZero() || f.To.IsZero() || !f.To.After(f.From) {
		return ErrInvalidFilter
	}
	if f.To.Sub(f.From) > maxWindow {
		return ErrInvalidFilter
	}
	if f.PageSize <= 0 || f.PageSize > maxPage {
		return ErrInvalidFilter
	}
	return nil
}

// Digest computes the stable digest for a query result set.
func Digest(events []audit.Event) string {
	hasher := sha256.New()
	for _, event := range events {
		encoded, _ := json.Marshal(event)
		hasher.Write(encoded)
		hasher.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// FilterDigest computes a stable digest of the effective filter.
func FilterDigest(f Filter) string {
	hasher := sha256.New()
	encoded, _ := json.Marshal(struct {
		TenantID    string    `json:"tenant_id"`
		CrossTenant bool      `json:"cross_tenant"`
		From        time.Time `json:"from"`
		To          time.Time `json:"to"`
		PageSize    int       `json:"page_size"`
		Cursor      string    `json:"cursor"`
	}{f.TenantID, f.CrossTenant, f.From.UTC(), f.To.UTC(), f.PageSize, f.Cursor})
	hasher.Write(encoded)
	return hex.EncodeToString(hasher.Sum(nil))
}

// NewQueryID returns a fresh random query identifier.
func NewQueryID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "qry_" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return "qry_" + hex.EncodeToString(buf)
}
