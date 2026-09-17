package inmemory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/query"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Store is an in-memory audit query store for contract and unit tests only.
type Store struct {
	mu      sync.Mutex
	events  []audit.Event
	records []query.QueryRecord
}

func New() *Store { return &Store{} }

func (s *Store) Seed(events ...audit.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, events...)
}

func (s *Store) Records() []query.QueryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]query.QueryRecord(nil), s.records...)
}

func (s *Store) Query(_ context.Context, filter query.Filter) (query.Page, error) {
	if s == nil {
		return query.Page{}, runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cursorOccurred, cursorTenant, cursorAudit, ok := decodeCursor(filter.Cursor)
	matched := make([]audit.Event, 0)
	for _, event := range s.events {
		if !filter.CrossTenant && event.TenantID != filter.TenantID {
			continue
		}
		if event.OccurredAt.Before(filter.From) || !event.OccurredAt.Before(filter.To) {
			continue
		}
		if ok && alreadyReturned(event, cursorOccurred, cursorTenant, cursorAudit) {
			continue
		}
		matched = append(matched, event)
	}
	sort.Slice(matched, func(i, j int) bool {
		if !matched[i].OccurredAt.Equal(matched[j].OccurredAt) {
			return matched[i].OccurredAt.After(matched[j].OccurredAt)
		}
		if matched[i].TenantID != matched[j].TenantID {
			return matched[i].TenantID > matched[j].TenantID
		}
		return matched[i].AuditID > matched[j].AuditID
	})
	hasMore := len(matched) > filter.PageSize
	if hasMore {
		matched = matched[:filter.PageSize]
	}
	page := query.Page{Events: matched}
	if hasMore && len(matched) > 0 {
		last := matched[len(matched)-1]
		page.NextCursor = last.OccurredAt.UTC().Format(time.RFC3339Nano) + "\n" + last.TenantID + "\n" + last.AuditID
	}
	return page, nil
}

func (s *Store) RecordAccess(_ context.Context, record query.QueryRecord) error {
	if s == nil {
		return runtime.ErrInvariantViolation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	return nil
}

// alreadyReturned reports whether (occurred, audit) sorts at or before the
// cursor in DESC order, i.e. (occurred, audit) >= (cursorOccurred, cursorAudit).
func alreadyReturned(event audit.Event, cursorOccurred time.Time, cursorTenant, cursorAudit string) bool {
	if event.OccurredAt.After(cursorOccurred) {
		return true
	}
	if !event.OccurredAt.Equal(cursorOccurred) {
		return false
	}
	if event.TenantID != cursorTenant {
		return event.TenantID > cursorTenant
	}
	return event.AuditID >= cursorAudit
}

func decodeCursor(cursor string) (time.Time, string, string, bool) {
	if cursor == "" {
		return time.Time{}, "", "", false
	}
	parts := strings.SplitN(cursor, "\n", 3)
	if len(parts) != 3 {
		return time.Time{}, "", "", false
	}
	occurred, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", "", false
	}
	return occurred, parts[1], parts[2], true
}

var _ query.Store = (*Store)(nil)
