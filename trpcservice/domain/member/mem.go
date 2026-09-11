package member

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type memStore struct {
	mu      sync.RWMutex
	members map[string]*Member // key: userID; one member belongs to one tenant
}

func NewMemStore() Store {
	return &memStore{members: make(map[string]*Member)}
}

func (s *memStore) Create(_ context.Context, m *Member) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.members[m.UserID]; exists {
		return fmt.Errorf("member %s already exists", m.UserID)
	}
	cp := *m
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now()
	}
	s.members[m.UserID] = &cp
	return nil
}

func (s *memStore) Get(_ context.Context, tenantID, userID string) (*Member, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.members[userID]
	if !ok || m.TenantID != tenantID {
		return nil, fmt.Errorf("member %s/%s not found", tenantID, userID)
	}
	cp := *m
	return &cp, nil
}

func (s *memStore) GetByUserID(_ context.Context, userID string) (*Member, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.members[userID]
	if !ok {
		return nil, fmt.Errorf("member %s not found", userID)
	}
	cp := *m
	return &cp, nil
}

func (s *memStore) List(_ context.Context, tenantID string) ([]*Member, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Member
	for _, m := range s.members {
		// An empty tenant means "every tenant" (the platform owner's view).
		if tenantID == "" || m.TenantID == tenantID {
			cp := *m
			result = append(result, &cp)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TenantID != result[j].TenantID {
			return result[i].TenantID < result[j].TenantID
		}
		return result[i].UserID < result[j].UserID
	})
	return result, nil
}

func (s *memStore) UpdatePassword(_ context.Context, tenantID, userID, passwordHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.members[userID]
	if !ok || m.TenantID != tenantID {
		return fmt.Errorf("member %s/%s not found", tenantID, userID)
	}
	m.Password = passwordHash
	return nil
}

func (s *memStore) UpdateRole(_ context.Context, tenantID, userID, role string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.members[userID]
	if !ok || m.TenantID != tenantID {
		return fmt.Errorf("member %s/%s not found", tenantID, userID)
	}
	m.Role = role
	return nil
}

func (s *memStore) Delete(_ context.Context, tenantID, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.members[userID]
	if !ok || m.TenantID != tenantID {
		return fmt.Errorf("member %s/%s not found", tenantID, userID)
	}
	delete(s.members, m.UserID)
	return nil
}
