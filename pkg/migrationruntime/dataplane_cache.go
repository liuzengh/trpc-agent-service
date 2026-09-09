package migrationruntime

import (
	"context"
	"errors"
	"sync"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
)

type dataPlaneCacheEntry[T any] struct {
	value    T
	refs     int
	borrowed bool
}
type dataPlaneCache[T any] struct {
	mu         sync.Mutex
	closed     bool
	entries    map[string]*dataPlaneCacheEntry[T]
	open       func(context.Context, string) (T, error)
	closeValue func(T) error
}

func newDataPlaneCache[T any](profile string, inner T, open func(context.Context, string) (T, error), closeValue func(T) error) *dataPlaneCache[T] {
	return &dataPlaneCache[T]{entries: map[string]*dataPlaneCacheEntry[T]{profile: {value: inner, borrowed: true}}, open: open, closeValue: closeValue}
}

// Each cached Worker retains its borrowed original handle and at most one
// destination handle. Active borrowers are never closed during a route change.
func (c *dataPlaneCache[T]) borrow(ctx context.Context, profile string) (T, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero T
	if c.closed {
		return zero, nil, datamigration.ErrMigrationCapability
	}
	entry := c.entries[profile]
	if entry == nil {
		for key, value := range c.entries {
			if len(c.entries) < 2 {
				break
			}
			if !value.borrowed && value.refs == 0 {
				if err := c.closeValue(value.value); err != nil {
					return zero, nil, err
				}
				delete(c.entries, key)
			}
		}
		if len(c.entries) >= 2 {
			return zero, nil, datamigration.ErrMigrationCapability
		}
		value, err := c.open(ctx, profile)
		if err != nil {
			return zero, nil, err
		}
		entry = &dataPlaneCacheEntry[T]{value: value}
		c.entries[profile] = entry
	}
	entry.refs++
	var once sync.Once
	release := func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			entry.refs--
			if c.closed && entry.refs == 0 {
				if !entry.borrowed {
					_ = c.closeValue(entry.value)
				}
				delete(c.entries, profile)
			}
		})
	}
	return entry.value, release, nil
}
func (c *dataPlaneCache[T]) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var result error
	for profile, entry := range c.entries {
		if entry.refs == 0 {
			if !entry.borrowed {
				result = errors.Join(result, c.closeValue(entry.value))
			}
			delete(c.entries, profile)
		}
	}
	return result
}
