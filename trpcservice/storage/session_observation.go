package storage

import (
	"context"
	"sort"
	"strings"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type SessionObservation struct {
	TenantID, BindingID, ConfigHash, State string
	ObservedAt                             time.Time
}

// Probe only initialized handles. Constructing a framework SQL service can
// initialize tables, so a health check must not initialize unused bindings.
func (r *SessionRouter) ObserveInitialized(ctx context.Context) []SessionObservation {
	r.mu.Lock()
	keys := make([]string, 0, len(r.services))
	for key := range r.services {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 0 && r.probeAfter != "" {
		start := sort.Search(len(keys), func(i int) bool { return keys[i] > r.probeAfter })
		keys = append(keys[start:], keys[:start]...)
	}
	if len(keys) > 32 {
		keys = keys[:32]
	}
	services := make(map[string]session.Service, len(keys))
	for _, key := range keys {
		services[key] = r.services[key]
	}
	if len(keys) > 0 {
		r.probeAfter = keys[len(keys)-1]
	}
	r.mu.Unlock()
	items := []SessionObservation{}
	for _, key := range keys {
		if ctx.Err() != nil {
			break
		}
		parts := strings.Split(key, "\x00")
		if len(parts) != 3 {
			continue
		}
		limited, cancel := context.WithTimeout(ctx, time.Second)
		_, err := services[key].ListAppStates(limited, readinessAppName)
		cancel()
		state := "ready"
		if err != nil {
			state = "unavailable"
		}
		items = append(items, SessionObservation{TenantID: parts[0], BindingID: parts[1], ConfigHash: parts[2], State: state, ObservedAt: time.Now().UTC()})
	}
	return items
}
