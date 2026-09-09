package platform

import (
	"errors"
	"sync"
)

var errBackendRegistryClosing = errors.New("platform: backend registry is closing")

type backendRegistry struct {
	mu           sync.Mutex
	defaultStore DataStore
	selections   map[string]backendSelection
	stores       map[string]DataStore
	active       map[DataStore]int
	retiring     map[DataStore]bool
	closing      bool
	leases       sync.WaitGroup
}

func newBackendRegistry(defaultStore DataStore, selections map[string]backendSelection) *backendRegistry {
	if selections == nil {
		selections = map[string]backendSelection{}
	}
	return &backendRegistry{
		defaultStore: defaultStore,
		selections:   selections,
		stores:       map[string]DataStore{},
		active:       map[DataStore]int{},
		retiring:     map[DataStore]bool{},
	}
}

func (r *backendRegistry) setSelections(selections map[string]backendSelection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selections = copyBackendSelections(selections)
}

func (r *backendRegistry) syncSelections(selections map[string]backendSelection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for tenantID, current := range r.selections {
		next, exists := selections[tenantID]
		if exists && next == current {
			continue
		}
		r.retireLocked(r.stores[tenantID])
		delete(r.stores, tenantID)
	}
	for tenantID, next := range selections {
		if current, exists := r.selections[tenantID]; !exists || current != next {
			r.retireLocked(r.stores[tenantID])
			delete(r.stores, tenantID)
		}
	}
	r.selections = copyBackendSelections(selections)
}

func (r *backendRegistry) selectionSnapshot() map[string]backendSelection {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyBackendSelections(r.selections)
}

func copyBackendSelections(selections map[string]backendSelection) map[string]backendSelection {
	result := make(map[string]backendSelection, len(selections))
	for key, value := range selections {
		result[key] = value
	}
	return result
}

func (r *backendRegistry) replaceDefault(store DataStore) {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.defaultStore
	r.defaultStore = store
	r.retireLocked(old)
}

func (r *backendRegistry) replace(tenantID string, selection backendSelection, store DataStore) {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.stores[tenantID]
	r.stores[tenantID] = store
	r.selections[tenantID] = selection
	r.retireLocked(old)
}

func (r *backendRegistry) acquire(tenantID string) (DataStore, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil, nil, errBackendRegistryClosing
	}
	store := r.stores[tenantID]
	if store == nil {
		if selection, selected := r.selections[tenantID]; selected {
			candidate, err := newBackendStore(selection)
			if err != nil || candidate == nil {
				candidate = &unavailableStore{backend: selection.Backend}
			}
			r.stores[tenantID] = candidate
			store = candidate
		} else {
			store = r.defaultStore
		}
	}
	r.leases.Add(1)
	r.active[store]++
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			r.mu.Lock()
			r.active[store]--
			shouldClose := r.active[store] == 0 && r.retiring[store]
			if shouldClose {
				closeBackend(r, store)
			}
			r.leases.Done()
			r.mu.Unlock()
		})
	}
	return store, release, nil
}

func (r *backendRegistry) beginClose() {
	r.mu.Lock()
	r.closing = true
	r.mu.Unlock()
}

func (r *backendRegistry) close() error {
	r.beginClose()
	r.leases.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	stores := map[DataStore]struct{}{}
	if r.defaultStore != nil {
		stores[r.defaultStore] = struct{}{}
	}
	for _, store := range r.stores {
		if store != nil {
			stores[store] = struct{}{}
		}
	}
	for store := range r.retiring {
		if store != nil {
			stores[store] = struct{}{}
		}
	}
	var first error
	for store := range stores {
		if err := closeDataStoreWithError(store); err != nil && first == nil {
			first = err
		}
		delete(r.active, store)
		delete(r.retiring, store)
	}
	r.stores = map[string]DataStore{}
	return first
}

func (r *backendRegistry) retireLocked(store DataStore) {
	if store == nil {
		return
	}
	r.retiring[store] = true
	if r.active[store] == 0 {
		closeBackend(r, store)
	}
}

func closeBackend(r *backendRegistry, store DataStore) {
	closeDataStore(store)
	delete(r.active, store)
	delete(r.retiring, store)
}

func closeDataStore(store DataStore) {
	_ = closeDataStoreWithError(store)
}

func closeDataStoreWithError(store DataStore) error {
	if closer, ok := store.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

func newBasicBackendStore(selection BackendEndpoint) (DataStore, error) {
	switch selection.Backend {
	case "inmemory":
		return NewInMemoryStore(), nil
	case "redis":
		return NewRedisStore(selection.Address), nil
	case "sqlite":
		return NewSQLiteStore(selection.Address)
	case "postgres":
		return NewPostgresStore(selection.Address)
	default:
		return nil, errors.New("platform: unsupported backend")
	}
}
