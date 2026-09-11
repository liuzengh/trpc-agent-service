package skill

import (
	"context"
	"sort"
)

// WithManagedStore is configured once, before any concurrent use of Registry.
func (r *Registry) WithManagedStore(store *Store) *Registry {
	if r != nil {
		r.managed = store
	}
	return r
}
func (r *Registry) HasDeploymentVersion(name, version string) bool {
	if r == nil {
		return false
	}
	_, ok := r.bundles[key(name, version)]
	return ok
}

func (r *Registry) ListContext(ctx context.Context, tenant string) ([]Descriptor, error) {
	out := r.List(tenant)
	if r == nil || r.managed == nil {
		return out, nil
	}
	after := ""
	for {
		page, next, err := r.managed.List(ctx, tenant, after, true)
		if err != nil {
			return nil, err
		}
		for _, d := range page {
			if !r.HasDeploymentVersion(d.Name, d.Version) {
				out = append(out, d.Descriptor)
			}
		}
		if next == "" {
			break
		}
		after = next
	}
	sort.Slice(out, func(i, j int) bool { return key(out[i].Name, out[i].Version) < key(out[j].Name, out[j].Version) })
	return out, nil
}
