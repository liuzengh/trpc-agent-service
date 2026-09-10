package inmemory

import (
	"context"
	"strconv"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
)

type entry struct {
	value skill.Package
	state string
}
type Catalog struct {
	mu     sync.Mutex
	values map[string]entry
}

func New() *Catalog { return &Catalog{values: map[string]entry{}} }
func key(tenant, id string, version int64) string {
	return tenant + "\x00" + id + "\x00" + strconv.FormatInt(version, 10)
}
func (c *Catalog) Stage(ctx context.Context, in skill.Package) (skill.Package, error) {
	if err := ctx.Err(); err != nil {
		return skill.Package{}, err
	}
	value, err := skill.ValidatePackage(in)
	if err != nil {
		return skill.Package{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := key(value.TenantID, value.SkillID, value.Version)
	if old, ok := c.values[k]; ok {
		if old.value != value {
			return skill.Package{}, runtime.ErrIdempotencyCollision
		}
		return old.value, nil
	}
	c.values[k] = entry{value: value, state: "staged"}
	return value, nil
}
func (c *Catalog) Publish(ctx context.Context, tenant, id string, version int64) (skill.Package, error) {
	if err := ctx.Err(); err != nil {
		return skill.Package{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := key(tenant, id, version)
	old, ok := c.values[k]
	if !ok {
		return skill.Package{}, runtime.ErrNotFound
	}
	if old.state == "published" {
		return old.value, nil
	}
	if old.state != "staged" {
		return skill.Package{}, runtime.ErrVersionConflict
	}
	old.state = "published"
	c.values[k] = old
	return old.value, nil
}
func (c *Catalog) Resolve(ctx context.Context, tenant, id string, version int64) (skill.Package, error) {
	if err := ctx.Err(); err != nil {
		return skill.Package{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.values[key(tenant, id, version)]
	if !ok {
		return skill.Package{}, runtime.ErrNotFound
	}
	if old.state != "published" {
		return skill.Package{}, runtime.ErrVersionMismatch
	}
	return old.value, nil
}

var _ skill.LifecycleCatalog = (*Catalog)(nil)
