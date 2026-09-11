package domain

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func entries() []Entry {
	return []Entry{
		{ID: "pg", Revision: 1, Label: "SQL", Kind: PostgreSQL, Roles: []Role{Session, Memory}, Enabled: true, TenantIDs: []string{"tenant-a", "tenant-b"}},
		{ID: "redis", Revision: 2, Label: "Redis", Kind: Redis, Roles: []Role{Session}, Enabled: true, TenantIDs: []string{"tenant-a"}},
		{ID: "vectors", Revision: 1, Label: "Knowledge", Kind: Qdrant, Roles: []Role{Knowledge}, Enabled: true, TenantIDs: []string{"tenant-a"}},
		{ID: "objects", Revision: 1, Label: "Artifacts", Kind: S3, Roles: []Role{Artifact}, Enabled: true, TenantIDs: []string{"tenant-a"}},
	}
}
func TestFourKindsExplicitSelection(t *testing.T) {
	c, err := NewCatalog(entries())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries() {
		for _, r := range e.Roles {
			v, err := c.Resolve("tenant-a", Selection{e.ID, e.Revision, r})
			if err != nil || v.ID != e.ID {
				t.Fatalf("%s %s: %v", e.ID, r, err)
			}
		}
	}
	if _, err := c.Resolve("tenant-a", Selection{"redis", 2, Memory}); !errors.Is(err, ErrCapability) {
		t.Fatal(err)
	}
	if _, err := c.Resolve("tenant-a", Selection{"pg", 2, Session}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatal(err)
	}
}
func TestTenantIsolationAndProjection(t *testing.T) {
	c, _ := NewCatalog(entries())
	vs, err := c.List("tenant-b")
	if err != nil || len(vs) != 1 || vs[0].ID != "pg" {
		t.Fatal(vs, err)
	}
	for _, id := range []string{"redis", "missing"} {
		if _, err := c.Resolve("tenant-b", Selection{id, 2, Session}); err != ErrNotAvailable {
			t.Fatal(err)
		}
	}
	b, _ := json.Marshal(vs)
	for _, forbidden := range []string{"tenant-a", "TenantIDs", "password", "target"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatalf("leaked %s", forbidden)
		}
	}
	empty, err := c.List("tenant-c")
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatal(empty, err)
	}
}
func TestImmutableCatalog(t *testing.T) {
	es := entries()
	c, _ := NewCatalog(es)
	before, _ := c.List("tenant-a")
	es[0].Roles[0] = Artifact
	es[0].TenantIDs[0] = "tenant-c"
	es[0].Label = "changed"
	vs, _ := c.List("tenant-a")
	vs[0].Roles[0] = Artifact
	v, _ := c.Resolve("tenant-a", Selection{"pg", 1, Session})
	v.Roles[0] = Artifact
	after, _ := c.List("tenant-a")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("catalog mutated")
	}
}
func TestDisabledIsVisibleButNotResolvable(t *testing.T) {
	es := entries()
	es[0].Enabled = false
	c, _ := NewCatalog(es)
	vs, _ := c.List("tenant-b")
	if len(vs) != 1 || vs[0].Available {
		t.Fatal(vs)
	}
	if _, err := c.Resolve("tenant-b", Selection{"pg", 1, Session}); err != ErrUnavailable {
		t.Fatal(err)
	}
}
func TestClosedConfiguration(t *testing.T) {
	mutations := []func(*Entry){
		func(e *Entry) { e.ID = "../pg" }, func(e *Entry) { e.Revision = 0 }, func(e *Entry) { e.Label = "" },
		func(e *Entry) { e.Kind = "unknown" }, func(e *Entry) { e.Roles = nil }, func(e *Entry) { e.Roles = []Role{Artifact} },
		func(e *Entry) { e.Roles = []Role{Session, Session} }, func(e *Entry) { e.TenantIDs = nil },
		func(e *Entry) { e.TenantIDs = []string{"*"} }, func(e *Entry) { e.TenantIDs = []string{"tenant-a", "tenant-a"} },
	}
	for i, m := range mutations {
		es := entries()
		m(&es[0])
		if _, err := NewCatalog(es); err != ErrInvalid {
			t.Fatalf("case %d: %v", i, err)
		}
	}
	es := entries()
	es = append(es, es[0])
	if _, err := NewCatalog(es); err != ErrInvalid {
		t.Fatal(err)
	}
	var c *Catalog
	if _, err := c.List("tenant-a"); err != ErrNotAvailable {
		t.Fatal(err)
	}
	if _, err := c.Resolve("tenant-a", Selection{}); err != ErrNotAvailable {
		t.Fatal(err)
	}
}

func TestCatalogRevisionIsJavaScriptSafe(t *testing.T) {
	for _, revision := range []uint64{0, 9007199254740992, ^uint64(0)} {
		es := entries()
		es[0].Revision = revision
		if _, err := NewCatalog(es); !errors.Is(err, ErrInvalid) {
			t.Fatalf("revision %d: %v", revision, err)
		}
	}
	es := entries()
	es[0].Revision = 9007199254740991
	c, err := NewCatalog(es)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Resolve("tenant-a", Selection{"pg", es[0].Revision, Session})
	if err != nil || v.Revision != es[0].Revision {
		t.Fatal(v, err)
	}
	b, err := json.Marshal(v)
	if err != nil || !strings.Contains(string(b), `"revision":9007199254740991`) {
		t.Fatal(string(b), err)
	}
}
