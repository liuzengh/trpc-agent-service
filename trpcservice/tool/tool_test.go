package tool

import "testing"

func TestDefaultCatalogResolve(t *testing.T) {
	catalog := DefaultCatalog()
	if len(catalog.Names()) != 9 {
		t.Fatalf("tool names = %#v", catalog.Names())
	}
	resolved, err := catalog.Resolve([]string{"echo", "current_time", "echo"})
	if err != nil || len(resolved) != 2 {
		t.Fatalf("resolved=%d err=%v", len(resolved), err)
	}
	if _, err := catalog.Resolve([]string{"unknown"}); err == nil {
		t.Fatal("expected unknown tool error")
	}
}
