package database

import "testing"

func TestLoadMigrations(t *testing.T) {
	items, err := loadMigrations()
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("no embedded migrations")
	}
	for index, item := range items {
		if item.version <= 0 || item.name == "" || len(item.checksum) != 64 || item.sql == "" {
			t.Fatalf("invalid migration: %+v", item)
		}
		if index > 0 && items[index-1].version >= item.version {
			t.Fatalf("migrations are not sorted: %+v", items)
		}
	}
}
