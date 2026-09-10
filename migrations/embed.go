// Package migrations embeds immutable, versioned PostgreSQL migrations.
package migrations

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

type Migration struct{ Version, Name, Up, Down string }

func All() ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	byVersion := make(map[string]*Migration)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		parts := strings.SplitN(strings.TrimSuffix(entry.Name(), ".sql"), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid migration name %q", entry.Name())
		}
		nameParts := strings.SplitN(parts[1], ".", 2)
		if len(nameParts) != 2 || (nameParts[1] != "up" && nameParts[1] != "down") {
			return nil, fmt.Errorf("invalid migration direction %q", entry.Name())
		}
		body, err := files.ReadFile(entry.Name())
		if err != nil {
			return nil, err
		}
		migration := byVersion[parts[0]]
		if migration == nil {
			migration = &Migration{Version: parts[0], Name: nameParts[0]}
			byVersion[parts[0]] = migration
		}
		if migration.Name != nameParts[0] {
			return nil, fmt.Errorf("migration name mismatch for %s", parts[0])
		}
		if nameParts[1] == "up" {
			migration.Up = string(body)
		} else {
			migration.Down = string(body)
		}
	}
	versions := make([]string, 0, len(byVersion))
	for version := range byVersion {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	result := make([]Migration, 0, len(versions))
	for _, version := range versions {
		migration := byVersion[version]
		if migration.Up == "" || migration.Down == "" {
			return nil, fmt.Errorf("migration %s lacks up/down pair", version)
		}
		result = append(result, *migration)
	}
	return result, nil
}
