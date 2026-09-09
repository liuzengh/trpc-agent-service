package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

func TestWebhookRouteKeyMigrationRegeneratesLegacyKeys(t *testing.T) {
	up, err := fs.ReadFile(files, "026_webhook_route_key_backfill.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	script := string(up)
	for _, required := range []string{
		"CREATE EXTENSION IF NOT EXISTS pgcrypto",
		"SELECT t.config",
		"INTO config_value",
		"using the FOR cursor's initial snapshot",
		"binding.channel_config",
		"cannot be mapped uniquely",
		"resolved_index := -1",
		"SET channel_index = resolved_index",
		"gen_random_uuid()",
		"webhook_token = route_key",
		"jsonb_set(item, '{webhookKey}'",
		"Never preserve a legacy JSON webhookKey",
	} {
		if !strings.Contains(script, required) {
			t.Errorf("026 migration missing %q", required)
		}
	}
	if strings.Contains(script, "existing_key :=") || strings.Contains(script, "route_key := existing_key") {
		t.Fatal("026 migration must not preserve a legacy webhook key")
	}
}
