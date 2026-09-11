package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWeComAccountProjectionStrictAndReloadable(t *testing.T) {
	valid := `[{"account_id":"acct","bot_id":"bot","revision":1,"enabled":true,"secret_env":"TEST_WECOM_SECRET"}]`
	for _, s := range []string{valid, "[]"} {
		if _, err := decodeWeComAccounts([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	bad := []string{"null", valid + "{}", `[{"account_id":"a","bot_id":"b","revision":1,"enabled":true}]`, strings.Replace(valid, `"revision":1`, `"revision":1.0`, 1), strings.Replace(valid, `"revision":1`, `"revision":0`, 1), strings.Replace(valid, `"enabled":true`, `"enabled":null`, 1), strings.Replace(valid, `"bot_id":"bot"`, `"bot_id":"bot","bot_id":"other"`, 1), strings.Replace(valid, `"bot_id"`, `"BOT_ID"`, 1), strings.Replace(valid, `"secret_env":"TEST_WECOM_SECRET"`, `"secret_env":"embedded secret"`, 1), strings.Replace(valid, `"bot_id":"bot"`, `"bot_id":"bot","token":"hidden"`, 1), valid[:len(valid)-1] + "," + valid[1:]}
	for _, s := range bad {
		if _, err := decodeWeComAccounts([]byte(s)); err == nil {
			t.Fatalf("accepted invalid projection %s", s)
		}
	}
	path := filepath.Join(t.TempDir(), "accounts.json")
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	source := accountFileSource{path: path}
	first, err := source.List(context.Background())
	if err != nil || len(first) != 1 || first[0].Revision != 1 {
		t.Fatal(first, err)
	}
	if err = os.WriteFile(path, []byte(strings.Replace(valid, `"revision":1`, `"revision":2`, 1)), 0600); err != nil {
		t.Fatal(err)
	}
	next, err := source.List(context.Background())
	if err != nil || next[0].Revision != 2 {
		t.Fatal(next, err)
	}
	if err = os.WriteFile(path, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = source.List(context.Background()); err == nil {
		t.Fatal("corrupt update reused stale projection")
	}
}
