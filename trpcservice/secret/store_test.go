package secret

import (
	"context"
	"errors"
	"testing"
)

func TestEnvAndStaticStore(t *testing.T) {
	t.Setenv("SECRET_STORE_TEST", "value")
	value, err := (EnvStore{}).Resolve(context.Background(), "env://SECRET_STORE_TEST")
	if err != nil || value != "value" {
		t.Fatalf("env value=%q err=%v", value, err)
	}
	static := StaticStore{"secret://one": "static"}
	value, err = static.Resolve(context.Background(), "secret://one")
	if err != nil || value != "static" {
		t.Fatalf("static value=%q err=%v", value, err)
	}
	if _, err := static.Resolve(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing error=%v", err)
	}
}
