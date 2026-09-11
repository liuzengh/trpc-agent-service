package eventadapter_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/eventadapter"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

// Isolate the public embedded-schema input in a fresh process, before its lazy
// compiler runs. No production decoder bypass or package-private test hook is used.
func TestSchemaFailureRemainsUnavailable(t *testing.T) {
	if os.Getenv("GATEWAY_SCHEMA_FAILURE_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSchemaFailureRemainsUnavailable$", "-test.count=1")
		cmd.Env = append(os.Environ(), "GATEWAY_SCHEMA_FAILURE_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated schema failure: %v\n%s", err, out)
		}
		return
	}
	wire.ReplyIntentSchema = []byte(`{`)
	calls := 0
	h, err := eventadapter.NewHandler(acceptFunc(func(context.Context, domain.Intent) (domain.Receipt, error) { calls++; return domain.Receipt{}, nil }))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, err := h.Handle(context.Background(), []byte(validFinal))
		if !errors.Is(err, domain.ErrUnavailable) || errors.Is(err, domain.ErrInvalid) || got != (domain.Receipt{}) || calls != 0 {
			t.Fatalf("internal decoder failure became permanent input failure: receipt=%v err=%v calls=%d", got, err, calls)
		}
	}
}
