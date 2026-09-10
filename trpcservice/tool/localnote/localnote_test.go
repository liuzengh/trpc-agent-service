package localnote

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestRegistrationResolvesOnlyExactTenantVersion(t *testing.T) {
	catalog, err := servicetool.NewCatalog(Registration("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := servicetool.Resolver{Catalog: catalog}
	values, err := resolver.ResolveTools(context.Background(), "tenant-a", []profile.VersionedRef{{ID: ID, Version: Version}})
	if err != nil || len(values) != 1 || values[0].Declaration().Name != ID {
		t.Fatalf("values=%#v err=%v", values, err)
	}
	if _, err := resolver.ResolveTools(context.Background(), "tenant-b", []profile.VersionedRef{{ID: ID, Version: Version}}); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cross tenant resolve: %v", err)
	}
}

func TestToolCreatesStableResultOnlyForConfirmedArguments(t *testing.T) {
	arguments := []byte(`{"content":" approved content ","title":" Release note "}`)
	_, digest, err := governance.CanonicalArguments(arguments)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request-a",
		SubjectID: "user-a", ToolCallID: "call-a", ArgsDigest: digest})
	value, err := (Tool{}).Call(ctx, arguments)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := value.(Result)
	if !ok || result.Status != "created" || result.NoteID == "" || result.Title != "Release note" || result.Content != "approved content" {
		t.Fatalf("result=%#v", value)
	}
	replayed, err := (Tool{}).Call(ctx, []byte(`{"title":" Release note ","content":" approved content "}`))
	if err != nil || replayed.(Result).NoteID != result.NoteID {
		t.Fatalf("replay=%#v err=%v", replayed, err)
	}
	wrong := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request-a",
		SubjectID: "user-a", ToolCallID: "call-a", ArgsDigest: "wrong"})
	if _, err := (Tool{}).Call(wrong, arguments); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("args mismatch: %v", err)
	}
}

func TestToolRejectsUnknownAndUntrustedInput(t *testing.T) {
	arguments := []byte(`{"title":"note","content":"body","secret":"no"}`)
	_, digest, err := governance.CanonicalArguments(arguments)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request-a",
		SubjectID: "user-a", ToolCallID: "call-a", ArgsDigest: digest})
	if _, err := (Tool{}).Call(ctx, arguments); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("unknown field: %v", err)
	}
	if _, err := (Tool{}).Call(context.Background(), []byte(`{"title":"note","content":"body"}`)); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("untrusted context: %v", err)
	}
}

var _ agenttool.CallableTool = Tool{}
