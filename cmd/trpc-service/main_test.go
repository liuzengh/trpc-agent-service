package main

import (
	"os"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
)

func TestNewResponderExplicitModes(t *testing.T) {
	responder, err := newResponder("echo")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := responder.(platform.EchoResponder); !ok {
		t.Fatalf("expected EchoResponder, got %T", responder)
	}
	t.Setenv("MODEL_BASE_URL", "http://127.0.0.1:1/v1")
	t.Setenv("MODEL_NAME", "test-model")
	t.Setenv("MODEL_API_KEY", "test-secret")
	responder, err = newResponder("runner")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := responder.(platform.RuntimeResponder); !ok {
		t.Fatalf("expected RuntimeResponder, got %T", responder)
	}
}

func TestNewResponderRejectsMissingRunnerConfig(t *testing.T) {
	for _, name := range []string{"MODEL_BASE_URL", "MODEL_NAME", "MODEL_API_KEY"} {
		_ = os.Unsetenv(name)
	}
	responder, err := newResponder("runner")
	if err == nil || responder != nil {
		t.Fatalf("expected runner configuration error, responder=%T err=%v", responder, err)
	}
}

func TestNewResponderRejectsUnknownMode(t *testing.T) {
	responder, err := newResponder("invalid")
	if err == nil {
		t.Fatal("expected unsupported provider mode to fail")
	}
	if responder != nil {
		t.Fatalf("expected no responder on configuration error, got %T", responder)
	}
}
