package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestSampleRequiresExplicitAuthorization(t *testing.T) {
	for _, args := range [][]string{{}, {"-mode", "sessions"}, {"-mode", "message_aibot_send", "-authorized"}, {"-apikey=secret-canary"}} {
		var out bytes.Buffer
		err := run(args, &out)
		if err == nil || out.Len() != 0 || strings.Contains(err.Error(), "secret-canary") {
			t.Fatalf("unsafe sample invocation: %v", err)
		}
	}
}
