package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestSampleRequiresExplicitAuthorization(t *testing.T) {
	for _, args := range [][]string{{}, {"-mode", "sessions"}, {"-mode", "group-messages"}, {"-mode", "group-reply", "-authorized"}, {"-mode", "group-reply", "-allow-send"}, {"-mode", "group-reply-read"}, {"-mode", "sessions", "-authorized", "-allow-send"}, {"-mode", "message_aibot_send", "-authorized"}, {"-apikey=secret-canary"}} {
		var out bytes.Buffer
		err := run(args, &out)
		if err == nil || out.Len() != 0 || strings.Contains(err.Error(), "secret-canary") {
			t.Fatalf("unsafe sample invocation: %v", err)
		}
	}
}
