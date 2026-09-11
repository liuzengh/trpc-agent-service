package wecom_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)

// Inspect bytes on the real HTTP/1 wire, before net/http normalizes field names.
// The official endpoint rejects Go's Sec-Websocket-* spelling with HTTP 404.
func TestHandshakePreservesProviderHeaderSpelling(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	rawHeaders := make(chan string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(conn)
		var raw strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			raw.WriteString(line)
			if line == "\r\n" {
				break
			}
		}
		rawHeaders <- raw.String()
		_, _ = conn.Write([]byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = wecom.ProbeAuthentication(ctx, wecom.Config{BotID: "fixture-bot", Secret: "fixture-secret", URL: "ws://" + listener.Addr().String()})
	select {
	case raw := <-rawHeaders:
		for _, key := range []string{"Sec-WebSocket-Key: ", "Sec-WebSocket-Version: 13"} {
			if !strings.Contains(raw, key) {
				t.Errorf("missing required HTTP/1 header spelling %s", key)
			}
		}
		if strings.Contains(raw, "fixture-secret") {
			t.Error("credential leaked into HTTP handshake")
		}
	case <-ctx.Done():
		t.Fatal("no HTTP handshake observed")
	}
}
