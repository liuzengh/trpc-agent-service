package sessionstore

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestRedisSessionTargetAndKeyBoundary(t *testing.T) {
	base := RedisTarget{Host: "127.0.0.1", Port: 6379, Username: "session_runtime"}
	for _, change := range []func(*RedisTarget){func(t *RedisTarget) { t.Username = "default" }, func(t *RedisTarget) { t.Host = "redis://injected" }, func(t *RedisTarget) { t.Database = 16 }, func(t *RedisTarget) { t.MaxConcurrency = -1 }} {
		target := base
		change(&target)
		if _, err := OpenRedis(context.Background(), target, "secret-password", 1024); !errors.Is(err, ErrIdentity) || strings.Contains(err.Error(), "secret-password") {
			t.Fatal(err)
		}
	}
	if redisKey("tenant", "a:b", "c") == redisKey("tenant", "a", "b:c") || redisKey("tenant", "session", "ref") == redisKey("other", "session", "ref") {
		t.Fatal("key isolation lost")
	}
	if redisFailure(errors.New("redis://user:password@host")) != errRedisUnavailable {
		t.Fatal("driver diagnostic leaked")
	}
}

func TestRedisSessionRejectsUntrustedTLS(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("untrusted TLS reached application") }))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	host, portText, _ := net.SplitHostPort(server.Listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	target := RedisTarget{Host: host, Port: uint16(port), Username: "session_runtime", TLS: true, MaxConcurrency: 1}
	if _, err := OpenRedis(context.Background(), target, "private-fixture-password", 1024); err != errRedisUnavailable {
		t.Fatal("untrusted certificate accepted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := OpenRedis(ctx, target, "private-fixture-password", 1024); !errors.Is(err, context.Canceled) {
		t.Fatal("Open cancellation lost", err)
	}
}
