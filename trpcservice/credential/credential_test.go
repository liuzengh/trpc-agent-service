package credential

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerFetchesOnceUnderConcurrency(t *testing.T) {
	t.Parallel()
	var gettokenCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gettokenCalls.Add(1)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"errcode": 0, "errmsg": "ok", "access_token": "shared-token", "expires_in": 7200,
		})
	}))
	t.Cleanup(server.Close)

	manager := NewManager("corp-id", "corp-secret", server.URL, server.Client(), NewMemoryTokenCache())
	const workers = 16
	var wait sync.WaitGroup
	wait.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wait.Done()
			token, err := manager.Token(context.Background())
			if err != nil || token != "shared-token" {
				t.Errorf("Token() = %q, err = %v", token, err)
			}
		}()
	}
	wait.Wait()
	if got := gettokenCalls.Load(); got != 1 {
		t.Fatalf("gettoken calls = %d, want exactly 1 under concurrency", got)
	}
}

func TestManagerReusesCacheAcrossCalls(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"errcode": 0, "errmsg": "ok", "access_token": "cached", "expires_in": 7200,
		})
	}))
	t.Cleanup(server.Close)
	manager := NewManager("corp-id", "corp-secret", server.URL, server.Client(), NewMemoryTokenCache())
	for i := 0; i < 3; i++ {
		if _, err := manager.Token(context.Background()); err != nil {
			t.Fatalf("Token() #%d error = %v", i, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("gettoken calls = %d, want 1 (cache reuse)", got)
	}
}

func TestManagerInvalidateForcesRefetch(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"errcode": 0, "errmsg": "ok", "access_token": "round", "expires_in": 7200,
		})
	}))
	t.Cleanup(server.Close)
	cache := NewMemoryTokenCache()
	manager := NewManager("corp-id", "corp-secret", server.URL, server.Client(), cache)
	if _, err := manager.Token(context.Background()); err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	manager.Invalidate(context.Background())
	if _, err := manager.Token(context.Background()); err != nil {
		t.Fatalf("Token() after invalidate error = %v", err)
	}
}

func TestIsExpiredAccessTokenError(t *testing.T) {
	t.Parallel()
	for code, want := range map[int64]bool{40014: true, 42001: true, 0: false, 40003: false} {
		if got := IsExpiredAccessTokenError(code); got != want {
			t.Fatalf("IsExpiredAccessTokenError(%d) = %v, want %v", code, got, want)
		}
	}
}

func TestAccessTokenCacheKeyIsScopedByApplicationCredential(t *testing.T) {
	t.Parallel()
	sameA := accessTokenCacheKey("corp-a", "secret-a")
	sameB := accessTokenCacheKey("corp-a", "secret-a")
	differentApp := accessTokenCacheKey("corp-a", "secret-b")
	differentCorp := accessTokenCacheKey("corp-b", "secret-a")
	if sameA != sameB {
		t.Fatalf("same credentials produced different cache keys: %q != %q", sameA, sameB)
	}
	if sameA == differentApp || sameA == differentCorp {
		t.Fatalf("distinct WeCom credentials share cache key %q", sameA)
	}
	if strings.Contains(sameA, "secret-a") {
		t.Fatalf("cache key leaked credential material: %q", sameA)
	}
}

func TestMemoryTokenCacheExpiry(t *testing.T) {
	t.Parallel()
	cache := NewMemoryTokenCache()
	cache.Set(context.Background(), "k", "v", -time.Second)
	if _, ok := cache.Get(context.Background(), "k"); ok {
		t.Fatal("expired entry should be evicted")
	}
	cache.Set(context.Background(), "k", "v", time.Hour)
	if value, ok := cache.Get(context.Background(), "k"); !ok || value != "v" {
		t.Fatalf("Get() = %q, %v, want v, true", value, ok)
	}
}
