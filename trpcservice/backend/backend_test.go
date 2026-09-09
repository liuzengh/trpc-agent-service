package backend

import (
	"context"
	"testing"
)

func TestBackendsRejectMissingConfiguration(t *testing.T) {
	if _, err := OpenPostgres(nil, "postgres://invalid"); err == nil {
		t.Fatal("OpenPostgres(nil) error = nil")
	}
	if _, err := OpenPostgres(context.Background(), ""); err == nil {
		t.Fatal("OpenPostgres() error = nil")
	}
	if _, err := OpenRedis(context.Background(), ""); err == nil {
		t.Fatal("OpenRedis() error = nil")
	}
	if _, err := OpenRedis(context.Background(), "://bad"); err == nil {
		t.Fatal("OpenRedis(invalid) error = nil")
	}
}

func TestNilRedisAdapterFailsClosed(t *testing.T) {
	var client *Redis
	ctx := context.Background()
	if err := client.Ping(ctx); err == nil {
		t.Fatal("nil Ping accepted")
	}
	if _, err := client.Eval(ctx, "return 1", nil); err == nil {
		t.Fatal("nil Eval accepted")
	}
	if err := client.Publish(ctx, "channel", nil); err == nil {
		t.Fatal("nil Publish accepted")
	}
	if _, _, err := client.Subscribe(ctx, "channel"); err == nil {
		t.Fatal("nil Subscribe accepted")
	}
	if err := client.CreateConsumerGroup(ctx, "stream", "group"); err == nil {
		t.Fatal("nil CreateConsumerGroup accepted")
	}
	if err := client.AddStream(ctx, "stream", []byte("synthetic")); err == nil {
		t.Fatal("nil AddStream accepted")
	}
	if _, err := client.ReadGroup(ctx, "stream", "group", "consumer", ">", 1, 0); err == nil {
		t.Fatal("nil ReadGroup accepted")
	}
	if err := client.AckStream(ctx, "stream", "group", "id"); err == nil {
		t.Fatal("nil AckStream accepted")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("nil Close error=%v", err)
	}
}
