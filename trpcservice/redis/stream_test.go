package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

func TestStreamReceiveReclaimsLaterPendingAfterOwnedPages(t *testing.T) {
	stream, mini := newTestStream(t, 100*time.Millisecond)

	for index := 0; index < autoClaimPageSize+1; index++ {
		dispatch := queue.Dispatch{
			OutboxID:  int64(index + 1),
			TenantID:  "tenant-a",
			AppID:     "support",
			RequestID: "request-active-" + uuid.NewString(),
		}
		if err := stream.Publish(context.Background(), dispatch); err != nil {
			t.Fatalf("publish active dispatch %d: %v", index, err)
		}
		if _, err := receiveTestDelivery(t, stream, "consumer-active"); err != nil {
			t.Fatalf("receive active dispatch %d: %v", index, err)
		}
	}

	abandoned := queue.Dispatch{
		OutboxID:  1000,
		TenantID:  "tenant-a",
		AppID:     "support",
		RequestID: "request-abandoned-" + uuid.NewString(),
	}
	if err := stream.Publish(context.Background(), abandoned); err != nil {
		t.Fatalf("publish abandoned dispatch: %v", err)
	}
	first, err := receiveTestDelivery(t, stream, "consumer-abandoned")
	if err != nil {
		t.Fatalf("receive abandoned dispatch: %v", err)
	}

	// Make every pending entry eligible. The abandoned entry is behind more
	// than one XAUTOCLAIM page of entries owned by the active consumer.
	mini.SetTime(time.Now().UTC().Add(time.Hour))
	recovered, err := receiveTestDelivery(t, stream, "consumer-active")
	if err != nil {
		t.Fatalf("reclaim abandoned dispatch: %v", err)
	}
	if recovered.ID != first.ID || recovered.Dispatch != abandoned {
		t.Fatalf("recovered delivery = %#v, want id=%q dispatch=%#v", recovered, first.ID, abandoned)
	}
}

func TestStreamReceiveDoesNotReclaimBeforeMinIdle(t *testing.T) {
	stream, _ := newTestStream(t, time.Hour)
	dispatch := queue.Dispatch{
		OutboxID:  1,
		TenantID:  "tenant-a",
		AppID:     "support",
		RequestID: "request-not-idle-" + uuid.NewString(),
	}
	if err := stream.Publish(context.Background(), dispatch); err != nil {
		t.Fatalf("publish dispatch: %v", err)
	}
	if _, err := receiveTestDelivery(t, stream, "consumer-owner"); err != nil {
		t.Fatalf("receive pending dispatch: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	delivery, err := stream.Receive(ctx, "consumer-reclaimer", 50*time.Millisecond)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receive before min idle = delivery=%#v err=%v, want deadline", delivery, err)
	}
	if delivery.ID != "" {
		t.Fatalf("reclaimed delivery before min idle: %#v", delivery)
	}
}

func TestStreamReceiveStopsOnNonAdvancingAutoClaimCursor(t *testing.T) {
	server := newScriptedRedisServer(t, "1-0")
	stream := newTestStreamWithURL(t, "redis://"+server.Addr()+"?protocol=2", time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	delivery, err := stream.Receive(ctx, "consumer-reclaimer", 25*time.Millisecond)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receive with non-advancing cursor = delivery=%#v err=%v, want deadline", delivery, err)
	}
	if delivery.ID != "" {
		t.Fatalf("received delivery from empty stream: %#v", delivery)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("receive exceeded cursor scan bound: %s", elapsed)
	}
	if calls := server.autoClaimCalls.Load(); calls != 2 {
		t.Fatalf("XAUTOCLAIM calls = %d, want two calls before repeated-cursor guard", calls)
	}
}

func newTestStream(t *testing.T, minIdle time.Duration) (*Stream, *miniredis.Miniredis) {
	t.Helper()
	mini, err := miniredis.Run()
	if err != nil {
		t.Fatalf("run miniredis: %v", err)
	}
	t.Cleanup(mini.Close)
	mini.SetTime(time.Now().UTC())
	return newTestStreamWithURL(t, "redis://"+mini.Addr(), minIdle), mini
}

func newTestStreamWithURL(t *testing.T, rawURL string, minIdle time.Duration) *Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := NewClient(ctx, rawURL)
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	stream, err := NewStream(client, "test-stream:"+uuid.NewString(), "workers", minIdle)
	if err != nil {
		t.Fatalf("new redis stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("init redis stream: %v", err)
	}
	return stream
}

func receiveTestDelivery(t *testing.T, stream *Stream, consumer string) (queue.Delivery, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return stream.Receive(ctx, consumer, time.Second)
}

// scriptedRedisServer is a minimal Redis protocol server for injecting a
// malformed XAUTOCLAIM cursor without changing the production client.
type scriptedRedisServer struct {
	listener       net.Listener
	cursor         string
	autoClaimCalls atomic.Int32
	wg             sync.WaitGroup
}

func newScriptedRedisServer(t *testing.T, cursor string) *scriptedRedisServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen scripted redis: %v", err)
	}
	server := &scriptedRedisServer{listener: listener, cursor: cursor}
	server.wg.Add(1)
	go server.serve()
	t.Cleanup(func() {
		_ = listener.Close()
		server.wg.Wait()
	})
	return server
}

func (s *scriptedRedisServer) Addr() string {
	return s.listener.Addr().String()
}

func (s *scriptedRedisServer) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
}

func (s *scriptedRedisServer) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		command, err := readRedisCommand(reader)
		if err != nil {
			return
		}
		switch strings.ToLower(command) {
		case "hello":
			_, _ = io.WriteString(conn, "-ERR unknown command 'hello'\r\n")
		case "ping":
			_, _ = io.WriteString(conn, "+PONG\r\n")
		case "xautoclaim":
			s.autoClaimCalls.Add(1)
			_, _ = io.WriteString(conn, "*3\r\n$3\r\n"+s.cursor+"\r\n*0\r\n*0\r\n")
		case "xreadgroup":
			_, _ = io.WriteString(conn, "*-1\r\n")
		default:
			_, _ = io.WriteString(conn, "+OK\r\n")
		}
	}
}

func readRedisCommand(reader *bufio.Reader) (string, error) {
	line, err := readRedisRESPLine(reader)
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[0] != '*' {
		return "", errors.New("redis command is not an array")
	}
	count, err := strconv.Atoi(string(line[1:]))
	if err != nil || count <= 0 {
		return "", errors.New("redis command array length is invalid")
	}
	var command string
	for index := 0; index < count; index++ {
		line, err := readRedisRESPLine(reader)
		if err != nil {
			return "", err
		}
		if len(line) < 2 || line[0] != '$' {
			return "", errors.New("redis command argument is not a bulk string")
		}
		length, err := strconv.Atoi(string(line[1:]))
		if err != nil || length < 0 {
			return "", errors.New("redis command argument length is invalid")
		}
		value := make([]byte, length)
		if _, err := io.ReadFull(reader, value); err != nil {
			return "", err
		}
		if err := readRedisRESPCRLF(reader); err != nil {
			return "", err
		}
		if index == 0 {
			command = string(value)
		}
	}
	return command, nil
}

func readRedisRESPLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("invalid redis RESP line ending")
	}
	return line[:len(line)-2], nil
}

func readRedisRESPCRLF(reader *bufio.Reader) error {
	var suffix [2]byte
	if _, err := io.ReadFull(reader, suffix[:]); err != nil {
		return err
	}
	if suffix != [2]byte{'\r', '\n'} {
		return errors.New("invalid redis RESP bulk ending")
	}
	return nil
}
