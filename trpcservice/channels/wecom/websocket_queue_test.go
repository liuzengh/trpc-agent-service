package wecom

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketClientDispatchesHandlerOutsideReadLoop(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErrors := make(chan error, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		auth, err := readWeComFrame(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		if err := writeWeComAck(conn, auth.Headers.ReqID); err != nil {
			serverErrors <- err
			return
		}
		if err := writeWeComFrame(conn, protocolFrame{
			Cmd: "aibot_msg_callback", Headers: frameHeaders{ReqID: "incoming-queue"},
			Body: bodyBytes(Message{MessageID: "message-queue", AIBotID: "bot-id", ChatType: "single", From: MessageFrom{UserID: "user-1"}, MessageType: "text", Text: MessageText{Content: "hello"}}),
		}); err != nil {
			serverErrors <- err
			return
		}
		send, err := readWeComFrame(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		if send.Cmd != "aibot_send_msg" {
			serverErrors <- errors.New("client did not process the inbound callback while handler was blocked")
			return
		}
		if err := writeWeComAck(conn, send.Headers.ReqID); err != nil {
			serverErrors <- err
		}
	}))
	defer server.Close()

	handlerStarted := make(chan struct{})
	var handlerStartedOnce sync.Once
	releaseHandler := make(chan struct{})
	client, err := NewClient(
		"bot-id", "bot-secret",
		WithWebSocketURL("ws"+strings.TrimPrefix(server.URL, "http")),
		WithWebSocketReconnectDelay(time.Millisecond, 5*time.Millisecond),
		WithMessageHandler(func(context.Context, Message) error {
			handlerStartedOnce.Do(func() { close(handlerStarted) })
			<-releaseHandler
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(runCtx) }()
	select {
	case <-handlerStarted:
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued handler")
	}

	sendCtx, sendCancel := context.WithTimeout(context.Background(), time.Second)
	defer sendCancel()
	if _, err := client.SendMessage(sendCtx, "user-1", "reply"); err != nil {
		t.Fatalf("send message while inbound handler blocked: %v", err)
	}
	close(releaseHandler)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close client: %v", err)
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client shutdown")
	}
	select {
	case err := <-serverErrors:
		t.Fatal(err)
	default:
	}
}

func TestWebSocketClientReportsFullHandlerQueue(t *testing.T) {
	client, err := NewClient(
		"bot-id", "bot-secret",
		WithMessageQueueSize(1),
	)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	client.messageQueue <- queuedMessage{}
	if err := client.enqueueMessage(queuedMessage{cmd: "aibot_msg_callback", body: []byte(`{"msgid":"message-queue-full"}`)}); !errors.Is(err, errWebSocketHandlerQueueFull) {
		t.Fatalf("full queue error = %v, want queue-full error", err)
	}
}

func TestWebSocketClientAllowsLaterMessageWhileHandlerIsBlocked(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErrors := make(chan error, 2)
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		auth, err := readWeComFrame(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		if err := writeWeComAck(conn, auth.Headers.ReqID); err != nil {
			serverErrors <- err
			return
		}
		for _, message := range []Message{
			{MessageID: "message-a", AIBotID: "bot-id", ChatType: "single", From: MessageFrom{UserID: "user-1"}, MessageType: "text", Text: MessageText{Content: "slow attachment"}},
			{MessageID: "message-b", AIBotID: "bot-id", ChatType: "single", From: MessageFrom{UserID: "user-1"}, MessageType: "text", Text: MessageText{Content: "later text"}},
		} {
			if err := writeWeComFrame(conn, protocolFrame{
				Cmd: "aibot_msg_callback", Headers: frameHeaders{ReqID: message.MessageID},
				Body: bodyBytes(message),
			}); err != nil {
				serverErrors <- err
				return
			}
		}
		<-serverDone
	}))
	defer server.Close()

	startedA := make(chan struct{})
	startedB := make(chan struct{})
	releaseA := make(chan struct{})
	var onceA, onceB sync.Once
	client, err := NewClient(
		"bot-id", "bot-secret",
		WithWebSocketURL("ws"+strings.TrimPrefix(server.URL, "http")),
		WithWebSocketReconnectDelay(time.Millisecond, 5*time.Millisecond),
		WithMessageHandler(func(_ context.Context, message Message) error {
			switch message.MessageID {
			case "message-a":
				onceA.Do(func() { close(startedA) })
				<-releaseA
			case "message-b":
				<-startedA
				onceB.Do(func() { close(startedB) })
			}
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(runCtx) }()
	select {
	case <-startedA:
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first handler")
	}
	select {
	case <-startedB:
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("later message remained blocked behind first handler")
	}

	close(releaseA)
	close(serverDone)
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("close client: %v", err)
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client shutdown")
	}
	select {
	case err := <-serverErrors:
		t.Fatal(err)
	default:
	}
}
