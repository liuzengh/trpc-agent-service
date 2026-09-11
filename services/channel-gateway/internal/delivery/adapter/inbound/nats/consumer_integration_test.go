package natsadapter

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	event "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/eventadapter"
	pg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type failTransportWrite struct {
	ReceiptStore
	fail bool
}

func (s *failTransportWrite) RecordTransportReceipt(ctx context.Context, r d.TransportReceipt) error {
	if s.fail {
		return d.ErrUnavailable
	}
	return s.ReceiptStore.RecordTransportReceipt(ctx, r)
}
func TestReplyHandoffPostgresNATSIntegration(t *testing.T) {
	dsn, broker := os.Getenv("GATEWAY_REPLY_TEST_DATABASE_URL"), os.Getenv("GATEWAY_REPLY_TEST_NATS_URL")
	if dsn == "" || broker == "" {
		t.Skip("GATEWAY_REPLY_TEST_DATABASE_URL and GATEWAY_REPLY_TEST_NATS_URL required; fresh dedicated broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("gateway_reply_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, e := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); e != nil {
			t.Error(e)
		}
	}()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store, err := pg.NewStore(pool, nil, pg.Options{})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(broker)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	// Create fails on an existing stream: this test never purges shared state.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: StreamName, Subjects: []string{"execution.reply-intent.v1"}, Storage: jetstream.FileStorage, Retention: jetstream.WorkQueuePolicy, MaxBytes: 1 << 20, MaxMsgSize: 1 << 20, Discard: jetstream.DiscardNew})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if e := js.DeleteStream(cleanup, StreamName); e != nil {
			t.Error(e)
		}
	}()
	durable, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: DurableName, FilterSubject: "execution.reply-intent.v1", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxDeliver: -1})
	if err != nil {
		t.Fatal(err)
	}
	p := &proof{}
	a := &admission{}
	acceptor, err := app.NewAcceptor(store, a, p, app.AcceptOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := event.NewHandler(acceptor)
	if err != nil {
		t.Fatal(err)
	}
	writes := &failTransportWrite{ReceiptStore: store, fail: true}
	c, err := New(durable, stream, h, writes)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema_version":1,"intent_id":"intent","admission_id":"admission","run_id":"run","kind":"final","sequence":1,"execution":{"attempt_id":"attempt","completion_id":"completion","generation":1},"content":{"type":"text","text":"Final"},"deadline":"2030-01-01T00:00:00Z"}`)
	if _, err = js.Publish(ctx, "execution.reply-intent.v1", raw); err != nil {
		t.Fatal(err)
	}
	msg, err := durable.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.process(ctx, msg); err == nil {
		t.Fatal("fault injection did not hit second transaction")
	}
	var business, transport int
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_delivery_intents),(SELECT count(*) FROM gateway_reply_transport_receipts)`).Scan(&business, &transport); err != nil || business != 1 || transport != 0 {
		t.Fatal("crash cut", business, transport, err)
	}
	if err = msg.Nak(); err != nil {
		t.Fatal(err)
	}
	// New Acceptor and consumer, same SQL and durable: an offline proof service
	// and stopped admission cannot prevent replaying the persisted business receipt.
	p.err = d.ErrUnavailable
	restarted, err := app.NewAcceptor(store, a, p, app.AcceptOptions{})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Stop()
	h, err = event.NewHandler(restarted)
	if err != nil {
		t.Fatal(err)
	}
	c, err = New(durable, stream, h, store)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = durable.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.process(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_delivery_intents),(SELECT count(*) FROM gateway_reply_transport_receipts WHERE outcome='ACCEPTED')`).Scan(&business, &transport); err != nil || business != 1 || transport != 1 || p.calls != 1 {
		t.Fatal("replay", business, transport, p.calls, err)
	}
	// A separate malformed wire gets a durable terminal rejection before ACK.
	if _, err = js.Publish(ctx, "execution.reply-intent.v1", []byte(`{"schema_version":1,"schema_version":1}`)); err != nil {
		t.Fatal(err)
	}
	msg, err = durable.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = c.process(ctx, msg); err != nil {
		t.Fatal(err)
	}
	var invalid int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM gateway_reply_transport_receipts WHERE reason='INVALID_WIRE' AND outcome='REJECTED'`).Scan(&invalid); err != nil || invalid != 1 {
		t.Fatal(invalid, err)
	}
	info, err := durable.Info(ctx)
	if err != nil || info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatal("not drained", info, err)
	}
	// Concurrent retries cannot mutate accepted receipt evidence.
	infoStream, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	position := d.TransportPosition{StreamName: StreamName, StreamID: infoStream.Created.UTC().Format(time.RFC3339Nano), Sequence: 1}
	if err = pool.QueryRow(ctx, `SELECT raw_digest FROM gateway_reply_transport_receipts WHERE stream_sequence=1`).Scan(&position.RawDigest); err != nil {
		t.Fatal(err)
	}
	record, found, err := store.FindTransportReceipt(ctx, position)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- store.RecordTransportReceipt(ctx, record) }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	position.RawDigest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, _, err = store.FindTransportReceipt(ctx, position); err != d.ErrConflict {
		t.Fatal("transport content collision accepted", err)
	}
	t.Log("REPLY_HANDOFF_PASS: real PG/NATS; Delivery commit before transport crash; offline proof restart replay; strict-wire rejection; durable ACK; concurrent immutable receipts")
}
