package queue

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	postgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

type postgresQueueFixture struct {
	queue  *PostgresQueue
	pool   *pgxpool.Pool
	base   *pgxpool.Pool
	url    string
	ctx    context.Context
	cancel context.CancelFunc
	schema string
}

func newPostgresQueueFixture(t *testing.T) *postgresQueueFixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; PostgreSQL durable Queue not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	cfg := postgres.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1}
	base, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p009b_queue_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	migrator, err := postgres.NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../migrations")))
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	fixture := &postgresQueueFixture{pool: pool, base: base, url: url, ctx: ctx, cancel: cancel, schema: schema}
	fixture.addTenant(t, "queue-tenant-a")
	fixture.queue, err = NewPostgresQueue(pool, PostgresQueueConfig{
		MaxJobAge:              2 * time.Hour,
		MaxVisibilityExtension: 2 * time.Hour,
		PollInterval:           5 * time.Millisecond,
	})
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		fixture.queue.Close()
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	})
	return fixture
}

func (f *postgresQueueFixture) addTenant(t *testing.T, tenantID string) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO tenant (tenant_id, name) VALUES ($1, $1)`, tenantID); err != nil {
		t.Fatal(err)
	}
}

func (f *postgresQueueFixture) newQueue(t *testing.T) *PostgresQueue {
	t.Helper()
	queue, err := NewPostgresQueue(f.pool, PostgresQueueConfig{
		MaxJobAge:              2 * time.Hour,
		MaxVisibilityExtension: 2 * time.Hour,
		PollInterval:           5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return queue
}

func (f *postgresQueueFixture) newIndependentQueue(t *testing.T) (*PostgresQueue, *pgxpool.Pool) {
	t.Helper()
	pool, err := postgres.NewPool(f.ctx, postgres.PostgresConfig{
		URL: f.url, MaxConns: 2, MinConns: 1, SearchPath: f.schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewPostgresQueue(pool, PostgresQueueConfig{
		MaxJobAge:              2 * time.Hour,
		MaxVisibilityExtension: 2 * time.Hour,
		PollInterval:           5 * time.Millisecond,
	})
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return queue, pool
}

func durableTestJob(tenantID, jobID, executionID string) AgentJob {
	clock := &testClock{now: time.Now().UTC()}
	job := validTestJob(clock)
	job.Tenant.TenantID = tenantID
	job.Agent.TenantID = tenantID
	job.JobID = jobID
	job.ExecutionID = executionID
	job.Trace.ExecutionID = executionID
	return job
}

type postgresQueueLeaseState struct {
	status        string
	deliveryID    string
	leasedUntil   time.Time
	now           time.Time
	attempt       int
	deliveryCount int
}

func queueLeaseState(t *testing.T, f *postgresQueueFixture, tenantID, jobID string) postgresQueueLeaseState {
	t.Helper()
	var state postgresQueueLeaseState
	err := f.pool.QueryRow(f.ctx, `
		SELECT status, COALESCE(delivery_id, ''),
			COALESCE(leased_until, 'epoch'::timestamptz), attempt, delivery_count,
			clock_timestamp()
		FROM job_queue WHERE tenant_id = $1 AND job_id = $2`, tenantID, jobID).Scan(
		&state.status, &state.deliveryID, &state.leasedUntil,
		&state.attempt, &state.deliveryCount, &state.now)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func requireQueueLeaseMargin(t *testing.T, state postgresQueueLeaseState, tenantID, jobID string, delivery Delivery, minimum time.Duration) {
	t.Helper()
	remaining := state.leasedUntil.Sub(state.now)
	if state.status != "in_flight" || state.deliveryID != delivery.DeliveryID || remaining < minimum {
		t.Fatalf("lease precondition tenant=%s job=%s status=%s token_match=%t database_now=%s leased_until=%s remaining=%s required=%s attempt=%d delivery_count=%d", tenantID, jobID, state.status, state.deliveryID == delivery.DeliveryID, state.now, state.leasedUntil, remaining, minimum, state.attempt, state.deliveryCount)
	}
}

func queueRow(t *testing.T, f *postgresQueueFixture, tenantID, jobID string) (status, deliveryID, lastDeliveryID string, attempt, deliveryCount int) {
	t.Helper()
	err := f.pool.QueryRow(f.ctx, `
		SELECT status, COALESCE(delivery_id, ''), COALESCE(last_delivery_id, ''), attempt, delivery_count
		FROM job_queue WHERE tenant_id = $1 AND job_id = $2`, tenantID, jobID).Scan(
		&status, &deliveryID, &lastDeliveryID, &attempt, &deliveryCount)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func isContextDeadlineTimeout(ctx context.Context, err error) bool {
	if ctx.Err() != context.DeadlineExceeded {
		return false
	}
	var timeout net.Error
	return errors.As(err, &timeout) && timeout.Timeout()
}

func requireNoDurableDelivery(t *testing.T, q *PostgresQueue, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := q.Receive(ctx, time.Minute)
	if errors.Is(err, context.DeadlineExceeded) || isContextDeadlineTimeout(ctx, err) {
		return
	}
	var transportTimeout net.Error
	stats := q.pool.Stat()
	t.Fatalf("Receive without visible job failure stage=claim_candidate category=unexpected_receiver_error context_deadline=%t transport_timeout=%t wait_group_complete=true pool_total=%d pool_idle=%d pool_acquired=%d", ctx.Err() == context.DeadlineExceeded, errors.As(err, &transportTimeout) && transportTimeout.Timeout(), stats.TotalConns(), stats.IdleConns(), stats.AcquiredConns())
}
func TestPostgresQueueEnqueueReceiveAckPersistsAndRestoresJob(t *testing.T) {
	f := newPostgresQueueFixture(t)
	job := durableTestJob("queue-tenant-a", "job-persist", "execution-persist")
	job.History = []MessageDTO{{ID: "history-1", Role: "user", Content: "previous", CreatedAt: time.Now().UTC(), ToolCalls: []ToolCallDTO{{ID: "tool-1", Name: "lookup", Arguments: `{}`}}}}
	originalHistory := append([]MessageDTO(nil), job.History...)
	receipt, err := f.queue.Enqueue(f.ctx, job)
	if err != nil || !receipt.Accepted || receipt.JobID != job.JobID || receipt.Attempt != 1 {
		t.Fatalf("enqueue receipt=%+v err=%v", receipt, err)
	}
	job.History[0].Content = "caller mutation"
	job.Message.Content = "caller mutation"

	delivery, err := f.queue.Receive(f.ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.DeliveryID == "" || delivery.Attempt != 1 || !delivery.VisibleUntil.After(delivery.ReceivedAt) {
		t.Fatalf("invalid delivery metadata: %+v", delivery)
	}
	if delivery.Job.Message.Content != "hello" || delivery.Job.Agent != job.Agent || delivery.Job.Trace != job.Trace || delivery.Job.Tenant.TenantID != job.Tenant.TenantID || delivery.Job.Tenant.SessionID != job.Tenant.SessionID || !reflect.DeepEqual(delivery.Job.History, originalHistory) {
		t.Fatalf("durable job changed with caller mutation: %+v", delivery.Job)
	}
	status, activeToken, lastToken, attempt, deliveryCount := queueRow(t, f, job.Tenant.TenantID, job.JobID)
	if status != "in_flight" || activeToken != delivery.DeliveryID || attempt != 1 || deliveryCount != 1 {
		t.Fatalf("claimed row status=%s token=%s attempt=%d deliveries=%d", status, activeToken, attempt, deliveryCount)
	}
	if err := f.queue.Ack(f.ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if err := f.queue.Ack(f.ctx, delivery); err != nil {
		t.Fatalf("idempotent ack: %v", err)
	}
	status, activeToken, lastToken, attempt, deliveryCount = queueRow(t, f, job.Tenant.TenantID, job.JobID)
	if status != "acked" || activeToken != "" || lastToken != delivery.DeliveryID || attempt != 1 || deliveryCount != 1 {
		t.Fatalf("acked row status=%s token=%s last=%s attempt=%d deliveries=%d", status, activeToken, lastToken, attempt, deliveryCount)
	}
	requireNoDurableDelivery(t, f.queue, 80*time.Millisecond)
}

func TestPostgresQueueEnqueueIsIdempotentOrConflict(t *testing.T) {
	f := newPostgresQueueFixture(t)
	job := durableTestJob("queue-tenant-a", "job-idempotent", "execution-idempotent")
	first, err := f.queue.Enqueue(f.ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.queue.Enqueue(f.ctx, job)
	if err != nil || !second.Accepted || second.ReceiptID != first.ReceiptID || second.Attempt != first.Attempt {
		t.Fatalf("duplicate enqueue receipt=%+v err=%v first=%+v", second, err, first)
	}
	conflicting := job
	conflicting.Message.Content = "different payload"
	if receipt, err := f.queue.Enqueue(f.ctx, conflicting); !errors.Is(err, ErrEnqueueConflict) || receipt.Accepted {
		t.Fatalf("payload conflict receipt=%+v err=%v", receipt, err)
	}
	executionConflict := job
	executionConflict.JobID = "job-other"
	if receipt, err := f.queue.Enqueue(f.ctx, executionConflict); !errors.Is(err, ErrEnqueueConflict) || receipt.Accepted {
		t.Fatalf("execution conflict receipt=%+v err=%v", receipt, err)
	}
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM job_queue WHERE tenant_id = $1`, job.Tenant.TenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("durable rows after duplicate/conflict enqueue=%d", count)
	}
}

func TestPostgresQueueEnqueueDatabaseFailureDoesNotAccept(t *testing.T) {
	f := newPostgresQueueFixture(t)
	_, err := f.pool.Exec(f.ctx, `
		CREATE OR REPLACE FUNCTION queue_insert_failure() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'queue insert failure'; END; $$;
		CREATE TRIGGER queue_insert_failure_trigger BEFORE INSERT ON job_queue
		FOR EACH ROW EXECUTE FUNCTION queue_insert_failure()`)
	if err != nil {
		t.Fatal(err)
	}
	job := durableTestJob("queue-tenant-a", "job-db-failure", "execution-db-failure")
	receipt, err := f.queue.Enqueue(f.ctx, job)
	var pgErr *pgconn.PgError
	if err == nil || receipt.Accepted || !errors.Is(err, ErrQueueBackend) || !errors.As(err, &pgErr) {
		t.Fatalf("database failure receipt=%+v err=%v pg=%v", receipt, err, pgErr)
	}
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM job_queue WHERE tenant_id = $1`, job.Tenant.TenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("database failure left %d durable rows", count)
	}
}

func TestPostgresQueueConcurrentReceiveHasOneActiveDelivery(t *testing.T) {
	f := newPostgresQueueFixture(t)
	job := durableTestJob("queue-tenant-a", "job-concurrent", "execution-concurrent")
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	q1, pool1 := f.newIndependentQueue(t)
	q2, pool2 := f.newIndependentQueue(t)
	t.Cleanup(func() { pool1.Close() })
	t.Cleanup(func() { pool2.Close() })
	type result struct {
		delivery            Delivery
		err                 error
		contextErr          error
		transportTimeout    bool
		totalConnections    int32
		idleConnections     int32
		acquiredConnections int32
	}
	results := make(chan result, 2)
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, q := range []*PostgresQueue{q1, q2} {
		wg.Add(1)
		go func(q *PostgresQueue) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
			delivery, err := q.Receive(ctx, 2*time.Second)
			var timeout net.Error
			stats := q.pool.Stat()
			results <- result{
				delivery:            delivery,
				err:                 err,
				contextErr:          ctx.Err(),
				transportTimeout:    errors.As(err, &timeout) && timeout.Timeout(),
				totalConnections:    stats.TotalConns(),
				idleConnections:     stats.IdleConns(),
				acquiredConnections: stats.AcquiredConns(),
			}
			cancel()
		}(q)
	}
	for i := 0; i < 2; i++ {
		<-ready
	}
	close(start)
	wg.Wait()
	close(results)
	var winner Delivery
	successes := 0
	for result := range results {
		if result.err == nil {
			successes++
			winner = result.delivery
			continue
		}
		if !errors.Is(result.err, context.DeadlineExceeded) && !(result.contextErr == context.DeadlineExceeded && result.transportTimeout) {
			t.Fatalf("concurrent Receive failure stage=claim_candidate category=unexpected_receiver_error context_deadline=%t transport_timeout=%t active_receivers=2 wait_group_complete=true pool_total=%d pool_idle=%d pool_acquired=%d", result.contextErr == context.DeadlineExceeded, result.transportTimeout, result.totalConnections, result.idleConnections, result.acquiredConnections)
		}
	}
	if successes != 1 || winner.DeliveryID == "" {
		t.Fatalf("concurrent Receive successes=%d winner=%+v", successes, winner)
	}
	status, activeToken, _, _, deliveryCount := queueRow(t, f, job.Tenant.TenantID, job.JobID)
	if status != "in_flight" || activeToken != winner.DeliveryID || deliveryCount != 1 {
		t.Fatalf("concurrent claim row status=%s token=%s deliveries=%d", status, activeToken, deliveryCount)
	}
	if err := f.queue.Ack(f.ctx, winner); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueVisibilityExpiryReclaimsAndFencesOldToken(t *testing.T) {
	f := newPostgresQueueFixture(t)
	job := durableTestJob("queue-tenant-a", "job-expiry", "execution-expiry")
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	first, err := f.queue.Receive(f.ctx, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	q2 := f.newQueue(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	second, err := q2.Receive(ctx, time.Second)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if second.DeliveryID == first.DeliveryID || second.Attempt != 2 {
		t.Fatalf("reclaimed delivery first=%+v second=%+v", first, second)
	}
	if err := f.queue.Ack(f.ctx, first); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("old Ack after expiry/reclaim=%v", err)
	}
	if err := f.queue.Nack(f.ctx, first, NackOptions{Requeue: true, Reason: "temporary"}); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("old Nack after expiry/reclaim=%v", err)
	}
	if err := f.queue.ExtendVisibility(f.ctx, first, time.Second); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("old ExtendVisibility after expiry/reclaim=%v", err)
	}
	status, activeToken, _, attempt, deliveryCount := queueRow(t, f, job.Tenant.TenantID, job.JobID)
	if status != "in_flight" || activeToken != second.DeliveryID || attempt != 2 || deliveryCount != 2 {
		t.Fatalf("reclaimed row status=%s token=%s attempt=%d deliveries=%d", status, activeToken, attempt, deliveryCount)
	}
	if err := q2.Ack(f.ctx, second); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueNackRequeuesWithDelayAndDiscards(t *testing.T) {
	f := newPostgresQueueFixture(t)
	job := durableTestJob("queue-tenant-a", "job-nack", "execution-nack")
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	first, err := f.queue.Receive(f.ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.queue.Nack(f.ctx, first, NackOptions{Requeue: true, RetryAfter: 30 * time.Millisecond, Reason: "temporary"}); err != nil {
		t.Fatal(err)
	}
	q2 := f.newQueue(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	second, err := q2.Receive(ctx, time.Second)
	cancel()
	if err != nil || second.Attempt != 2 || second.DeliveryID == first.DeliveryID {
		t.Fatalf("delayed redelivery=%+v err=%v", second, err)
	}
	if err := f.queue.Ack(f.ctx, first); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("old Ack after Nack requeue=%v", err)
	}
	if err := q2.Nack(f.ctx, second, NackOptions{Reason: "execution_permanent_failure"}); err != nil {
		t.Fatal(err)
	}
	status, activeToken, _, attempt, deliveryCount := queueRow(t, f, job.Tenant.TenantID, job.JobID)
	if status != "discarded" || activeToken != "" || attempt != 2 || deliveryCount != 2 {
		t.Fatalf("discarded row status=%s token=%s attempt=%d deliveries=%d", status, activeToken, attempt, deliveryCount)
	}
	if err := q2.Nack(f.ctx, second, NackOptions{Requeue: true}); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("duplicate Nack after discard=%v", err)
	}
	requireNoDurableDelivery(t, q2, 80*time.Millisecond)
}

func TestPostgresQueueExtendVisibilityUsesConditionalLease(t *testing.T) {
	f := newPostgresQueueFixture(t)
	job := durableTestJob("queue-tenant-a", "job-extend", "execution-extend")
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}

	const (
		receiveVisibility = 2 * time.Second
		extension         = 5 * time.Second
		leaseGuard        = time.Second
	)
	first, err := f.queue.Receive(f.ctx, receiveVisibility)
	if err != nil {
		t.Fatal(err)
	}
	beforeExtend := queueLeaseState(t, f, job.Tenant.TenantID, job.JobID)
	requireQueueLeaseMargin(t, beforeExtend, job.Tenant.TenantID, job.JobID, first, leaseGuard)
	if beforeExtend.attempt != first.Attempt || beforeExtend.deliveryCount != 1 {
		t.Fatalf("initial lease state tenant=%s job=%s attempt=%d delivery_count=%d delivery_attempt=%d", job.Tenant.TenantID, job.JobID, beforeExtend.attempt, beforeExtend.deliveryCount, first.Attempt)
	}

	if err := f.queue.ExtendVisibility(f.ctx, first, extension); err != nil {
		t.Fatalf("ExtendVisibility valid lease tenant=%s job=%s database_now=%s leased_until=%s err=%v", job.Tenant.TenantID, job.JobID, beforeExtend.now, beforeExtend.leasedUntil, err)
	}
	extended := queueLeaseState(t, f, job.Tenant.TenantID, job.JobID)
	if extended.status != "in_flight" || extended.deliveryID != first.DeliveryID || extended.attempt != first.Attempt || extended.deliveryCount != 1 || !extended.leasedUntil.After(first.VisibleUntil) || !extended.leasedUntil.After(extended.now) {
		t.Fatalf("extended lease tenant=%s job=%s status=%s token_match=%t database_now=%s first_visible_until=%s leased_until=%s attempt=%d delivery_count=%d", job.Tenant.TenantID, job.JobID, extended.status, extended.deliveryID == first.DeliveryID, extended.now, first.VisibleUntil, extended.leasedUntil, extended.attempt, extended.deliveryCount)
	}

	q2 := f.newQueue(t)
	t.Cleanup(func() { _ = q2.Close() })
	requireNoDurableDelivery(t, q2, 120*time.Millisecond)

	result, err := f.pool.Exec(f.ctx, `
		UPDATE job_queue
		SET leased_until = clock_timestamp() - interval '1 second', updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND job_id = $2 AND status = 'in_flight' AND delivery_id = $3`,
		job.Tenant.TenantID, job.JobID, first.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("expire fixture tenant=%s job=%s rows=%d", job.Tenant.TenantID, job.JobID, result.RowsAffected())
	}
	expired := queueLeaseState(t, f, job.Tenant.TenantID, job.JobID)
	if expired.status != "in_flight" || expired.deliveryID != first.DeliveryID || expired.leasedUntil.After(expired.now) {
		t.Fatalf("expired fixture tenant=%s job=%s status=%s token_match=%t database_now=%s leased_until=%s", job.Tenant.TenantID, job.JobID, expired.status, expired.deliveryID == first.DeliveryID, expired.now, expired.leasedUntil)
	}
	if err := f.queue.ExtendVisibility(f.ctx, first, extension); !errors.Is(err, ErrDeliveryExpired) {
		t.Fatalf("ExtendVisibility expired lease tenant=%s job=%s database_now=%s leased_until=%s err=%v", job.Tenant.TenantID, job.JobID, expired.now, expired.leasedUntil, err)
	}

	second, err := q2.Receive(f.ctx, receiveVisibility)
	if err != nil {
		t.Fatal(err)
	}
	if second.DeliveryID == first.DeliveryID || second.Attempt != first.Attempt+1 {
		t.Fatalf("reclaimed delivery tenant=%s job=%s token_changed=%t first_attempt=%d second_attempt=%d", job.Tenant.TenantID, job.JobID, second.DeliveryID != first.DeliveryID, first.Attempt, second.Attempt)
	}
	beforeSecondExtend := queueLeaseState(t, f, job.Tenant.TenantID, job.JobID)
	requireQueueLeaseMargin(t, beforeSecondExtend, job.Tenant.TenantID, job.JobID, second, leaseGuard)
	if beforeSecondExtend.attempt != second.Attempt || beforeSecondExtend.deliveryCount != 2 {
		t.Fatalf("reclaimed lease state tenant=%s job=%s attempt=%d delivery_count=%d delivery_attempt=%d", job.Tenant.TenantID, job.JobID, beforeSecondExtend.attempt, beforeSecondExtend.deliveryCount, second.Attempt)
	}
	if err := f.queue.ExtendVisibility(f.ctx, first, extension); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("ExtendVisibility stale token tenant=%s job=%s database_now=%s leased_until=%s err=%v", job.Tenant.TenantID, job.JobID, beforeSecondExtend.now, beforeSecondExtend.leasedUntil, err)
	}
	if err := q2.ExtendVisibility(f.ctx, second, extension); err != nil {
		t.Fatalf("ExtendVisibility reclaimed lease tenant=%s job=%s database_now=%s leased_until=%s err=%v", job.Tenant.TenantID, job.JobID, beforeSecondExtend.now, beforeSecondExtend.leasedUntil, err)
	}
	extendedSecond := queueLeaseState(t, f, job.Tenant.TenantID, job.JobID)
	if extendedSecond.status != "in_flight" || extendedSecond.deliveryID != second.DeliveryID || !extendedSecond.leasedUntil.After(second.VisibleUntil) || !extendedSecond.leasedUntil.After(extendedSecond.now) {
		t.Fatalf("reclaimed lease extension tenant=%s job=%s status=%s token_match=%t database_now=%s second_visible_until=%s leased_until=%s", job.Tenant.TenantID, job.JobID, extendedSecond.status, extendedSecond.deliveryID == second.DeliveryID, extendedSecond.now, second.VisibleUntil, extendedSecond.leasedUntil)
	}
	if err := q2.Ack(f.ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := q2.ExtendVisibility(f.ctx, second, time.Second); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("ExtendVisibility after ack tenant=%s job=%s err=%v", job.Tenant.TenantID, job.JobID, err)
	}
}

func TestPostgresQueueTenantIsolationForActionsAndSharedIDs(t *testing.T) {
	f := newPostgresQueueFixture(t)
	f.addTenant(t, "queue-tenant-b")
	jobA := durableTestJob("queue-tenant-a", "job-shared", "execution-shared")
	jobB := durableTestJob("queue-tenant-b", "job-shared", "execution-shared")
	if _, err := f.queue.Enqueue(f.ctx, jobA); err != nil {
		t.Fatal(err)
	}
	if _, err := f.queue.Enqueue(f.ctx, jobB); err != nil {
		t.Fatal(err)
	}
	first, err := f.queue.Receive(f.ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wrongTenant := first
	wrongTenant.Job.Tenant.TenantID = "queue-tenant-missing"
	if err := f.queue.Ack(f.ctx, wrongTenant); !errors.Is(err, ErrDeliveryExpired) {
		t.Fatalf("wrong-tenant Ack=%v", err)
	}
	if err := f.queue.Ack(f.ctx, first); err != nil {
		t.Fatal(err)
	}
	second, err := f.queue.Receive(f.ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Job.Tenant.TenantID == first.Job.Tenant.TenantID || second.Job.JobID != first.Job.JobID {
		t.Fatalf("shared tenant/job delivery first=%+v second=%+v", first, second)
	}
	if err := f.queue.Ack(f.ctx, second); err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"queue-tenant-a", "queue-tenant-b"} {
		status, _, _, _, _ := queueRow(t, f, tenantID, "job-shared")
		if status != "acked" {
			t.Fatalf("tenant=%s status=%s", tenantID, status)
		}
	}
}

func TestPostgresQueueCloseDoesNotDeleteOrLoseUnfinishedJob(t *testing.T) {
	f := newPostgresQueueFixture(t)
	waiting := f.newQueue(t)
	result := make(chan error, 1)
	go func() {
		_, err := waiting.Receive(context.Background(), time.Second)
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := waiting.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("Receive after Close=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake Receive")
	}

	job := durableTestJob("queue-tenant-a", "job-close", "execution-close")
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	delivery, err := f.queue.Receive(f.ctx, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.queue.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.queue.Enqueue(f.ctx, durableTestJob("queue-tenant-a", "job-after-close", "execution-after-close")); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("Enqueue after Close=%v", err)
	}
	status, _, _, _, _ := queueRow(t, f, job.Tenant.TenantID, job.JobID)
	if status != "in_flight" {
		t.Fatalf("Close changed unfinished row status=%s", status)
	}
	q2 := f.newQueue(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	reclaimed, err := q2.Receive(ctx, time.Second)
	cancel()
	if err != nil || reclaimed.Job.JobID != delivery.Job.JobID {
		t.Fatalf("job after Queue Close reclaimed=%+v err=%v", reclaimed, err)
	}
	if err := q2.Ack(f.ctx, reclaimed); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueConnectionLossRecoversExpiredDelivery(t *testing.T) {
	f := newPostgresQueueFixture(t)
	job := durableTestJob("queue-tenant-a", "job-connection-loss", "execution-connection-loss")
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	crashedQueue, crashedPool := f.newIndependentQueue(t)
	first, err := crashedQueue.Receive(f.ctx, 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	crashedPool.Close()

	recoveryQueue := f.newQueue(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	second, err := recoveryQueue.Receive(ctx, time.Second)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if second.DeliveryID == first.DeliveryID || second.Attempt != 2 || second.Job.Message.Content != job.Message.Content {
		t.Fatalf("recovered delivery first=%+v second=%+v", first, second)
	}
	if err := recoveryQueue.Ack(f.ctx, first); !errors.Is(err, ErrDeliveryFinished) {
		t.Fatalf("old token after connection loss=%v", err)
	}
	if err := recoveryQueue.Ack(f.ctx, second); err != nil {
		t.Fatal(err)
	}
	status, activeToken, _, attempt, deliveryCount := queueRow(t, f, job.Tenant.TenantID, job.JobID)
	if status != "acked" || activeToken != "" || attempt != 2 || deliveryCount != 2 {
		t.Fatalf("recovered final row status=%s token=%s attempt=%d deliveries=%d", status, activeToken, attempt, deliveryCount)
	}
}
